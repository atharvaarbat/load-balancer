package lb

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// fakeUpstream is a controllable backend used across the test suite. While
// up, it answers /health and / like a normal server. While down, it
// hijacks and immediately closes the raw connection, producing a real
// connection-level failure — the same failure shape a crashed or
// unreachable backend produces — instead of an HTTP error status (which
// httputil.ReverseProxy's ErrorHandler does not trigger on).
type fakeUpstream struct {
	server *httptest.Server
	up     atomic.Bool
	hits   atomic.Int64
}

func newFakeUpstream(t *testing.T) *fakeUpstream {
	t.Helper()

	f := &fakeUpstream{}
	f.up.Store(true)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(f.handle))
	// The hijack-close technique deliberately produces a connection reset
	// mid-request; silence the default server logging for that so test
	// output isn't full of expected "broken pipe" noise.
	srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	srv.Start()

	f.server = srv
	t.Cleanup(srv.Close)
	return f
}

func (f *fakeUpstream) handle(w http.ResponseWriter, r *http.Request) {
	f.hits.Add(1)

	if !f.up.Load() {
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				conn.Close()
			}
		}
		return
	}

	if r.URL.Path == "/health" {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "hello from %s", f.server.URL)
}

func (f *fakeUpstream) setUp(up bool) { f.up.Store(up) }
func (f *fakeUpstream) Hits() int64   { return f.hits.Load() }

// noKeepAliveTransport is used everywhere a test needs a fakeUpstream's
// down transition to be observed deterministically. Without it, Go's
// http.Transport can silently retry a request against a fresh connection
// when a *reused* pooled connection turns out dead, which would mask a
// down probe and throw off exact hit-count assertions.
func noKeepAliveTransport() *http.Transport {
	return &http.Transport{DisableKeepAlives: true}
}

// upstreamFor wraps a fakeUpstream as a real *Upstream, wired with a
// non-keep-alive transport for deterministic failure injection.
func upstreamFor(t *testing.T, f *fakeUpstream) *Upstream {
	t.Helper()

	u, err := NewUpstream(f.server.URL)
	if err != nil {
		t.Fatalf("NewUpstream(%s): %v", f.server.URL, err)
	}
	u.ReverseProxy.Transport = noKeepAliveTransport()
	return u
}

// dummyUpstreams builds n *Upstream values with distinct placeholder URLs
// and no backing server — for tests that only exercise selection logic
// (RoundRobin, pool filtering, concurrent SetAlive/NextUpstream) and never
// actually issue a request.
func dummyUpstreams(t *testing.T, n int) []*Upstream {
	t.Helper()

	upstreams := make([]*Upstream, n)
	for i := range n {
		u, err := NewUpstream(fmt.Sprintf("http://upstream-%d.invalid", i))
		if err != nil {
			t.Fatalf("NewUpstream: %v", err)
		}
		upstreams[i] = u
	}
	return upstreams
}

// newFakePool builds a ServerPool of n fake upstreams behind the real
// RoundRobin algorithm. The returned fakes slice is index-aligned with
// pool.Upstreams().
func newFakePool(t *testing.T, n int) (*ServerPool, []*fakeUpstream) {
	t.Helper()

	fakes := make([]*fakeUpstream, n)
	upstreams := make([]*Upstream, n)
	for i := range n {
		fakes[i] = newFakeUpstream(t)
		upstreams[i] = upstreamFor(t, fakes[i])
	}

	return NewServerPool(upstreams, &RoundRobin{}), fakes
}

// fullStack wires ServerPool + StickySession + HealthChecker together
// exactly like cmd/server/main.go, fronted by an httptest.Server, for
// full end-to-end scenarios.
type fullStack struct {
	Server  *httptest.Server
	Pool    *ServerPool
	Sticky  *StickySession
	Checker *HealthChecker
	Fakes   []*fakeUpstream
}

// newFullStack starts a HealthChecker goroutine that has no Stop() (none
// exists on HealthChecker today), so it outlives the test. It only ever
// touches atomics on fakeUpstreams whose listeners get closed by
// t.Cleanup, which just means later probes fail with connection-refused —
// harmless for test purposes.
func newFullStack(t *testing.T, n int, healthInterval time.Duration) *fullStack {
	t.Helper()

	pool, fakes := newFakePool(t, n)
	sticky := NewStickySession(pool)

	checker := NewHealthChecker(pool, healthInterval)
	checker.client = &http.Client{Timeout: 500 * time.Millisecond, Transport: noKeepAliveTransport()}
	checker.Start()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		upstream := sticky.Route(w, r)
		if upstream == nil {
			http.Error(w, "no healthy upstreams available", http.StatusServiceUnavailable)
			return
		}
		upstream.ReverseProxy.ServeHTTP(w, r)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return &fullStack{Server: srv, Pool: pool, Sticky: sticky, Checker: checker, Fakes: fakes}
}
