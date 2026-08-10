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
// (P2C, pool filtering, concurrent SetAlive/NextUpstream) and never
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
// P2C algorithm. The returned fakes slice is index-aligned with
// pool.Upstreams().
func newFakePool(t *testing.T, n int) (*ServerPool, []*fakeUpstream) {
	t.Helper()

	fakes := make([]*fakeUpstream, n)
	upstreams := make([]*Upstream, n)
	for i := range n {
		fakes[i] = newFakeUpstream(t)
		upstreams[i] = upstreamFor(t, fakes[i])
	}

	return NewServerPool(upstreams, &P2C{}), fakes
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

// newFullStack wires everything together with proper cleanup: both the
// HealthChecker's probe loop and the StickySession's sweeper are stopped
// via t.Cleanup so they don't outlive the test.
func newFullStack(t *testing.T, n int, healthInterval time.Duration) *fullStack {
	t.Helper()

	pool, fakes := newFakePool(t, n)
	sticky := NewStickySession(pool)
	t.Cleanup(sticky.Stop)

	checker := NewHealthChecker(pool, healthInterval)
	checker.client = &http.Client{Timeout: 500 * time.Millisecond, Transport: noKeepAliveTransport()}
	checker.Start()
	t.Cleanup(checker.Stop)

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
		pool.Serve(w, r, upstream)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return &fullStack{Server: srv, Pool: pool, Sticky: sticky, Checker: checker, Fakes: fakes}
}
