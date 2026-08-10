package lb

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"time"
)

// Upstream represents one server the load balancer can forward requests to.
type Upstream struct {
	URL          *url.URL
	ReverseProxy *httputil.ReverseProxy

	alive    atomic.Bool
	inflight atomic.Int64

	// failStreak counts consecutive request failures (transport-level
	// errors or 5xx responses) since the last success. ServerPool uses it
	// to eject an upstream only after repeated failures rather than a
	// single blip, and resets it to 0 on every successful response.
	failStreak atomic.Int64

	// Cumulative counters for observability (see /metrics in cmd/server).
	requestsTotal atomic.Int64
	failuresTotal atomic.Int64
}

// newTransport builds a per-upstream *http.Transport instead of relying on
// http.DefaultTransport, which every upstream would otherwise share. The
// default's MaxIdleConnsPerHost is only 2, so under any real concurrency
// the LB would tear down and re-dial a backend connection on nearly every
// request — adding a handshake to the hot path and burning through
// ephemeral ports. ResponseHeaderTimeout also guards against a backend that
// accepts a connection and then hangs forever.
func newTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   256,
		MaxConnsPerHost:       512,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
	}
}

// NewUpstream builds an Upstream for the given server URL, ready to use.
func NewUpstream(rawURL string) (*Upstream, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("upstream %q: scheme must be http or https", rawURL)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("upstream %q: missing host", rawURL)
	}

	up := &Upstream{URL: u}
	up.ReverseProxy = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(u)
			// This load balancer is the trust boundary: an inbound
			// X-Forwarded-For is client-supplied and must not be relayed
			// as-is, or a client could spoof a trusted-looking chain (e.g.
			// an internal IP) that a backend reads as the original caller.
			pr.Out.Header.Del("X-Forwarded-For")
			pr.SetXForwarded() // sets X-Forwarded-For/Host/Proto from the real request
		},
		Transport: newTransport(),
	}
	up.alive.Store(true)
	return up, nil
}

// SetAlive updates the upstream's health status.
func (u *Upstream) SetAlive(alive bool) {
	u.alive.Store(alive)
}

// IsAlive reports whether the upstream is currently considered healthy.
func (u *Upstream) IsAlive() bool {
	return u.alive.Load()
}

// IncInflight records that a request has been dispatched to this upstream.
func (u *Upstream) IncInflight() {
	u.inflight.Add(1)
}

// DecInflight records that an in-flight request to this upstream has finished.
func (u *Upstream) DecInflight() {
	u.inflight.Add(-1)
}

// Inflight reports how many requests are currently in flight to this
// upstream. Load-aware algorithms (e.g. P2C) use this to route around
// momentarily-busy or slow upstreams.
func (u *Upstream) Inflight() int64 {
	return u.inflight.Load()
}

// RequestsTotal reports the cumulative number of requests dispatched to
// this upstream since it was created.
func (u *Upstream) RequestsTotal() int64 {
	return u.requestsTotal.Load()
}

// FailuresTotal reports the cumulative number of failed requests (transport
// errors or 5xx responses) for this upstream since it was created.
func (u *Upstream) FailuresTotal() int64 {
	return u.failuresTotal.Load()
}
