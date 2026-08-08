package lb

import (
	"net/http/httputil"
	"net/url"
	"sync/atomic"
)

// Upstream represents one server the load balancer can forward requests to.
type Upstream struct {
	URL          *url.URL
	ReverseProxy *httputil.ReverseProxy

	alive    atomic.Bool
	inflight atomic.Int64
}

// NewUpstream builds an Upstream for the given server URL, ready to use.
func NewUpstream(rawURL string) (*Upstream, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	up := &Upstream{
		URL:          u,
		ReverseProxy: httputil.NewSingleHostReverseProxy(u),
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
