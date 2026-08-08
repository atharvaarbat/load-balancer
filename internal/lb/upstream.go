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

	alive atomic.Bool
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
