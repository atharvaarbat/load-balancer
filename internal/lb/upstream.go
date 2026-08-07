package lb

import (
	"net/http/httputil"
	"net/url"
	"sync"
)

// Upstream represents one server the load balancer can forward requests to.
type Upstream struct {
	URL          *url.URL
	ReverseProxy *httputil.ReverseProxy

	mu    sync.RWMutex
	alive bool
}

// NewUpstream builds an Upstream for the given server URL, ready to use.
func NewUpstream(rawURL string) (*Upstream, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	return &Upstream{
		URL:          u,
		ReverseProxy: httputil.NewSingleHostReverseProxy(u),
		alive:        true,
	}, nil
}

// SetAlive updates the upstream's health status.
func (u *Upstream) SetAlive(alive bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.alive = alive
}

// IsAlive reports whether the upstream is currently considered healthy.
func (u *Upstream) IsAlive() bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.alive
}
