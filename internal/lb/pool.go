package lb

import (
	"log"
	"net/http"
)

// ServerPool holds the set of upstreams the load balancer can route
// requests to, plus the Algorithm used to pick between them.
type ServerPool struct {
	upstreams []*Upstream
	algorithm Algorithm
}

// NewServerPool builds a pool from a list of upstreams and the algorithm
// to use when selecting between them. Each upstream's reverse proxy is
// wired to fail over to another healthy upstream if a request to it fails,
// rather than waiting for the next scheduled health check to notice.
func NewServerPool(upstreams []*Upstream, algorithm Algorithm) *ServerPool {
	p := &ServerPool{upstreams: upstreams, algorithm: algorithm}

	for _, u := range upstreams {
		u.ReverseProxy.ErrorHandler = p.handleProxyError(u)
	}

	return p
}

// Upstreams returns the full list of upstreams in the pool.
func (p *ServerPool) Upstreams() []*Upstream {
	return p.upstreams
}

// NextUpstream returns the next upstream to handle a request, as decided
// by the pool's algorithm, considering only currently healthy upstreams.
// It returns nil if none are healthy.
func (p *ServerPool) NextUpstream() *Upstream {
	alive := p.aliveUpstreams()
	if len(alive) == 0 {
		return nil
	}
	return p.algorithm.Next(alive)
}

func (p *ServerPool) aliveUpstreams() []*Upstream {
	alive := make([]*Upstream, 0, len(p.upstreams))
	for _, u := range p.upstreams {
		if u.IsAlive() {
			alive = append(alive, u)
		}
	}
	return alive
}

// handleProxyError builds the failure handler for a single upstream's
// reverse proxy. It only fires when the request to that upstream failed
// outright (e.g. connection refused) — before any response reached the
// client — so it's safe to immediately retry the same request against a
// different healthy upstream instead of failing it.
func (p *ServerPool) handleProxyError(u *Upstream) func(http.ResponseWriter, *http.Request, error) {
	return func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("upstream %s failed: %v — marking unhealthy", u.URL, err)
		u.SetAlive(false)

		next := p.NextUpstream()
		if next == nil {
			http.Error(w, "no healthy upstreams available", http.StatusServiceUnavailable)
			return
		}

		next.ReverseProxy.ServeHTTP(w, r)
	}
}
