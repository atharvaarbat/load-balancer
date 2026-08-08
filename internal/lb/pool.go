package lb

import (
	"context"
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

// Serve dispatches a request to the given upstream, counting it as in-flight
// for the duration so load-aware algorithms (e.g. P2C) see real-time load.
// The count is incremented before the request is proxied and decremented via
// defer once it completes, so every exit path — a clean response, an error,
// or an internal failover to another upstream — is accounted for exactly
// once. All request dispatch should go through here rather than calling
// upstream.ReverseProxy.ServeHTTP directly, or the in-flight count will drift.
func (p *ServerPool) Serve(w http.ResponseWriter, r *http.Request, u *Upstream) {
	u.IncInflight()
	defer u.DecInflight()
	u.ReverseProxy.ServeHTTP(w, r)
}

// retryCountKey is the context key used to track how many times a request
// has already been failed over to a different upstream.
type retryCountKey struct{}

func retryCountFrom(r *http.Request) int {
	if n, ok := r.Context().Value(retryCountKey{}).(int); ok {
		return n
	}
	return 0
}

func withIncrementedRetryCount(r *http.Request) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), retryCountKey{}, retryCountFrom(r)+1))
}

// handleProxyError builds the failure handler for a single upstream's
// reverse proxy. It only fires when the request to that upstream failed
// outright (e.g. connection refused) — before any response reached the
// client — so it's usually safe to retry the same request against a
// different healthy upstream instead of failing it. Three guards keep that
// safe:
//   - if the request's own context is already done, the client disconnected
//     or its deadline expired; that's not the upstream's fault, so we
//     neither penalize it nor retry.
//   - if the request has a body, it may already have been partially
//     streamed to the failed upstream, so blindly replaying it elsewhere
//     could silently send a truncated body. Only bodyless requests are
//     retried automatically.
//   - retries are capped at one attempt per remaining upstream, so a
//     request can't bounce through the whole pool more times than there
//     are upstreams to try.
func (p *ServerPool) handleProxyError(u *Upstream) func(http.ResponseWriter, *http.Request, error) {
	return func(w http.ResponseWriter, r *http.Request, err error) {
		if r.Context().Err() != nil {
			return
		}

		log.Printf("upstream %s failed: %v — marking unhealthy", u.URL, err)
		u.SetAlive(false)

		if r.ContentLength != 0 {
			http.Error(w, "upstream request failed", http.StatusBadGateway)
			return
		}

		if retryCountFrom(r) >= len(p.upstreams)-1 {
			http.Error(w, "no healthy upstreams available", http.StatusServiceUnavailable)
			return
		}

		next := p.NextUpstream()
		if next == nil {
			http.Error(w, "no healthy upstreams available", http.StatusServiceUnavailable)
			return
		}

		p.Serve(w, withIncrementedRetryCount(r), next)
	}
}
