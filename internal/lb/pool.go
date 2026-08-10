package lb

import (
	"context"
	"log"
	"net/http"
)

// passiveFailThreshold is how many consecutive request failures (transport
// errors or 5xx responses) an upstream must accumulate before the failover
// path ejects it from the pool. Without this, a single transient failure —
// a benign keep-alive race, a dropped packet — would eject an otherwise
// healthy upstream immediately, contradicting the hysteresis the active
// health checker applies. It intentionally matches unhealthyThreshold so
// both paths agree on how much evidence "unhealthy" requires.
const passiveFailThreshold = unhealthyThreshold

// ServerPool holds the set of upstreams the load balancer can route
// requests to, plus the Algorithm used to pick between them.
type ServerPool struct {
	upstreams []*Upstream
	algorithm Algorithm
}

// NewServerPool builds a pool from a list of upstreams and the algorithm
// to use when selecting between them. Each upstream's reverse proxy is
// wired to fail over to another healthy upstream if a request to it fails,
// rather than waiting for the next scheduled health check to notice, and to
// track consecutive failures — including 5xx responses, which
// httputil.ReverseProxy does not otherwise treat as an error — toward
// passive ejection.
func NewServerPool(upstreams []*Upstream, algorithm Algorithm) *ServerPool {
	p := &ServerPool{upstreams: upstreams, algorithm: algorithm}

	for _, u := range upstreams {
		u.ReverseProxy.ErrorHandler = p.handleProxyError(u)
		u.ReverseProxy.ModifyResponse = p.handleProxyResponse(u)
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

// nextUpstreamExcluding is like NextUpstream but filters out any upstream
// already recorded in tried, so a failover retry can never bounce back to
// an upstream this same request has already failed against — regardless of
// whether that upstream has crossed the passive-ejection threshold yet.
func (p *ServerPool) nextUpstreamExcluding(tried map[*Upstream]bool) *Upstream {
	alive := p.aliveUpstreams()
	candidates := make([]*Upstream, 0, len(alive))
	for _, u := range alive {
		if !tried[u] {
			candidates = append(candidates, u)
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	return p.algorithm.Next(candidates)
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
	u.requestsTotal.Add(1)
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

// triedKey is the context key used to track which upstreams a request has
// already been attempted against, so a failover retry never revisits one of
// them even if it hasn't yet crossed the passive-ejection threshold.
type triedKey struct{}

func triedFrom(r *http.Request) map[*Upstream]bool {
	if m, ok := r.Context().Value(triedKey{}).(map[*Upstream]bool); ok {
		return m
	}
	return nil
}

func withTried(r *http.Request, u *Upstream) *http.Request {
	prior := triedFrom(r)
	next := make(map[*Upstream]bool, len(prior)+1)
	for k := range prior {
		next[k] = true
	}
	next[u] = true
	return r.WithContext(context.WithValue(r.Context(), triedKey{}, next))
}

// handleProxyResponse builds the success-path hook for a single upstream's
// reverse proxy. httputil.ReverseProxy only calls ErrorHandler for
// transport-level failures, never for an HTTP error status — so without
// this, a backend that returns 500 to every real request but still answers
// its own /health check with 200 would stay "alive" forever. Folding 5xx
// responses into the same consecutive-failure streak as transport errors
// gives the passive path a basic circuit breaker for that case. A non-5xx
// response resets the streak, since the upstream just proved it's working.
func (p *ServerPool) handleProxyResponse(u *Upstream) func(*http.Response) error {
	return func(resp *http.Response) error {
		if resp.StatusCode >= http.StatusInternalServerError {
			u.failuresTotal.Add(1)
			streak := u.failStreak.Add(1)
			if streak >= passiveFailThreshold {
				log.Printf("upstream %s returned %d — marking unhealthy after %d consecutive failures", u.URL, resp.StatusCode, streak)
				u.SetAlive(false)
			}
			return nil
		}
		u.failStreak.Store(0)
		return nil
	}
}

// handleProxyError builds the failure handler for a single upstream's
// reverse proxy. It only fires when the request to that upstream failed
// outright (e.g. connection refused) — before any response reached the
// client — so it's usually safe to retry the same request against a
// different healthy upstream instead of failing it. Several guards keep
// that safe:
//   - if the request's own context is already done, the client disconnected
//     or its deadline expired; that's not the upstream's fault, so we
//     neither penalize it nor retry.
//   - if the request has a body, it may already have been partially
//     streamed to the failed upstream, so blindly replaying it elsewhere
//     could silently send a truncated body. Only bodyless requests are
//     retried automatically.
//   - retries are capped at one attempt per remaining upstream, so a
//     request can't bounce through the whole pool more times than there
//     are upstreams to try, and a per-request "tried" set (not just the
//     alive flag) guarantees a retry never revisits an upstream this
//     request already failed against.
//   - the upstream isn't ejected from the pool until passiveFailThreshold
//     consecutive failures have accumulated, so one isolated blip doesn't
//     permanently remove a healthy upstream from rotation; that's still
//     tracked via failStreak even though this request fails over regardless.
func (p *ServerPool) handleProxyError(u *Upstream) func(http.ResponseWriter, *http.Request, error) {
	return func(w http.ResponseWriter, r *http.Request, err error) {
		if r.Context().Err() != nil {
			return
		}

		u.failuresTotal.Add(1)
		streak := u.failStreak.Add(1)
		if streak >= passiveFailThreshold {
			log.Printf("upstream %s failed: %v — marking unhealthy after %d consecutive failures", u.URL, err, streak)
			u.SetAlive(false)
		} else {
			log.Printf("upstream %s failed: %v (%d/%d consecutive failures, not yet ejecting)", u.URL, err, streak, passiveFailThreshold)
		}

		if r.ContentLength != 0 {
			http.Error(w, "upstream request failed", http.StatusBadGateway)
			return
		}

		if retryCountFrom(r) >= len(p.upstreams)-1 {
			http.Error(w, "no healthy upstreams available", http.StatusServiceUnavailable)
			return
		}

		r = withTried(r, u)
		next := p.nextUpstreamExcluding(triedFrom(r))
		if next == nil {
			http.Error(w, "no healthy upstreams available", http.StatusServiceUnavailable)
			return
		}

		p.Serve(w, withIncrementedRetryCount(r), next)
	}
}
