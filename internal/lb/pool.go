package lb

// ServerPool holds the set of upstreams the load balancer can route
// requests to, plus the Algorithm used to pick between them.
type ServerPool struct {
	upstreams []*Upstream
	algorithm Algorithm
}

// NewServerPool builds a pool from a list of upstreams and the algorithm
// to use when selecting between them.
func NewServerPool(upstreams []*Upstream, algorithm Algorithm) *ServerPool {
	return &ServerPool{upstreams: upstreams, algorithm: algorithm}
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
