package lb

import "sync/atomic"

// Algorithm decides which upstream should handle the next request, given
// the full list of upstreams in the pool. Swapping the Algorithm a
// ServerPool uses changes routing behavior without touching any other code.
type Algorithm interface {
	Next(upstreams []*Upstream) *Upstream
}

// RoundRobin cycles through upstreams in order, wrapping back to the start.
type RoundRobin struct {
	current uint64
}

func (r *RoundRobin) Next(upstreams []*Upstream) *Upstream {
	next := atomic.AddUint64(&r.current, 1)
	idx := next % uint64(len(upstreams))
	return upstreams[idx]
}
