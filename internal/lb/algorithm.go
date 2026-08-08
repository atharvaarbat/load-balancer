package lb

import "math/rand/v2"

// Algorithm decides which upstream should handle the next request, given
// the list of upstreams the pool considers eligible. Swapping the Algorithm
// a ServerPool uses changes routing behavior without touching any other code.
type Algorithm interface {
	Next(upstreams []*Upstream) *Upstream
}

// P2C implements the "power of two choices" load-balancing strategy: pick
// two distinct upstreams at random and route to whichever currently has
// fewer in-flight requests.
//
// It costs O(1) per request — no global scan of the pool and no shared
// counter to contend on — yet it routes around a momentarily-overloaded or
// slow upstream the way plain round robin can't, because it looks at real
// live load rather than just taking turns. When load is even it degenerates
// to uniform random selection (a tie returns the first pick, which is itself
// drawn uniformly), so distribution stays balanced in the common case.
type P2C struct{}

func (p *P2C) Next(upstreams []*Upstream) *Upstream {
	n := len(upstreams)
	switch n {
	case 0:
		return nil
	case 1:
		return upstreams[0]
	}

	i := rand.IntN(n)
	// Pick a second, distinct index without a rejection loop: draw from the
	// n-1 remaining slots, then skip over i so i and j can never collide.
	j := rand.IntN(n - 1)
	if j >= i {
		j++
	}

	a, b := upstreams[i], upstreams[j]
	if b.Inflight() < a.Inflight() {
		return b
	}
	return a
}
