package lb

import "testing"

func TestP2C_SingleUpstream(t *testing.T) {
	upstreams := dummyUpstreams(t, 1)
	p := &P2C{}

	for range 10 {
		if got := p.Next(upstreams); got != upstreams[0] {
			t.Fatalf("Next() = %v, want the only upstream %v", got, upstreams[0])
		}
	}
}

func TestP2C_NoUpstreams(t *testing.T) {
	p := &P2C{}
	if got := p.Next(nil); got != nil {
		t.Fatalf("Next(nil) = %v, want nil", got)
	}
}

// TestP2C_PicksLeastLoaded exercises the load-aware core of P2C. With exactly
// two upstreams, both are always the two candidates, so the pick is fully
// deterministic: it must always be whichever has fewer in-flight requests.
func TestP2C_PicksLeastLoaded(t *testing.T) {
	upstreams := dummyUpstreams(t, 2)
	p := &P2C{}

	// upstreams[0] is busier, so every pick must land on upstreams[1].
	for range 5 {
		upstreams[0].IncInflight()
	}
	for range 100 {
		if got := p.Next(upstreams); got != upstreams[1] {
			t.Fatalf("Next() = %v, want least-loaded upstream %v", got, upstreams[1])
		}
	}

	// Flip the load: drain [0] and make [1] busier; the picks must follow.
	for range 5 {
		upstreams[0].DecInflight()
	}
	for range 3 {
		upstreams[1].IncInflight()
	}
	for range 100 {
		if got := p.Next(upstreams); got != upstreams[0] {
			t.Fatalf("Next() = %v, want least-loaded upstream %v after load flip", got, upstreams[0])
		}
	}
}

// TestP2C_EvenLoadDistributesUniformly checks the common case: when every
// upstream carries equal in-flight load, ties resolve to the first (uniformly
// random) pick, so selections spread roughly evenly across the pool. P2C is
// randomized, so this asserts a tolerance band around the mean rather than an
// exact split.
func TestP2C_EvenLoadDistributesUniformly(t *testing.T) {
	upstreams := dummyUpstreams(t, 4)
	p := &P2C{}

	counts := make(map[*Upstream]int)
	const calls = 8000
	for range calls {
		counts[p.Next(upstreams)]++
	}

	if len(counts) != len(upstreams) {
		t.Fatalf("expected all %d upstreams to be selected at least once, got %d distinct", len(upstreams), len(counts))
	}

	want := calls / len(upstreams)
	const tolerance = 0.15
	lo := int(float64(want) * (1 - tolerance))
	hi := int(float64(want) * (1 + tolerance))
	for u, got := range counts {
		if got < lo || got > hi {
			t.Errorf("upstream %s: got %d selections, want within [%d, %d] (even load should spread roughly uniformly)", u.URL, got, lo, hi)
		}
	}
}

func TestServerPool_NextUpstream_NoneAlive(t *testing.T) {
	upstreams := dummyUpstreams(t, 3)
	for _, u := range upstreams {
		u.SetAlive(false)
	}

	pool := NewServerPool(upstreams, &P2C{})
	if got := pool.NextUpstream(); got != nil {
		t.Fatalf("NextUpstream() = %v, want nil when no upstreams are alive", got)
	}
}

func TestServerPool_NextUpstream_SkipsDead(t *testing.T) {
	upstreams := dummyUpstreams(t, 3)
	upstreams[0].SetAlive(false)
	upstreams[2].SetAlive(false)

	pool := NewServerPool(upstreams, &P2C{})
	for range 20 {
		got := pool.NextUpstream()
		if got != upstreams[1] {
			t.Fatalf("NextUpstream() = %v, want the only alive upstream %v", got, upstreams[1])
		}
	}
}
