package lb

import "testing"

func TestRoundRobin_Distribution(t *testing.T) {
	upstreams := dummyUpstreams(t, 4)
	rr := &RoundRobin{}

	counts := make(map[*Upstream]int)
	const calls = 4000
	for range calls {
		counts[rr.Next(upstreams)]++
	}

	if len(counts) != len(upstreams) {
		t.Fatalf("expected all %d upstreams to be selected at least once, got %d distinct", len(upstreams), len(counts))
	}

	want := calls / len(upstreams)
	for u, got := range counts {
		if got != want {
			t.Errorf("upstream %s: got %d selections, want exactly %d (round robin should be perfectly even over a multiple of len(upstreams))", u.URL, got, want)
		}
	}
}

func TestRoundRobin_SingleUpstream(t *testing.T) {
	upstreams := dummyUpstreams(t, 1)
	rr := &RoundRobin{}

	for range 10 {
		if got := rr.Next(upstreams); got != upstreams[0] {
			t.Fatalf("Next() = %v, want the only upstream %v", got, upstreams[0])
		}
	}
}

func TestServerPool_NextUpstream_NoneAlive(t *testing.T) {
	upstreams := dummyUpstreams(t, 3)
	for _, u := range upstreams {
		u.SetAlive(false)
	}

	pool := NewServerPool(upstreams, &RoundRobin{})
	if got := pool.NextUpstream(); got != nil {
		t.Fatalf("NextUpstream() = %v, want nil when no upstreams are alive", got)
	}
}

func TestServerPool_NextUpstream_SkipsDead(t *testing.T) {
	upstreams := dummyUpstreams(t, 3)
	upstreams[0].SetAlive(false)
	upstreams[2].SetAlive(false)

	pool := NewServerPool(upstreams, &RoundRobin{})
	for range 20 {
		got := pool.NextUpstream()
		if got != upstreams[1] {
			t.Fatalf("NextUpstream() = %v, want the only alive upstream %v", got, upstreams[1])
		}
	}
}
