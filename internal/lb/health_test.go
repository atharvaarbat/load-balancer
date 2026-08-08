package lb

import (
	"net/http"
	"testing"
	"time"
)

// newTestChecker builds a HealthChecker for tests that drive checkAll()
// manually rather than through Start()'s ticker loop, so probe timing is
// fully deterministic. The interval is irrelevant since Start() is never
// called.
func newTestChecker(pool *ServerPool) *HealthChecker {
	h := NewHealthChecker(pool, time.Hour)
	h.client = &http.Client{Timeout: 500 * time.Millisecond, Transport: noKeepAliveTransport()}
	return h
}

func TestHealthChecker_FlipsAliveOnlyAfterThreshold(t *testing.T) {
	pool, _ := newFakePool(t, 1)
	upstream := pool.Upstreams()[0]
	upstream.SetAlive(false) // starts dead; backend itself is healthy

	h := newTestChecker(pool)

	h.checkAll()
	if upstream.IsAlive() {
		t.Fatal("upstream flipped alive after a single successful probe; healthyThreshold is 2")
	}

	h.checkAll()
	if !upstream.IsAlive() {
		t.Fatal("upstream should be alive after 2 consecutive successful probes")
	}
}

func TestHealthChecker_FlipsDeadOnlyAfterThreshold(t *testing.T) {
	pool, fakes := newFakePool(t, 1)
	upstream := pool.Upstreams()[0] // starts alive by default
	fakes[0].setUp(false)

	h := newTestChecker(pool)

	h.checkAll()
	if !upstream.IsAlive() {
		t.Fatal("upstream flipped dead after a single failed probe; unhealthyThreshold is 2")
	}

	h.checkAll()
	if upstream.IsAlive() {
		t.Fatal("upstream should be dead after 2 consecutive failed probes")
	}
}

func TestHealthChecker_ResistsFlapping(t *testing.T) {
	pool, fakes := newFakePool(t, 1)
	upstream := pool.Upstreams()[0]
	h := newTestChecker(pool)

	// Alternate up/down on every probe. Each result immediately resets and
	// reverses the streak, so it can never reach the threshold of 2 in
	// either direction — alive should never flip away from its start state.
	for i := range 10 {
		fakes[0].setUp(i%2 == 0)
		h.checkAll()
		if !upstream.IsAlive() {
			t.Fatalf("iteration %d: upstream flipped dead on an isolated blip during flapping", i)
		}
	}
}

func TestHealthChecker_RecoversAfterSustainedOutage(t *testing.T) {
	pool, fakes := newFakePool(t, 1)
	upstream := pool.Upstreams()[0]
	h := newTestChecker(pool)

	fakes[0].setUp(false)
	h.checkAll()
	h.checkAll()
	if upstream.IsAlive() {
		t.Fatal("upstream should be dead after a sustained outage")
	}

	fakes[0].setUp(true)
	h.checkAll()
	h.checkAll()
	if !upstream.IsAlive() {
		t.Fatal("upstream should recover once probes succeed again")
	}
}
