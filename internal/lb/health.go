package lb

import (
	"log"
	"net/http"
	"sync"
	"time"
)

// Number of consecutive probe results required before an upstream's alive
// status flips. This adds hysteresis so a single transient blip (a dropped
// packet, a GC pause) doesn't flap an otherwise-healthy upstream in and out
// of the pool.
const (
	unhealthyThreshold = 2
	healthyThreshold   = 2
)

// HealthChecker periodically probes each upstream's /health endpoint and
// updates its alive status accordingly.
type HealthChecker struct {
	pool     *ServerPool
	interval time.Duration
	client   *http.Client

	mu     sync.Mutex
	streak map[*Upstream]int // positive = consecutive successes, negative = consecutive failures

	stop chan struct{}
	done chan struct{}
}

// NewHealthChecker builds a checker that polls every interval.
func NewHealthChecker(pool *ServerPool, interval time.Duration) *HealthChecker {
	return &HealthChecker{
		pool:     pool,
		interval: interval,
		client:   &http.Client{Timeout: 2 * time.Second},
		streak:   make(map[*Upstream]int),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start runs the health check loop in a background goroutine: an immediate
// check first, then again every interval, until Stop is called.
func (h *HealthChecker) Start() {
	go func() {
		defer close(h.done)

		h.checkAll()

		ticker := time.NewTicker(h.interval)
		defer ticker.Stop()

		for {
			select {
			case <-h.stop:
				return
			case <-ticker.C:
				h.checkAll()
			}
		}
	}()
}

// Stop halts the background probe loop and waits for it to exit.
func (h *HealthChecker) Stop() {
	close(h.stop)
	<-h.done
}

func (h *HealthChecker) checkAll() {
	var wg sync.WaitGroup

	for _, upstream := range h.pool.Upstreams() {
		wg.Add(1)
		go func(upstream *Upstream) {
			defer wg.Done()
			h.record(upstream, h.check(upstream))
		}(upstream)
	}

	wg.Wait()
}

// record folds a single probe result into the upstream's consecutive
// success/failure streak and flips its alive status only once the streak
// crosses the relevant threshold, so isolated bad probes don't cause flapping.
func (h *HealthChecker) record(upstream *Upstream, ok bool) {
	h.mu.Lock()
	streak := h.streak[upstream]

	// The alive flag can also be flipped out-of-band by the failover path
	// (pool.go) ejecting an upstream after repeated request failures. If
	// that happened, our streak no longer reflects reality — e.g. alive was
	// forced false while streak was still a large positive number from a
	// long healthy run. Left alone, the very next successful probe would
	// cross the healthy threshold and re-admit the upstream instantly,
	// bypassing the hysteresis this checker exists to provide. Detect the
	// mismatch (a streak value that could never have produced the current
	// alive state on its own) and reset it so re-admission still requires a
	// full healthy streak.
	alive := upstream.IsAlive()
	if (!alive && streak > healthyThreshold-1) || (alive && streak < -(unhealthyThreshold-1)) {
		streak = 0
	}

	if ok {
		if streak < 0 {
			streak = 0
		}
		streak++
	} else {
		if streak > 0 {
			streak = 0
		}
		streak--
	}
	h.streak[upstream] = streak
	h.mu.Unlock()

	switch {
	case streak >= healthyThreshold && !upstream.IsAlive():
		log.Printf("upstream %s alive=true", upstream.URL)
		upstream.SetAlive(true)
	case streak <= -unhealthyThreshold && upstream.IsAlive():
		log.Printf("upstream %s alive=false", upstream.URL)
		upstream.SetAlive(false)
	}
}

func (h *HealthChecker) check(upstream *Upstream) bool {
	resp, err := h.client.Get(upstream.URL.String() + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}
