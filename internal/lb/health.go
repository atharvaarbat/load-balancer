package lb

import (
	"log"
	"net/http"
	"sync"
	"time"
)

// HealthChecker periodically probes each upstream's /health endpoint and
// updates its alive status accordingly.
type HealthChecker struct {
	pool     *ServerPool
	interval time.Duration
	client   *http.Client
}

// NewHealthChecker builds a checker that polls every interval.
func NewHealthChecker(pool *ServerPool, interval time.Duration) *HealthChecker {
	return &HealthChecker{
		pool:     pool,
		interval: interval,
		client:   &http.Client{Timeout: 2 * time.Second},
	}
}

// Start runs the health check loop in a background goroutine: an immediate
// check first, then again every interval, for as long as the process runs.
func (h *HealthChecker) Start() {
	go func() {
		h.checkAll()

		ticker := time.NewTicker(h.interval)
		defer ticker.Stop()

		for range ticker.C {
			h.checkAll()
		}
	}()
}

func (h *HealthChecker) checkAll() {
	var wg sync.WaitGroup

	for _, upstream := range h.pool.Upstreams() {
		wg.Add(1)
		go func(upstream *Upstream) {
			defer wg.Done()

			alive := h.check(upstream)

			if alive != upstream.IsAlive() {
				log.Printf("upstream %s alive=%v", upstream.URL, alive)
			}
			upstream.SetAlive(alive)
		}(upstream)
	}

	wg.Wait()
}

func (h *HealthChecker) check(upstream *Upstream) bool {
	resp, err := h.client.Get(upstream.URL.String() + "/health")
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}
