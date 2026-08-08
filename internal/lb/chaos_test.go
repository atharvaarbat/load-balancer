package lb

import (
	"io"
	"math/rand"
	"net/http"
	"net/http/cookiejar"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPool_ConcurrentSelectionAndAliveFlips hammers the bare pool (no HTTP
// involved) with concurrent reads and concurrent alive-status flips. It
// exists to be run under `go test -race`: the pool's algorithm and each
// upstream's alive flag are exactly the shared state a real deployment
// mutates from multiple goroutines (health checker vs. failover vs.
// routing), so this is where a missed lock or a non-atomic flag would
// surface.
func TestPool_ConcurrentSelectionAndAliveFlips(t *testing.T) {
	upstreams := dummyUpstreams(t, 8)
	pool := NewServerPool(upstreams, &P2C{})

	const readers = 50
	const flippers = 20
	const duration = 200 * time.Millisecond

	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(readers)
	for range readers {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				u := pool.NextUpstream()
				if u == nil {
					continue
				}
				if !slices.Contains(upstreams, u) {
					t.Errorf("NextUpstream() returned an upstream not in the pool")
				}
			}
		}()
	}

	wg.Add(flippers)
	for seed := range flippers {
		go func(seed int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(int64(seed)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				idx := rnd.Intn(len(upstreams))
				upstreams[idx].SetAlive(rnd.Intn(2) == 0)
			}
		}(seed)
	}

	time.Sleep(duration)
	close(stop)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("goroutines did not finish — possible deadlock in pool selection or SetAlive")
	}
}

// TestFullStack_ConcurrentClientsWithFlappingUpstreams drives real HTTP
// traffic through the full stack (ServerPool + StickySession +
// HealthChecker, exactly as cmd/server/main.go wires them) while upstreams
// randomly go up and down underneath it. Each simulated client keeps its
// own cookie jar, so this also stresses StickySession's concurrent
// map access, including the delete-and-reissue path when a client's
// pinned upstream dies mid-chaos. This is the "brutal" tier: it only
// asserts invariants (never panics/deadlocks, every response is a
// well-formed 200/502/503, the pool recovers once upstreams settle) since
// exact routing outcomes are inherently racy under concurrent flapping.
func TestFullStack_ConcurrentClientsWithFlappingUpstreams(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping full-stack chaos test in -short mode")
	}

	stack := newFullStack(t, 4, 20*time.Millisecond)

	const clients = 16
	const chaosDuration = 400 * time.Millisecond

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var total, ok200, ok5xx, bad atomic.Int64

	wg.Add(clients)
	for range clients {
		go func() {
			defer wg.Done()
			jar, _ := cookiejar.New(nil)
			client := &http.Client{Timeout: 2 * time.Second, Jar: jar}

			for {
				select {
				case <-stop:
					return
				default:
				}
				total.Add(1)

				resp, err := client.Get(stack.Server.URL + "/")
				if err != nil {
					bad.Add(1)
					continue
				}
				body, readErr := io.ReadAll(resp.Body)
				resp.Body.Close()

				switch resp.StatusCode {
				case http.StatusOK:
					ok200.Add(1)
					if readErr != nil || !strings.Contains(string(body), "hello from") {
						bad.Add(1)
					}
				case http.StatusServiceUnavailable, http.StatusBadGateway:
					ok5xx.Add(1)
				default:
					bad.Add(1)
				}
			}
		}()
	}

	flipStop := make(chan struct{})
	var flipWg sync.WaitGroup
	flipWg.Go(func() {
		rnd := rand.New(rand.NewSource(1))
		for {
			select {
			case <-flipStop:
				return
			default:
			}
			idx := rnd.Intn(len(stack.Fakes))
			stack.Fakes[idx].setUp(rnd.Intn(4) != 0) // mostly up, sometimes down
			time.Sleep(5 * time.Millisecond)
		}
	})

	time.Sleep(chaosDuration)
	close(flipStop)
	flipWg.Wait()

	for _, f := range stack.Fakes {
		f.setUp(true)
	}

	close(stop)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("client goroutines did not finish — possible deadlock in the LB under chaos")
	}

	t.Logf("chaos summary: total=%d ok200=%d ok5xx=%d bad=%d", total.Load(), ok200.Load(), ok5xx.Load(), bad.Load())
	if total.Load() == 0 {
		t.Fatal("no requests were made — test setup problem")
	}
	if bad.Load() != 0 {
		t.Errorf("%d responses were malformed or raw connection errors instead of a clean 200/502/503", bad.Load())
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		allAlive := true
		for _, u := range stack.Pool.Upstreams() {
			if !u.IsAlive() {
				allAlive = false
				break
			}
		}
		if allAlive {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("pool did not recover to fully healthy after all upstreams came back up")
}
