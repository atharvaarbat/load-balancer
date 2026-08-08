# Journey

## Phase 1 — Core load balancer

**`cmd/backend`** — minimal test backend. Serves `/health` (200 "ok") and `/` (`Hello from backend on port <PORT>`). Used only to exercise the LB in `test/lb_test.py`.

**`cmd/server`** — LB entry point. Wires 3 static upstreams (`localhost:9001-9003`) into a `ServerPool` with `RoundRobin` routing, wraps that in `StickySession`, starts the `HealthChecker`, and serves `/health` (static "ok") and `/` (proxies via sticky routing) on `:8080`.

**`internal/lb/upstream.go`** — `Upstream`: a backend's URL, its `httputil.ReverseProxy`, and a mutex-guarded `alive` flag.

**`internal/lb/algorithm.go`** — `Algorithm` interface (`Next(upstreams) *Upstream`); one implementation, `RoundRobin`, cycling via an atomic counter.

**`internal/lb/pool.go`** — `ServerPool`: holds the upstream list + algorithm, filters to only-alive upstreams before picking one. Each upstream's reverse proxy has an error handler that fails over to another healthy upstream on connection failure, with guards: skips retry if the client already disconnected (`r.Context().Err() != nil`), skips retry if the request has a body (may be partially consumed), and caps retries at `len(upstreams)-1`.

**`internal/lb/health.go`** — `HealthChecker`: polls each upstream's `/health` every interval (5s), tracks a consecutive success/failure streak per upstream, and only flips `alive` once the streak crosses a threshold (2) — avoids flapping on a single bad probe.

**`internal/lb/sticky.go`** — `StickySession`: cookie (`LB_SESSION_ID`) pins a client to the upstream it first landed on. Sessions carry a `lastAccess` timestamp; a background sweeper evicts entries idle longer than 30 minutes so the map doesn't grow unbounded.

**`test/lb_test.py`** — integration harness: builds the backend image, (re)creates 3 backend containers, builds + starts the LB binary, waits for everything to report healthy, runs a smoke test against `/`.

**State**: everything above is in-memory only — no persistence. A LB restart resets health status and sticky-session assignments (see discussion, not yet written up here).

---

*Future phases get appended here as the project grows.*
