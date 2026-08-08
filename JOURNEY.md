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

## Phase 2 — Load-aware routing (RoundRobin → P2C)

**Why.** RoundRobin only takes turns; it's blind to what each upstream is actually doing. The moment request cost varies — one backend stuck on a slow request, a GC pause, a fat payload — RoundRobin keeps handing that backend its share anyway, and requests pile up behind it (head-of-line blocking). We wanted the routing to react to real load without paying the cost of exact least-connections (an O(N) scan per request, plus a "herd" where every concurrent request stampedes onto the single idlest server at the same instant).

**What we picked.** Power of Two Choices (P2C): pick two upstreams at random, route to whichever has fewer in-flight requests. It's O(1), needs no shared counter to contend on, and avoids the herd because the two candidates differ per request. When load is even it degenerates to uniform-random selection, so it stays balanced in the common case but routes *around* a busy/slow upstream when one appears. We chose it over "improve RoundRobin" (still load-blind) and over EWMA/least-response-time (more powerful but needs latency tracking + decay tuning — overkill for now). In-flight count is a good-enough proxy for load.

**What changed.**

- **`internal/lb/upstream.go`** — added an atomic `inflight` counter with `IncInflight()` / `DecInflight()` / `Inflight()`. This is the live-load signal the algorithm reads.
- **`internal/lb/algorithm.go`** — replaced `RoundRobin` with `P2C`. Picks two *distinct* random indices (via the `n-1` + skip trick, no rejection loop) and returns the one with fewer in-flight requests; ties go to the first pick, which keeps even-load distribution uniform.
- **`internal/lb/pool.go`** — added `Serve(w, r, u)` as the single dispatch choke point: `IncInflight` → `defer DecInflight` → `ReverseProxy.ServeHTTP`. The `defer` guarantees the count is decremented on *every* exit path (clean response, error, or failover), so it can't drift. The failover path in `handleProxyError` now dispatches through `Serve` too.
- **`cmd/server/main.go`** — wired `&lb.P2C{}` and routed dispatch through `pool.Serve`.
- **Tests** — swapped the algorithm in the pool/chaos/full-stack tests to `P2C`; the full-stack helper now mirrors `main.go` by dispatching through `pool.Serve`. New `algorithm_test.go` covers single/empty pools, a deterministic least-loaded pick (with 2 upstreams the choice is fully determined by load), and an even-load distribution band.

**Gotchas to remember.**

- **All dispatch must go through `pool.Serve`.** Calling `ReverseProxy.ServeHTTP` directly leaks the in-flight count upward, and that upstream then looks permanently busy so P2C starves it. This is the one invariant that keeps the algorithm honest.
- **Sticky sessions still bypass the algorithm** for returning clients, so P2C only balances the *new-session* path — unchanged, but it means load-awareness applies to fresh sessions, not every request.
- During a failover the failed upstream's count stays +1 until its outer `ServeHTTP` returns, so it and the retry target both show in-flight briefly. Harmless: the failed upstream is already marked `alive=false` and filtered out before P2C sees it.

---

*Future phases get appended here as the project grows.*
