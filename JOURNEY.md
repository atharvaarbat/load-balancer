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

---

## Phase 3 — Production hardening

**Why.** A review against real production deployment turned up several gaps: an unauthenticated memory-exhaustion path, a flapping bug in the health/failover interaction, missing HTTP server timeouts, connection-pool starvation, spoofable forwarding headers, and no operational visibility at all. None of these show up in a demo — they show up under real traffic, real attackers, or a real deploy.

**What changed.**

- **`internal/lb/health.go`** — fixed a flap-loop: `handleProxyError` (passive path) flips `alive` directly, but `HealthChecker.record` tracked its own success/failure streak independently, so an upstream ejected passively while its active streak was still deeply positive would be re-admitted after a *single* lucky probe, bypassing the 2-consecutive-success rule entirely. `record` now detects a streak that couldn't have produced the upstream's current `alive` state on its own (i.e. was changed out-of-band) and resets it, so re-admission always requires a full healthy streak regardless of who flipped the flag.
- **`internal/lb/pool.go`** — the passive path no longer ejects an upstream on a single failed request; it now takes `passiveFailThreshold` (2, matching the active checker) consecutive failures — transport errors *and* 5xx responses, via a new `ModifyResponse` hook, since `ReverseProxy` never calls `ErrorHandler` for an HTTP error status. A per-request "tried" set (context-carried) replaces reliance on the `alive` flag for failover exclusion, so a retry chain can never revisit an upstream it already failed against even before that upstream crosses the ejection threshold.
- **`internal/lb/sticky.go`** — capped the session table at `maxSessions` (100k); past the cap, cookie-less traffic (the common shape for API clients, probes, or an attacker looping requests) is still routed but not pinned, instead of growing the map without bound. Cookie now sets `Secure`, `SameSite=Lax`, and `Max-Age`. `sweep()` collects expired keys under a read lock and only takes the write lock for the (much smaller) delete pass, so a sweep over a large map no longer blocks every concurrent `Route()` call for its full duration. Added `Stop()` for a clean shutdown of the sweeper goroutine.
- **`internal/lb/health.go`** — added `Stop()` so the probe loop can be halted instead of leaking for the life of the process.
- **`internal/lb/upstream.go`** — each upstream now gets its own tuned `*http.Transport` (previously nil, silently falling back to `http.DefaultTransport`'s `MaxIdleConnsPerHost` of 2, which meant connections were torn down and re-dialed on nearly every request under real concurrency). Switched from the legacy `Director` to the `Rewrite` hook with `SetXForwarded()`, and explicitly strip any inbound `X-Forwarded-For` first — this LB is the trust boundary, so a client-supplied value must not be relayed as if it were part of a trusted proxy chain. `NewUpstream` now rejects a URL with no scheme or host instead of silently accepting it (`url.Parse` alone is too permissive — a typo'd config like `"localhost:9001"` parses without error).
- **`cmd/server/main.go`** — rewritten: upstream list and listen address now come from `LB_UPSTREAMS`/`LB_ADDR` (previously hardcoded, requiring a rebuild to change); `/health` reflects real pool state instead of a static "ok"; the `http.Server` sets read/write/idle timeouts (previously none, making the LB trivially slow-loris-able); SIGINT/SIGTERM trigger `Server.Shutdown` with a drain timeout instead of the process dying mid-response; added `/metrics` (hand-rolled Prometheus text format — the project stays dependency-free) and structured JSON access logs with a generated/propagated `X-Request-ID`.
- **Repo hygiene** — added `.gitignore`; untracked `server.exe`, `test/bin/server.exe`, `cmd/backend/bin/backend`, and `test/lb_output.log` (all stale build artifacts already regenerated fresh by `test/lb_test.py`); added `cmd/server/Dockerfile` alongside the existing `cmd/backend/Dockerfile`.

**Gotchas to remember.**

- **The cookie's `Secure` flag means sticky sessions silently stop working over plain HTTP.** Fine in production behind TLS (terminated at the edge or by this process), but a browser hitting `http://localhost:8080` directly in local dev won't send the cookie back — every request looks cookie-less. `curl`/`httptest`-based testing is unaffected since nothing there enforces the attribute.
- **`passiveFailThreshold` and `unhealthyThreshold` are intentionally the same constant** (`passiveFailThreshold = unhealthyThreshold`) so the passive and active paths agree on how much evidence "unhealthy" requires. Don't let them drift apart without a reason.
- Hot-reload of the upstream list (SIGHUP or a config watcher) was considered and deliberately deferred — env-var config unblocks real deployment, but changing the pool still requires a restart.

---

*Future phases get appended here as the project grows.*
