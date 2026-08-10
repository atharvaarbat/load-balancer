# load-balancer

This project is a small HTTP load balancer. It is written in Go. It is a learning project, built from scratch.

The load balancer sits in front of a pool of backend servers. It has these functions:

- It routes requests based on server load.
- It checks the health of each upstream server.
- It keeps a client connected to the same upstream server (sticky sessions).
- It switches to a different upstream server when a server fails (automatic failover).

## Features

### Power of Two Choices (P2C) routing

For each new session, the load balancer picks two upstream servers at random. It sends the request to the upstream server with fewer in-flight requests.

This method does not scan the full pool of upstream servers. It does not use a shared counter. As a result, the method takes constant time per request (O(1)).

Unlike round robin, this method avoids an upstream server that is slow or overloaded at that moment.

### Active health checking

The load balancer polls the `/health` endpoint of each upstream server. By default, it polls every 5 seconds.

The load balancer changes an upstream server's status only after 2 consecutive successes or 2 consecutive failures. This rule stops a single failed check from removing a healthy upstream server from the pool.

### Sticky sessions

The load balancer sets an `LB_SESSION_ID` cookie on a client's first request. This cookie pins the client to the upstream server that handled that request. Later requests from the same client go to the same upstream server. The cookie is `HttpOnly`, `Secure`, and `SameSite=Lax`, with a `Max-Age` matching the session TTL below.

If a session is idle for 30 minutes or more, the load balancer removes it during a periodic sweep. This sweep stops the session map from growing without limit. The session table also has a hard cap (100,000 entries); once full, new clients are still routed but without a pinned session, so a flood of cookie-less requests can't grow the map without bound.

If the pinned upstream server fails, the load balancer picks a new upstream server for the client.

### Automatic failover

A request to an upstream server can fail outright (connection refused) or come back with a 5xx status. When this happens, the load balancer:

1. Counts it as a consecutive failure for that upstream server. Only after 2 consecutive failures does it mark the server unhealthy — a single blip doesn't eject an otherwise-healthy server from the pool, matching the hysteresis the active health checker applies. Any successful response resets the count.
2. For a failed-outright request, retries it on a different, healthy upstream server not already tried by this request.

The load balancer retries a request at most once per remaining upstream server.

The load balancer does not retry a request in these cases:

- The request has a body.
- The client has already disconnected.

These rules stop the load balancer from resending a partial body. They also stop the load balancer from doing work for a request that no client is waiting for.

For the design rationale and history behind these decisions, see [JOURNEY.md](./JOURNEY.md).

### Observability

`GET /health` reflects real pool state: it returns 200 only while at least one upstream server is alive, so a readiness probe or DNS failover check can actually detect a fully-down load balancer.

`GET /metrics` exposes per-upstream alive status, in-flight count, and cumulative request/failure counters, plus the current sticky-session count, in Prometheus text-exposition format.

Every request is logged as one structured JSON line (method, path, status, duration, remote address) with a request ID that's generated if absent and propagated to the backend and back to the client via `X-Request-ID`.

## Project layout

```
cmd/
  server/    entry point for the load balancer (port 8080), plus its Dockerfile
  backend/   test backend used by the integration test harness
internal/lb/
  upstream.go    Upstream type: URL, reverse proxy, alive flag, in-flight/failure counters
  algorithm.go   Algorithm interface and the P2C implementation
  pool.go        ServerPool type: filters alive upstream servers, dispatches requests, handles failover
  health.go      HealthChecker type: polls /health on an interval, applies the consecutive-check rule
  sticky.go      StickySession type: manages session affinity with cookies
test/
  lb_test.py     Docker-based integration test harness
```

## Requirements

- Go, version 1.25.6 or later
- Docker (needed only for the integration test harness in `test/`, or to build the images in `cmd/server/Dockerfile` / `cmd/backend/Dockerfile`)
- Python 3 (needed only to run `test/lb_test.py`)

## Configuration

The load balancer is configured entirely through environment variables, so it can be redeployed with a different pool or listen address without a rebuild:

| Variable              | Default                                                             | Purpose                                    |
| --------------------- | -------------------------------------------------------------------- | ------------------------------------------- |
| `LB_ADDR`              | `:8080`                                                              | Listen address                              |
| `LB_UPSTREAMS`         | `http://localhost:9001,http://localhost:9002,http://localhost:9003` | Comma-separated upstream URLs               |
| `LB_HEALTH_INTERVAL`   | `5s`                                                                 | Active health-check polling interval        |
| `LB_SHUTDOWN_TIMEOUT`  | `15s`                                                                | How long graceful shutdown waits to drain   |

## How to run the load balancer

Step 1: Start three backend instances, on ports 9001, 9002, and 9003.

```sh
PORT=9001 go run ./cmd/backend
PORT=9002 go run ./cmd/backend
PORT=9003 go run ./cmd/backend
```

Step 2: Start the load balancer.

```sh
go run ./cmd/server
```

The load balancer listens on port 8080 (or `LB_ADDR`). It proxies requests on `/` to an upstream server, reports pool health on `/health`, and exposes Prometheus-format metrics on `/metrics`. It shuts down gracefully on SIGINT/SIGTERM, draining in-flight requests before exiting.

## How to test the load balancer

### Unit and integration tests

Run this command to run the unit and integration tests for the `internal/lb` package:

```sh
go test ./...
```

### End-to-end test

There is also a Docker-based end-to-end test harness. This harness does these tasks, in order:

1. It builds the backend image.
2. It starts three backend containers.
3. It builds and starts the load balancer binary.
4. It runs a smoke test against the load balancer.

Run this command to run the end-to-end test:

```sh
python test/lb_test.py
```

Note: Before you run this test, stop any load balancer started with `go run ./cmd/server`. The test harness needs port 8080 to be free.

## How the load balancer routes a request

1. `StickySession.Route` checks the request for an `LB_SESSION_ID` cookie.
2. If the cookie is present and the pinned upstream server is alive, the load balancer sends the request to that server. Go to step 6.
3. If the cookie is not present, or the pinned upstream server is not alive, `ServerPool.NextUpstream` filters the pool to alive upstream servers.
4. The `P2C` algorithm picks one upstream server from the filtered pool.
5. The load balancer sets a new session cookie on the response.
6. `ServerPool.Serve` increments the in-flight counter of the chosen upstream server.
7. `ServerPool.Serve` proxies the request to the chosen upstream server.
8. `ServerPool.Serve` decrements the in-flight counter when the request is done. This step uses `defer`, so the counter is decremented on every exit path.
9. If the proxied request fails outright or returns a 5xx, the load balancer counts it toward that upstream server's consecutive-failure streak (marking it unhealthy once the streak crosses the threshold) and, for a request that failed outright, retries it on a different, healthy upstream server. The retry rules in the Automatic Failover section apply.
