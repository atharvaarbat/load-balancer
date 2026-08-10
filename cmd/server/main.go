package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/atharvaarbat/load-balancer/internal/lb"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	var upstreams []*lb.Upstream
	for _, raw := range upstreamURLs() {
		u, err := lb.NewUpstream(raw)
		if err != nil {
			logger.Error("invalid upstream configuration", "url", raw, "error", err)
			os.Exit(1)
		}
		upstreams = append(upstreams, u)
	}
	if len(upstreams) == 0 {
		logger.Error("no upstreams configured; set LB_UPSTREAMS to a comma-separated list of URLs")
		os.Exit(1)
	}

	pool := lb.NewServerPool(upstreams, &lb.P2C{})

	sticky := lb.NewStickySession(pool)
	defer sticky.Stop()

	healthChecker := lb.NewHealthChecker(pool, healthInterval())
	healthChecker.Start()
	defer healthChecker.Stop()

	mux := http.NewServeMux()

	// Reflects real pool state: an orchestrator's readiness probe (or a
	// DNS/LB failover check) needs this to go unhealthy when there's
	// nothing left to route to, or it'll keep sending traffic here forever.
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		if pool.NextUpstream() == nil {
			http.Error(w, "no healthy upstreams", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok"))
	})

	mux.HandleFunc("/metrics", metricsHandler(pool, sticky))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		upstream := sticky.Route(w, r)
		if upstream == nil {
			http.Error(w, "no healthy upstreams available", http.StatusServiceUnavailable)
			return
		}
		pool.Serve(w, r, upstream)
	})

	addr := listenAddr()
	srv := &http.Server{
		Addr:    addr,
		Handler: withRequestID(withAccessLog(logger, mux)),
		// Without these, the load balancer has no protection against a
		// slow-loris client (one that opens a connection and trickles
		// bytes, or never reads the response) tying up a goroutine and fd
		// forever.
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	go func() {
		logger.Info("load balancer listening", "addr", addr, "upstreams", len(upstreams))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	logger.Info("shutdown signal received, draining connections")
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout())
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("graceful shutdown did not complete cleanly", "error", err)
	}
}

// upstreamURLs returns the configured upstream server URLs, read from the
// comma-separated LB_UPSTREAMS environment variable so the pool can be
// resized without a rebuild. Falls back to the three-backend local dev
// setup when unset.
func upstreamURLs() []string {
	raw := os.Getenv("LB_UPSTREAMS")
	if raw == "" {
		return []string{
			"http://localhost:9001",
			"http://localhost:9002",
			"http://localhost:9003",
		}
	}

	var urls []string
	for s := range strings.SplitSeq(raw, ",") {
		if s = strings.TrimSpace(s); s != "" {
			urls = append(urls, s)
		}
	}
	return urls
}

func listenAddr() string {
	if v := os.Getenv("LB_ADDR"); v != "" {
		return v
	}
	return ":8080"
}

func healthInterval() time.Duration {
	return durationEnv("LB_HEALTH_INTERVAL", 5*time.Second)
}

func shutdownTimeout() time.Duration {
	return durationEnv("LB_SHUTDOWN_TIMEOUT", 15*time.Second)
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

// withRequestID assigns each request an ID (or preserves one already set by
// an upstream caller) and propagates it to the backend and back to the
// client, so a single request can be traced across logs on both sides.
func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = generateRequestID()
			r.Header.Set("X-Request-ID", id)
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r)
	})
}

func generateRequestID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// statusRecorder captures the status code a handler wrote, since
// http.ResponseWriter doesn't expose it after the fact.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// withAccessLog logs one structured line per request. This is the only
// visibility an operator has into request rate, status-code distribution,
// and latency without a metrics scraper attached.
func withAccessLog(logger *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rec, r)

		logger.Info("request",
			"request_id", r.Header.Get("X-Request-ID"),
			"method", r.Method,
			"path", r.URL.Path,
			"status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(),
			"remote_addr", r.RemoteAddr,
		)
	})
}

// metricsHandler exposes per-upstream counters in Prometheus text-exposition
// format without pulling in the client library, keeping the project
// dependency-free.
func metricsHandler(pool *lb.ServerPool, sticky *lb.StickySession) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		fmt.Fprintln(w, "# HELP lb_upstream_alive Whether the upstream is currently considered healthy.")
		fmt.Fprintln(w, "# TYPE lb_upstream_alive gauge")
		for _, u := range pool.Upstreams() {
			alive := 0
			if u.IsAlive() {
				alive = 1
			}
			fmt.Fprintf(w, "lb_upstream_alive{upstream=%q} %d\n", u.URL.String(), alive)
		}

		fmt.Fprintln(w, "# HELP lb_upstream_inflight In-flight requests currently dispatched to the upstream.")
		fmt.Fprintln(w, "# TYPE lb_upstream_inflight gauge")
		for _, u := range pool.Upstreams() {
			fmt.Fprintf(w, "lb_upstream_inflight{upstream=%q} %d\n", u.URL.String(), u.Inflight())
		}

		fmt.Fprintln(w, "# HELP lb_upstream_requests_total Total requests dispatched to the upstream.")
		fmt.Fprintln(w, "# TYPE lb_upstream_requests_total counter")
		for _, u := range pool.Upstreams() {
			fmt.Fprintf(w, "lb_upstream_requests_total{upstream=%q} %d\n", u.URL.String(), u.RequestsTotal())
		}

		fmt.Fprintln(w, "# HELP lb_upstream_failures_total Total failed requests (transport errors or 5xx) for the upstream.")
		fmt.Fprintln(w, "# TYPE lb_upstream_failures_total counter")
		for _, u := range pool.Upstreams() {
			fmt.Fprintf(w, "lb_upstream_failures_total{upstream=%q} %d\n", u.URL.String(), u.FailuresTotal())
		}

		fmt.Fprintln(w, "# HELP lb_sticky_sessions Current number of tracked sticky-session mappings.")
		fmt.Fprintln(w, "# TYPE lb_sticky_sessions gauge")
		fmt.Fprintf(w, "lb_sticky_sessions %d\n", sticky.SessionCount())
	}
}
