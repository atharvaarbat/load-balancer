package lb

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPool_Failover_BodylessRequestRetriesOnFailure(t *testing.T) {
	pool, fakes := newFakePool(t, 3)
	target := pool.Upstreams()[0]
	fakes[0].setUp(false)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	target.ReverseProxy.ServeHTTP(w, r)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 after failing over to a healthy upstream", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "hello from") {
		t.Fatalf("unexpected body after failover: %q", body)
	}

	if !target.IsAlive() {
		t.Error("a single transient failure should not eject the upstream; ejection requires passiveFailThreshold consecutive failures")
	}
	if got := fakes[0].Hits(); got != 1 {
		t.Errorf("failed upstream got %d hits, want exactly 1 (the failed attempt, no repeat retries against it)", got)
	}
}

func TestPool_Failover_RequestWithBodyNotRetried(t *testing.T) {
	pool, fakes := newFakePool(t, 3)
	target := pool.Upstreams()[0]
	fakes[0].setUp(false)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("payload"))
	target.ReverseProxy.ServeHTTP(w, r)

	resp := w.Result()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 (a request with a body must not be retried elsewhere)", resp.StatusCode)
	}

	for i, f := range fakes[1:] {
		if got := f.Hits(); got != 0 {
			t.Errorf("upstream %d should never have been tried for a request with a body, got %d hits", i+1, got)
		}
	}
	if !target.IsAlive() {
		t.Error("a single transient failure should not eject the upstream, even for a request that wasn't retried")
	}
}

func TestPool_Failover_ClientDisconnectNotPenalized(t *testing.T) {
	pool, fakes := newFakePool(t, 3)
	target := pool.Upstreams()[0]
	fakes[0].setUp(false)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // simulate the client having already disconnected

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	target.ReverseProxy.ServeHTTP(w, r)

	if !target.IsAlive() {
		t.Error("upstream should not be penalized when the failure is due to the client already disconnecting")
	}
	if w.Body.Len() != 0 {
		t.Errorf("no response body should be written once the client has disconnected, got %q", w.Body.String())
	}
	for i, f := range fakes[1:] {
		if got := f.Hits(); got != 0 {
			t.Errorf("no failover retry should be attempted for a disconnected client; upstream %d got %d hits", i+1, got)
		}
	}
}

func TestPool_Failover_CascadesThroughWholePoolThenFails(t *testing.T) {
	pool, fakes := newFakePool(t, 3)
	for _, f := range fakes {
		f.setUp(false)
	}
	target := pool.Upstreams()[0]

	cascade := func() int {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		target.ReverseProxy.ServeHTTP(w, r)
		return w.Result().StatusCode
	}

	// First cascade: every upstream fails exactly once. Since a single
	// failure is below passiveFailThreshold, none should be ejected yet —
	// that's the fix for the bug where one blip removed a healthy upstream
	// from rotation. The tried-set on the request still guarantees the
	// retry chain visits each upstream at most once.
	if status := cascade(); status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 once every upstream has failed", status)
	}
	for i, f := range fakes {
		if got := f.Hits(); got != 1 {
			t.Errorf("upstream %d got %d hits after first cascade, want exactly 1 — the retry cap should try each upstream once, not loop", i, got)
		}
	}
	for i, u := range pool.Upstreams() {
		if !u.IsAlive() {
			t.Errorf("upstream %d should still be alive after a single failed attempt (passiveFailThreshold=%d)", i, passiveFailThreshold)
		}
	}

	// Second cascade: every upstream now accumulates its second consecutive
	// failure, crossing passiveFailThreshold, so this time all three should
	// be ejected.
	if status := cascade(); status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 on second cascade", status)
	}
	for i, f := range fakes {
		if got := f.Hits(); got != 2 {
			t.Errorf("upstream %d got %d hits after second cascade, want exactly 2", i, got)
		}
	}
	for i, u := range pool.Upstreams() {
		if u.IsAlive() {
			t.Errorf("upstream %d should be marked dead after %d consecutive failures", i, passiveFailThreshold)
		}
	}
}
