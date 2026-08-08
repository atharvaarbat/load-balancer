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

	if target.IsAlive() {
		t.Error("upstream that failed the request should be marked dead")
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
	if target.IsAlive() {
		t.Error("target upstream should be marked dead even though the request wasn't retried")
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

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	target.ReverseProxy.ServeHTTP(w, r)

	resp := w.Result()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 once every upstream has failed", resp.StatusCode)
	}

	for i, f := range fakes {
		if got := f.Hits(); got != 1 {
			t.Errorf("upstream %d got %d hits, want exactly 1 — the retry cap should try each upstream once, not loop", i, got)
		}
	}
	for i, u := range pool.Upstreams() {
		if u.IsAlive() {
			t.Errorf("upstream %d should be marked dead after failing", i)
		}
	}
}
