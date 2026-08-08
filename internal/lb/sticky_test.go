package lb

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestStickySession_PinsToSameUpstream(t *testing.T) {
	pool, _ := newFakePool(t, 3)
	sticky := NewStickySession(pool)

	w1 := httptest.NewRecorder()
	r1 := httptest.NewRequest(http.MethodGet, "/", nil)
	first := sticky.Route(w1, r1)
	if first == nil {
		t.Fatal("Route() returned nil on first request")
	}

	cookies := w1.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != stickyCookieName {
		t.Fatalf("expected exactly one %s cookie to be set, got %v", stickyCookieName, cookies)
	}

	for range 5 {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.AddCookie(cookies[0])
		if got := sticky.Route(w, r); got != first {
			t.Fatalf("Route() = %v on repeat request, want pinned upstream %v", got, first)
		}
	}
}

func TestStickySession_DeadUpstreamFallsThrough(t *testing.T) {
	pool, _ := newFakePool(t, 3)
	sticky := NewStickySession(pool)

	w1 := httptest.NewRecorder()
	r1 := httptest.NewRequest(http.MethodGet, "/", nil)
	pinned := sticky.Route(w1, r1)
	cookie := w1.Result().Cookies()[0]

	pinned.SetAlive(false)

	w2 := httptest.NewRecorder()
	r2 := httptest.NewRequest(http.MethodGet, "/", nil)
	r2.AddCookie(cookie)
	got := sticky.Route(w2, r2)

	if got == nil {
		t.Fatal("Route() returned nil, want a fresh alive upstream")
	}
	if got == pinned {
		t.Fatal("Route() returned the dead pinned upstream instead of falling through to a healthy one")
	}

	sticky.mu.Lock()
	_, stillPresent := sticky.sessions[cookie.Value]
	sticky.mu.Unlock()
	if stillPresent {
		t.Error("stale session mapping to the dead upstream should have been deleted")
	}
}

func TestStickySession_UnknownCookieGetsFreshSession(t *testing.T) {
	pool, _ := newFakePool(t, 3)
	sticky := NewStickySession(pool)

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.AddCookie(&http.Cookie{Name: stickyCookieName, Value: "never-issued-session-id"})

	got := sticky.Route(w, r)
	if got == nil {
		t.Fatal("Route() returned nil for an unrecognized cookie")
	}

	cookies := w.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Value == "never-issued-session-id" {
		t.Fatalf("expected a fresh session cookie to be issued for an unrecognized id, got %v", cookies)
	}
}

func TestStickySession_SweepEvictsExpiredSessions(t *testing.T) {
	pool, _ := newFakePool(t, 2)
	sticky := NewStickySession(pool)
	upstreams := pool.Upstreams()

	stale := &session{upstream: upstreams[0]}
	stale.lastAccess.Store(time.Now().Add(-sessionTTL - time.Minute).UnixNano())

	fresh := &session{upstream: upstreams[1]}
	fresh.lastAccess.Store(time.Now().UnixNano())

	sticky.mu.Lock()
	sticky.sessions["stale-id"] = stale
	sticky.sessions["fresh-id"] = fresh
	sticky.mu.Unlock()

	sticky.sweep()

	sticky.mu.RLock()
	_, staleStillThere := sticky.sessions["stale-id"]
	_, freshStillThere := sticky.sessions["fresh-id"]
	sticky.mu.RUnlock()

	if staleStillThere {
		t.Error("sweep() should have evicted the session past sessionTTL")
	}
	if !freshStillThere {
		t.Error("sweep() should not evict a recently-used session")
	}
}
