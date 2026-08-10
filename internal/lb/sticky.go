package lb

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const stickyCookieName = "LB_SESSION_ID"

// sessionTTL is how long a sticky-session mapping is kept after its last
// use. Without an expiry, the session map would grow without bound over the
// life of a long-running process as clients come and go.
const sessionTTL = 30 * time.Minute

// sessionSweepInterval controls how often expired sessions are purged.
const sessionSweepInterval = 5 * time.Minute

// maxSessions caps how many sticky-session mappings are kept at once. Every
// request without a valid session cookie (any cookie-less client — curl,
// SDKs, health probes, or an attacker looping requests) would otherwise
// allocate a permanent entry, growing the map without bound between sweeps.
// Once the cap is hit, new clients still get routed but fall back to
// stateless selection (no cookie is issued) until sweep() frees up room.
const maxSessions = 100_000

type session struct {
	upstream   *Upstream
	lastAccess atomic.Int64 // unix nanos; atomic so the read path only needs an RLock
}

// StickySession wraps a ServerPool and remembers which upstream each
// client was previously routed to, so repeat requests from the same
// client keep landing on the same upstream.
type StickySession struct {
	pool *ServerPool

	mu       sync.RWMutex
	sessions map[string]*session

	stop chan struct{}
	done chan struct{}
}

// NewStickySession wraps a pool with sticky-session routing.
func NewStickySession(pool *ServerPool) *StickySession {
	s := &StickySession{
		pool:     pool,
		sessions: make(map[string]*session),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	s.startSweeper()
	return s
}

// startSweeper periodically evicts sessions that haven't been used in
// sessionTTL, so the map doesn't grow forever as clients churn.
func (s *StickySession) startSweeper() {
	go func() {
		defer close(s.done)

		ticker := time.NewTicker(sessionSweepInterval)
		defer ticker.Stop()

		for {
			select {
			case <-s.stop:
				return
			case <-ticker.C:
				s.sweep()
			}
		}
	}()
}

// Stop halts the background sweeper goroutine and waits for it to exit, so
// callers can shut it down cleanly instead of leaking it for the life of
// the process.
func (s *StickySession) Stop() {
	close(s.stop)
	<-s.done
}

// sweep evicts expired sessions in two phases so it never holds the
// exclusive lock for the full map scan: expired keys are collected under a
// read lock (letting Route() keep serving concurrently), then deleted under
// a briefly-held write lock. Without this, a sweep over a large map would
// block every in-flight request for its entire duration.
func (s *StickySession) sweep() {
	cutoff := time.Now().Add(-sessionTTL).UnixNano()

	s.mu.RLock()
	var expired []string
	for id, sess := range s.sessions {
		if sess.lastAccess.Load() < cutoff {
			expired = append(expired, id)
		}
	}
	s.mu.RUnlock()

	if len(expired) == 0 {
		return
	}

	s.mu.Lock()
	for _, id := range expired {
		delete(s.sessions, id)
	}
	s.mu.Unlock()
}

// Route returns the upstream that should handle this request. If the
// client already has a valid session cookie, it returns the upstream that
// was recorded for it. Otherwise it asks the pool's algorithm for a fresh
// pick, records it, and sets a cookie on the response so future requests
// from this client come back here.
func (s *StickySession) Route(w http.ResponseWriter, r *http.Request) *Upstream {
	if cookie, err := r.Cookie(stickyCookieName); err == nil {
		s.mu.RLock()
		sess, ok := s.sessions[cookie.Value]
		if ok && sess.upstream.IsAlive() {
			sess.lastAccess.Store(time.Now().UnixNano())
			upstream := sess.upstream
			s.mu.RUnlock()
			return upstream
		}
		s.mu.RUnlock()

		if ok {
			// The upstream we'd previously pinned this client to has died.
			// Drop the stale mapping and fall through to pick a fresh one.
			s.mu.Lock()
			delete(s.sessions, cookie.Value)
			s.mu.Unlock()
		}
	}

	upstream := s.pool.NextUpstream()
	if upstream == nil {
		return nil
	}

	s.mu.Lock()
	full := len(s.sessions) >= maxSessions
	if !full {
		sessionID := generateSessionID()
		sess := &session{upstream: upstream}
		sess.lastAccess.Store(time.Now().UnixNano())
		s.sessions[sessionID] = sess
		s.mu.Unlock()

		http.SetCookie(w, &http.Cookie{
			Name:     stickyCookieName,
			Value:    sessionID,
			Path:     "/",
			MaxAge:   int(sessionTTL.Seconds()),
			HttpOnly: true,
			Secure:   true,
			SameSite: http.SameSiteLaxMode,
		})
		return upstream
	}
	s.mu.Unlock()

	// Session table is at capacity. Route this request without pinning it,
	// rather than letting the map grow without bound; it'll get a real
	// sticky session once sweep() frees up room.
	return upstream
}

// SessionCount reports how many sticky-session mappings are currently
// tracked. Exposed for observability (see /metrics in cmd/server).
func (s *StickySession) SessionCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.sessions)
}

func generateSessionID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
