package lb

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"sync"
	"time"
)

const stickyCookieName = "LB_SESSION_ID"

// sessionTTL is how long a sticky-session mapping is kept after its last
// use. Without an expiry, the session map would grow without bound over the
// life of a long-running process as clients come and go.
const sessionTTL = 30 * time.Minute

// sessionSweepInterval controls how often expired sessions are purged.
const sessionSweepInterval = 5 * time.Minute

type session struct {
	upstream   *Upstream
	lastAccess time.Time
}

// StickySession wraps a ServerPool and remembers which upstream each
// client was previously routed to, so repeat requests from the same
// client keep landing on the same upstream.
type StickySession struct {
	pool *ServerPool

	mu       sync.Mutex
	sessions map[string]*session
}

// NewStickySession wraps a pool with sticky-session routing.
func NewStickySession(pool *ServerPool) *StickySession {
	s := &StickySession{
		pool:     pool,
		sessions: make(map[string]*session),
	}
	s.startSweeper()
	return s
}

// startSweeper periodically evicts sessions that haven't been used in
// sessionTTL, so the map doesn't grow forever as clients churn.
func (s *StickySession) startSweeper() {
	go func() {
		ticker := time.NewTicker(sessionSweepInterval)
		defer ticker.Stop()

		for range ticker.C {
			s.sweep()
		}
	}()
}

func (s *StickySession) sweep() {
	cutoff := time.Now().Add(-sessionTTL)

	s.mu.Lock()
	defer s.mu.Unlock()

	for id, sess := range s.sessions {
		if sess.lastAccess.Before(cutoff) {
			delete(s.sessions, id)
		}
	}
}

// Route returns the upstream that should handle this request. If the
// client already has a valid session cookie, it returns the upstream that
// was recorded for it. Otherwise it asks the pool's algorithm for a fresh
// pick, records it, and sets a cookie on the response so future requests
// from this client come back here.
func (s *StickySession) Route(w http.ResponseWriter, r *http.Request) *Upstream {
	if cookie, err := r.Cookie(stickyCookieName); err == nil {
		s.mu.Lock()
		sess, ok := s.sessions[cookie.Value]
		if ok && sess.upstream.IsAlive() {
			sess.lastAccess = time.Now()
			upstream := sess.upstream
			s.mu.Unlock()
			return upstream
		}
		if ok {
			// The upstream we'd previously pinned this client to has died.
			// Drop the stale mapping and fall through to pick a fresh one.
			delete(s.sessions, cookie.Value)
		}
		s.mu.Unlock()
	}

	upstream := s.pool.NextUpstream()
	if upstream == nil {
		return nil
	}

	sessionID := generateSessionID()
	s.mu.Lock()
	s.sessions[sessionID] = &session{upstream: upstream, lastAccess: time.Now()}
	s.mu.Unlock()

	http.SetCookie(w, &http.Cookie{
		Name:     stickyCookieName,
		Value:    sessionID,
		Path:     "/",
		HttpOnly: true,
	})

	return upstream
}

func generateSessionID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
