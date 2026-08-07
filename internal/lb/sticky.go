package lb

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"sync"
)

const stickyCookieName = "LB_SESSION_ID"

// StickySession wraps a ServerPool and remembers which upstream each
// client was previously routed to, so repeat requests from the same
// client keep landing on the same upstream.
type StickySession struct {
	pool *ServerPool

	mu       sync.RWMutex
	sessions map[string]*Upstream
}

// NewStickySession wraps a pool with sticky-session routing.
func NewStickySession(pool *ServerPool) *StickySession {
	return &StickySession{
		pool:     pool,
		sessions: make(map[string]*Upstream),
	}
}

// Route returns the upstream that should handle this request. If the
// client already has a valid session cookie, it returns the upstream that
// was recorded for it. Otherwise it asks the pool's algorithm for a fresh
// pick, records it, and sets a cookie on the response so future requests
// from this client come back here.
func (s *StickySession) Route(w http.ResponseWriter, r *http.Request) *Upstream {
	if cookie, err := r.Cookie(stickyCookieName); err == nil {
		s.mu.RLock()
		upstream, ok := s.sessions[cookie.Value]
		s.mu.RUnlock()
		if ok {
			return upstream
		}
	}

	upstream := s.pool.NextUpstream()

	sessionID := generateSessionID()
	s.mu.Lock()
	s.sessions[sessionID] = upstream
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
