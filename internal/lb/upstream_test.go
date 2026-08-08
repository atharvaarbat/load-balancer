package lb

import "testing"

func TestNewUpstream_Valid(t *testing.T) {
	u, err := NewUpstream("http://localhost:9001")
	if err != nil {
		t.Fatalf("NewUpstream: %v", err)
	}
	if !u.IsAlive() {
		t.Error("new upstream should start alive")
	}
	if u.ReverseProxy == nil {
		t.Error("new upstream should have a configured ReverseProxy")
	}
}

func TestNewUpstream_InvalidURL(t *testing.T) {
	// "%zz" is not a valid percent-encoding escape, so url.Parse rejects it.
	if _, err := NewUpstream("http://%zz"); err == nil {
		t.Fatal("NewUpstream() with a malformed URL: want error, got nil")
	}
}

func TestUpstream_SetAlive(t *testing.T) {
	u, err := NewUpstream("http://localhost:9001")
	if err != nil {
		t.Fatalf("NewUpstream: %v", err)
	}

	u.SetAlive(false)
	if u.IsAlive() {
		t.Fatal("IsAlive() = true after SetAlive(false)")
	}

	u.SetAlive(true)
	if !u.IsAlive() {
		t.Fatal("IsAlive() = false after SetAlive(true)")
	}
}
