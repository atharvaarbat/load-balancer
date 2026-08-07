package main

import (
	"log"
	"net/http"

	"github.com/atharvaarbat/load-balancer/internal/lb"
)

func main() {
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})

	upstreamURLs := []string{
		"http://localhost:9001",
		"http://localhost:9002",
		"http://localhost:9003",
	}

	var upstreams []*lb.Upstream
	for _, raw := range upstreamURLs {
		u, err := lb.NewUpstream(raw)
		if err != nil {
			log.Fatal(err)
		}
		upstreams = append(upstreams, u)
	}

	pool := lb.NewServerPool(upstreams, &lb.RoundRobin{})
	sticky := lb.NewStickySession(pool)

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		upstream := sticky.Route(w, r)
		upstream.ReverseProxy.ServeHTTP(w, r)
	})

	log.Println("Load Balancer running on :8080")
	err := http.ListenAndServe(":8080", mux)

	if err != nil {
		log.Fatal(err)
	}
}
