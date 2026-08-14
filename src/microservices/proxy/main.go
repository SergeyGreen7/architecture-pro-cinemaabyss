package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Proxy struct {
	monolith               *httputil.ReverseProxy
	movies                 *httputil.ReverseProxy
	events                 *httputil.ReverseProxy
	gradualMigration       bool
	moviesMigrationPercent int
}

func main() {
	port := getEnv("PORT", "8000")
	gradual := strings.EqualFold(getEnv("GRADUAL_MIGRATION", "true"), "true")
	moviesMigrationPercent, _ := strconv.Atoi(getEnv("MOVIES_MIGRATION_PERCENT", "0"))

	p := &Proxy{
		monolith:               httputil.NewSingleHostReverseProxy(mustURL(getEnv("MONOLITH_URL", "http://localhost:8080"))),
		movies:                 httputil.NewSingleHostReverseProxy(mustURL(getEnv("MOVIES_SERVICE_URL", "http://localhost:8081"))),
		events:                 httputil.NewSingleHostReverseProxy(mustURL(getEnv("EVENTS_SERVICE_URL", "http://localhost:8082"))),
		gradualMigration:       gradual,
		moviesMigrationPercent: moviesMigrationPercent,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Strangler Fig Proxy is healthy"))
	})
	mux.Handle("/", p)

	log.Printf("proxy listening on :%s", port)
	log.Printf("  /movies (gradual=%v, movies=%d%%)", gradual, moviesMigrationPercent)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasPrefix(r.URL.Path, "/api/movies"):
		if p.useMovies() {
			log.Printf("%s %s -> movies", r.Method, r.URL.Path)
			p.movies.ServeHTTP(w, r)
			return
		}
		log.Printf("%s %s -> monolith", r.Method, r.URL.Path)
		p.monolith.ServeHTTP(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/events"):
		log.Printf("%s %s -> events", r.Method, r.URL.Path)
		p.events.ServeHTTP(w, r)
	default:
		log.Printf("%s %s -> monolith", r.Method, r.URL.Path)
		p.monolith.ServeHTTP(w, r)
	}
}

// Strangler Fig: gradual on → percent to movies; gradual off → always movies.
func (p *Proxy) useMovies() bool {
	if !p.gradualMigration {
		return true
	}
	if p.moviesMigrationPercent <= 0 {
		return false
	}
	if p.moviesMigrationPercent >= 100 {
		return true
	}
	return rand.Intn(100) < p.moviesMigrationPercent
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func mustURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		log.Fatalf("invalid URL %q: %v", raw, err)
	}
	if u.Scheme == "" || u.Host == "" {
		log.Fatalf("invalid URL %q: must include scheme and host, e.g. http://localhost:8080", raw)
	}
	return u
}
