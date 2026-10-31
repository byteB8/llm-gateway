// Command alertsink is an Alertmanager webhook receiver that records every notification with the
// time it arrived. Experiments read GET /alerts to measure how long an alert took to fire after a
// fault was injected, and whether anything fired when nothing was wrong.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"sync"
	"time"
)

type alert struct {
	Status   string            `json:"status"`
	Labels   map[string]string `json:"labels"`
	StartsAt time.Time         `json:"startsAt"`
	EndsAt   time.Time         `json:"endsAt"`
}

type received struct {
	ReceivedAt time.Time         `json:"received_at"`
	Status     string            `json:"status"` // firing | resolved
	Alertname  string            `json:"alertname"`
	Severity   string            `json:"severity"`
	StartsAt   time.Time         `json:"starts_at"`
	Labels     map[string]string `json:"labels"`
}

func main() {
	addr := flag.String("listen", ":8080", "listen address")
	flag.Parse()
	var (
		mu      sync.Mutex
		history []received
	)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /alerts", func(w http.ResponseWriter, r *http.Request) {
		var msg struct {
			Alerts []alert `json:"alerts"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&msg); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		now := time.Now().UTC()
		mu.Lock()
		for _, a := range msg.Alerts {
			rec := received{ReceivedAt: now, Status: a.Status, Alertname: a.Labels["alertname"],
				Severity: a.Labels["severity"], StartsAt: a.StartsAt, Labels: a.Labels}
			history = append(history, rec)
			b, _ := json.Marshal(rec)
			log.Printf("%s", b)
		}
		mu.Unlock()
	})
	mux.HandleFunc("GET /alerts", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(history)
	})
	mux.HandleFunc("DELETE /alerts", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		history = nil
		mu.Unlock()
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	log.Printf("alertsink listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
