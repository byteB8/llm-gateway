package fakellm

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
)

// Fault is misbehaviour injected at runtime through POST /fault, so experiments can make a
// running backend slow, failing or flaky without restarting it. The zero value is healthy.
type Fault struct {
	ExtraLatencyMs int     `json:"extra_latency_ms"` // added before the first token
	ErrorRate      float64 `json:"error_rate"`       // share of requests answered with 503
	CutRate        float64 `json:"cut_rate"`         // share of streams dropped after the first token
}

func (f Fault) validate() error {
	if f.ExtraLatencyMs < 0 || f.ErrorRate < 0 || f.ErrorRate > 1 || f.CutRate < 0 || f.CutRate > 1 {
		return fmt.Errorf("latency must be >= 0 and rates in [0, 1]: %+v", f)
	}
	return nil
}

func (s *Server) currentFault() Fault {
	if f := s.fault.Load(); f != nil {
		return *f
	}
	return Fault{}
}

// SetFault replaces the injected fault (the zero Fault clears it).
func (s *Server) SetFault(f Fault) error {
	if err := f.validate(); err != nil {
		return err
	}
	s.fault.Store(&f)
	return nil
}

func (s *Server) faultHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var f Fault
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&f); err != nil {
			http.Error(w, "bad fault: "+err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.SetFault(f); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.currentFault())
}

func chance(p float64) bool { return p > 0 && rand.Float64() < p }
