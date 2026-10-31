// Command otlpsink accepts OTLP/HTTP trace exports, counts the spans and throws them away. The
// overhead benchmark (bench/sustain/e08.sh) points the gateway's exporter here, so the gateway pays
// the full cost of building, encoding and sending its spans while nothing downstream is measured.
package main

import (
	"compress/gzip"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"
	"sync/atomic"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func main() {
	addr := flag.String("listen", "127.0.0.1:4318", "listen address")
	flag.Parse()
	var spans, exports atomic.Int64
	ok, _ := proto.Marshal(&coltracepb.ExportTraceServiceResponse{})
	http.HandleFunc("POST /v1/traces", func(w http.ResponseWriter, r *http.Request) {
		var body io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			zr, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			body = zr
		}
		b, err := io.ReadAll(body)
		var req coltracepb.ExportTraceServiceRequest
		if err == nil {
			err = proto.Unmarshal(b, &req)
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		for _, rs := range req.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				spans.Add(int64(len(ss.Spans)))
			}
		}
		exports.Add(1)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Write(ok)
	})
	http.HandleFunc("GET /count", func(w http.ResponseWriter, _ *http.Request) {
		json.NewEncoder(w).Encode(map[string]int64{"spans": spans.Load(), "exports": exports.Load()})
	})
	log.Printf("otlpsink listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
