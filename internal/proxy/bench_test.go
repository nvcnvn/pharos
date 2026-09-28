package proxy

import (
	"context"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/state"
)

// ARCHITECTURE §15: Pharos adds p99 < 2 ms before a 32 KiB chat request
// reaches a warm target (auth, body read, prefix hashing, routing; Done.Overhead,
// the value behind pharos_overhead_seconds). Reported, not asserted: compare
// p99-µs against the budget. The fake engine runs with no delays.
//
//	go test -run '^$' -bench Overhead ./internal/proxy
func BenchmarkOverhead(b *testing.B) {
	f := fake(b, engine.VLLM, "m")
	e := start(b, state.BackendSpec{URL: f.URL()})
	e.rounds(1)
	body := chatBody("m", strings.Repeat("a 32 KiB prompt ", 2<<10))
	var took []time.Duration
	b.ReportAllocs()
	for b.Loop() {
		resp := post(context.Background(), b, e.srv.URL+"/v1/chat/completions", body)
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b.Fatalf("status %d", resp.StatusCode)
		}
		took = append(took, (<-e.done).Overhead)
	}
	slices.Sort(took)
	at := func(p float64) float64 { return float64(took[int(p*float64(len(took)-1))].Nanoseconds()) / 1e3 }
	b.ReportMetric(at(0.5), "p50-µs")
	b.ReportMetric(at(0.99), "p99-µs")
}
