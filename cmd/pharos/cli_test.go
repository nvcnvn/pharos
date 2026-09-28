package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/fakeengine"
)

// The operator's path from `pharos keys new` to a working key: paste the
// printed entry under keys:, start serve, and the printed key is let in while
// others aren't. The key itself is in no file.
func TestKeysNewPrintsAKeyThatWorks(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	newKey := func(name string) (key, entry string) {
		var out bytes.Buffer
		if err := keys([]string{"new", "-name", name}, &out, io.Discard); err != nil {
			t.Fatal(err)
		}
		m := regexp.MustCompile(`(?s)^key \(shown once; give it to \w+\): (\S+)\n\nadd to keys: in the config:\n(.*)$`).FindStringSubmatch(out.String())
		if m == nil {
			t.Fatalf("output:\n%s", out.String())
		}
		return m[1], m[2]
	}
	alice, aliceEntry := newKey("alice")
	bob, bobEntry := newKey("bob")
	if alice == bob {
		t.Fatal("two keys new printed the same key")
	}

	f := fakeengine.New(fakeengine.Config{Kind: engine.VLLM, Models: []fakeengine.Model{{Name: "m"}}})
	defer f.Close()
	addr := freePorts(t, 1)[0]
	cfg := filepath.Join(t.TempDir(), "pharos.yaml")
	yaml := fmt.Sprintf("listen: %s\ndrain: {grace: 10ms}\nbackends: [{url: %q}]\nkeys:\n%s", addr, f.URL(), aliceEntry)
	if err := os.WriteFile(cfg, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(cfg)
	if bytes.Contains(b, []byte(alice)) {
		t.Fatal("the config holds the key itself")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, []string{"-config", cfg}, io.Discard) }()
	defer func() {
		// A connection the client dialed but never used holds Shutdown for 5 s.
		http.DefaultClient.CloseIdleConnections()
		cancel()
		if err := <-done; err != nil {
			t.Errorf("serve: %v", err)
		}
	}()
	waitFor(t, "healthy", func() bool { return healthy(addr) })
	for key, want := range map[string]int{alice: http.StatusOK, bob: http.StatusUnauthorized, "": http.StatusUnauthorized} {
		if status, _ := send(t, addr, key, "m"); status != want {
			t.Errorf("key %.8s…: %d, want %d", key, status, want)
		}
	}
	if !strings.Contains(bobEntry, "name: bob") {
		t.Errorf("entry for bob:\n%s", bobEntry)
	}
}

// doctor is how an operator learns what Pharos reads from each backend: the
// detected kind and version, which probe fills each signal, why the others
// were dropped, and which signals are unknown (never shown as 0).
func TestDoctorShowsThePlanAndWhatIsUnknown(t *testing.T) {
	cases := []struct {
		kind    engine.Kind
		version string
		probes  map[string]string // signal -> the probe doctor names for it
		dropped map[string]string // probe -> reason
		unknown string
	}{
		{engine.Ollama, "0.34.4",
			map[string]string{"residency": "ollama-ps-residency", "size_bytes": "ollama-tags-size", "models": "openai-models"},
			map[string]string{"ollama-log-num-parallel": "no log feed"},
			"running, waiting, capacity, kv_usage"},
		{engine.LlamaCpp, "b11146-7fe450e19",
			map[string]string{"running": "llamacpp-running", "waiting": "llamacpp-waiting", "capacity": "llamacpp-props-total-slots"},
			map[string]string{"llamacpp-slots-is-processing": "redundant"},
			"residency, vram_bytes, size_bytes, kv_usage"},
		{engine.VLLM, "0.30.0",
			map[string]string{"running": "vllm-running", "waiting": "vllm-waiting", "kv_usage": "vllm-kv-cache-usage-perc"},
			nil,
			"residency, vram_bytes, size_bytes, capacity"},
	}
	for _, c := range cases {
		t.Run(string(c.kind), func(t *testing.T) {
			f := fakeengine.New(fakeengine.Config{Kind: c.kind, Models: []fakeengine.Model{{Name: "m", SizeBytes: 1 << 30}}})
			defer f.Close()
			var out bytes.Buffer
			if err := doctor([]string{"-url", f.URL()}, &out, io.Discard); err != nil {
				t.Fatalf("%v\n%s", err, out.String())
			}
			rows := map[string][]string{} // first field (or signal) -> the rest; the header's version comes before the signal's
			for line := range strings.Lines(out.String()) {
				fields := strings.Fields(line)
				if len(fields) > 0 && (fields[0] == "active" || fields[0] == "dropped") {
					fields = fields[1:]
				}
				if len(fields) < 2 {
					continue
				}
				if _, seen := rows[fields[0]]; !seen {
					rows[fields[0]] = fields[1:]
				}
			}
			check := func(what, key, want string) {
				if got := strings.Join(rows[key], " "); !strings.HasPrefix(got, want) {
					t.Errorf("%s %s: %q, want %q\n%s", what, key, got, want, out.String())
				}
			}
			check("kind", "kind", string(c.kind)+" (detected)")
			check("version", "version", c.version)
			for sig, probe := range c.probes {
				check("signal", sig, probe)
			}
			for probe, reason := range c.dropped {
				check("dropped", probe, reason)
			}
			check("signals", "unknown", c.unknown)
		})
	}
}

// Checking a whole config: a backend that doesn't answer is reported and
// fails the command, and the others are still shown.
func TestDoctorReportsABackendThatDoesNotAnswer(t *testing.T) {
	f := fakeengine.New(fakeengine.Config{Kind: engine.VLLM, Models: []fakeengine.Model{{Name: "m"}}})
	defer f.Close()
	dead := "http://" + freePorts(t, 1)[0] // nothing listens there
	cfg := filepath.Join(t.TempDir(), "pharos.yaml")
	os.WriteFile(cfg, fmt.Appendf(nil, "backends: [{url: %q}, {url: %q}]\n", dead, f.URL()), 0o600)
	var out bytes.Buffer
	err := doctor([]string{"-config", cfg, "-timeout", "2s"}, &out, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "1 of 2 backends did not answer") {
		t.Errorf("err %v", err)
	}
	if s := out.String(); !strings.Contains(s, "== "+dead+"\n  error:") || !strings.Contains(s, "vllm (detected)") {
		t.Errorf("output:\n%s", s)
	}
}
