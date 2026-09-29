package discovery

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/state"
)

// testdata/docker-29.1.3-containers.json is GET /containers/json filtered on
// pharos.enable=true with one ollama/ollama:0.34.4 container, run as
// `docker run -l pharos.enable=true -l pharos.memory_gb=8 ollama/ollama:0.34.4`.
func recordedContainer(t *testing.T) container {
	t.Helper()
	b, err := os.ReadFile("testdata/docker-29.1.3-containers.json")
	if err != nil {
		t.Fatal(err)
	}
	var cs []container
	if err := json.Unmarshal(b, &cs); err != nil || len(cs) != 1 {
		t.Fatalf("%d containers, %v", len(cs), err)
	}
	return cs[0]
}

const recordedID = "358f252335fb487678d2ca0787325f545b6f42b02493739b0bb39c500686c608"

func TestLabelSpec(t *testing.T) {
	base := recordedContainer(t)
	with := func(labels map[string]string, edit func(*container)) container {
		c := base
		c.Labels = map[string]string{}
		for k, v := range base.Labels {
			c.Labels[k] = v
		}
		for k, v := range labels {
			c.Labels[k] = v
		}
		if edit != nil {
			edit(&c)
		}
		return c
	}
	for _, tt := range []struct {
		name string
		c    container
		want state.BackendSpec
		err  string
	}{
		{"zero_config_uses_the_one_exposed_port_and_the_container_ip", base,
			state.BackendSpec{URL: "http://172.17.0.2:11434", Kind: engine.Auto, MemoryBytes: 8 << 30, Logs: "docker://" + recordedID}, ""},
		{"url_label_wins", with(map[string]string{"pharos.url": "http://127.0.0.1:11434/", "pharos.kind": "ollama", "pharos.capacity": "4"}, nil),
			state.BackendSpec{URL: "http://127.0.0.1:11434", Kind: engine.Ollama, MemoryBytes: 8 << 30, Capacity: 4, Logs: "docker://" + recordedID}, ""},
		{"port_label_picks_among_several", with(map[string]string{"pharos.port": "8000"}, func(c *container) {
			c.Ports = append(c.Ports, c.Ports[0])
			c.Ports[1].PrivatePort = 8000
		}), state.BackendSpec{URL: "http://172.17.0.2:8000", Kind: engine.Auto, MemoryBytes: 8 << 30, Logs: "docker://" + recordedID}, ""},
		{"several_ports_need_a_port_label", with(nil, func(c *container) {
			c.Ports = append(c.Ports, c.Ports[0])
			c.Ports[1].PrivatePort = 8000
		}), state.BackendSpec{}, "2 exposed TCP ports"},
		{"no_address_needs_a_url_label", with(nil, func(c *container) { c.NetworkSettings.Networks = nil }), state.BackendSpec{}, "no network address"},
		{"unknown_kind", with(map[string]string{"pharos.kind": "tgi"}, nil), state.BackendSpec{}, "unknown kind"},
		{"bad_numbers", with(map[string]string{"pharos.memory_gb": "lots", "pharos.capacity": "-1", "pharos.port": "0"}, nil),
			state.BackendSpec{}, "pharos.memory_gb"},
		{"bad_url", with(map[string]string{"pharos.url": "ollama:11434"}, nil), state.BackendSpec{}, "pharos.url"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := labelSpec(tt.c)
			if tt.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.err) {
					t.Errorf("err = %v, want it to mention %q", err, tt.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.URL != tt.want.URL || got.Kind != tt.want.Kind || got.MemoryBytes != tt.want.MemoryBytes ||
				got.Capacity != tt.want.Capacity || got.Logs != tt.want.Logs {
				t.Errorf("got %+v\nwant %+v", got, tt.want)
			}
		})
	}
}

// fakeDocker serves the recorded list and event bodies on a unix socket. The
// test sets the list and pushes events one at a time.
type fakeDocker struct {
	mu     sync.Mutex
	list   string
	events chan string
}

func startFakeDocker(t *testing.T) *fakeDocker {
	t.Helper()
	dir, err := os.MkdirTemp("", "pd") // short: unix socket paths are limited
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "d.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeDocker{list: "[]", events: make(chan string)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /containers/json", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("filters") != labelFilter {
			t.Errorf("list filters %q", r.URL.Query().Get("filters"))
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		w.Write([]byte(f.list))
	})
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		for {
			select {
			case <-r.Context().Done():
				return
			case ev := <-f.events:
				w.Write([]byte(ev + "\n"))
				w.(http.Flusher).Flush()
			}
		}
	})
	srv := &http.Server{Handler: mux}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	t.Setenv("DOCKER_HOST", "unix://"+sock)
	return f
}

func (f *fakeDocker) setList(body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.list = body
}

func TestDockerLabelsFollowsContainerEvents(t *testing.T) {
	f := startFakeDocker(t)
	containers, err := os.ReadFile("testdata/docker-29.1.3-containers.json")
	if err != nil {
		t.Fatal(err)
	}
	evFile, err := os.Open("testdata/docker-29.1.3-events.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer evFile.Close()
	events := map[string]string{} // action -> recorded line
	for sc := bufio.NewScanner(evFile); sc.Scan(); {
		var ev struct{ Action string }
		json.Unmarshal(sc.Bytes(), &ev)
		events[ev.Action] = sc.Text()
	}

	updates := make(chan []state.BackendSpec, 10)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	go func() { done <- DockerLabels(ctx, func(s []state.BackendSpec) { updates <- s }) }()
	next := func(what string) []state.BackendSpec {
		t.Helper()
		select {
		case s := <-updates:
			return s
		case <-time.After(5 * time.Second):
			t.Fatalf("no update after %s", what)
			return nil
		}
	}

	if s := next("start"); len(s) != 0 {
		t.Errorf("before any container: %v", s)
	}
	f.setList(string(containers))
	f.events <- events["start"]
	if s := next("start event"); len(s) != 1 || s[0].URL != "http://172.17.0.2:11434" || s[0].Logs != "docker://"+recordedID {
		t.Errorf("after start: %+v", s)
	}
	// Labels that don't make a backend leave the container out.
	f.setList(strings.Replace(string(containers), `"pharos.memory_gb":"8"`, `"pharos.memory_gb":"8","pharos.kind":"tgi"`, 1))
	f.events <- events["create"]
	if s := next("bad labels"); len(s) != 0 {
		t.Errorf("container with an unknown kind: %+v", s)
	}
	f.setList("[]")
	f.events <- events["die"]
	if s := next("die event"); len(s) != 0 {
		t.Errorf("after die: %+v", s)
	}
	cancel()
	if err := <-done; err != context.Canceled {
		t.Errorf("DockerLabels returned %v", err)
	}
}
