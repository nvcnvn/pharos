package config

import (
	"strings"
	"testing"

	"github.com/nvcnvn/pharos/internal/engine"
)

// The backends part of the ARCHITECTURE §10 example.
const example = `
listen: :8080
backends:
  - url: http://gpu-box:11434
    kind: auto
    memory_gb: 24
    capacity: 4
  - url: http://gpu-box:8000
    kind: vllm
    logs: docker://vllm-1
    probes:
      - name: my-vllm-running
        signal: running
        prom: {path: /metrics, metric: vllm:num_requests_running, per_model: model_name}
      - name: my-model-loaded
        signal: residency
        log: {match: 'model (?P<model>\S+) loaded', value: loaded}
      - name: my-kv-percent
        signal: kv_usage
        prom: {path: /metrics, metric: engine_kv_percent, scale: 0.01}
`

func TestParseExample(t *testing.T) {
	c, err := Parse([]byte(example))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Backends) != 2 {
		t.Fatalf("backends = %d, want 2", len(c.Backends))
	}
	if c.Listen != ":8080" || c.Policy != "cost" {
		t.Errorf("listen %q, policy %q", c.Listen, c.Policy)
	}
	if b := c.Backends[0]; b.URL != "http://gpu-box:11434" || b.Kind != engine.Auto || b.Logs != "" || len(b.Probes) != 0 || b.MemoryBytes != 24<<30 || b.Capacity != 4 {
		t.Errorf("backend 0 = %+v", b)
	}
	b := c.Backends[1]
	if b.Kind != engine.VLLM || b.Logs != "docker://vllm-1" {
		t.Errorf("backend 1 = %s %s", b.Kind, b.Logs)
	}

	// Own probes read what their spec says, in config order.
	for i, tt := range []struct {
		name string
		sig  engine.Signal
		in   string
		want string // Show()n
	}{
		{"my-vllm-running", engine.Running, `vllm:num_requests_running{engine="0",model_name="qwen2.5-0.5b"} 2.0`, "[qwen2.5-0.5b=2]"},
		{"my-model-loaded", engine.Residency, "model qwen2.5 loaded in 3s", "[qwen2.5=loaded]"},
		{"my-kv-percent", engine.KVUsage, "engine_kv_percent 25", "[=0.25]"},
	} {
		p := b.Probes[i]
		if p.Name != tt.name || p.Signal != tt.sig {
			t.Errorf("probe %d = %s %s, want %s %s", i, p.Name, p.Signal, tt.name, tt.sig)
			continue
		}
		s, err := p.Parse([]byte(tt.in))
		if err != nil {
			t.Errorf("%s: %v", tt.name, err)
		} else if got := s.Show(tt.sig); got != tt.want {
			t.Errorf("%s read %s, want %s", tt.name, got, tt.want)
		}
	}
	if b.Probes[1].Feed != (engine.Feed{Log: true}) || b.Probes[0].Feed != (engine.Feed{Path: "/metrics"}) {
		t.Errorf("feeds = %+v, %+v", b.Probes[0].Feed, b.Probes[1].Feed)
	}
}

func TestParseRejects(t *testing.T) {
	probe := func(spec string) string {
		return "backends:\n  - url: http://h:1\n    probes:\n      - " + strings.ReplaceAll(spec, "\n", "\n        ") + "\n"
	}
	for _, tt := range []struct {
		name, yaml, mention string
	}{
		{"not_yaml", "backends: [", "yaml"},
		{"backend_without_url", "backends:\n  - kind: vllm\n", "url"},
		{"url_without_scheme", "backends:\n  - url: gpu-box:8000\n", "url"},
		{"unknown_kind", "backends:\n  - url: http://h:1\n    kind: tgi\n", "tgi"},
		{"unknown_policy", "policy: round-robin\n", "round-robin"},
		{"negative_capacity", "backends:\n  - url: http://h:1\n    capacity: -1\n", "capacity"},
		{"log_feed_not_docker", "backends:\n  - url: http://h:1\n    logs: /var/log/vllm.log\n", "docker://"},
		{"probe_without_name", probe("signal: running\nprom: {path: /metrics, metric: m}"), "name"},
		{"probe_name_of_a_library_probe", probe("name: vllm-running\nsignal: running\nprom: {path: /metrics, metric: m}"), "vllm-running"},
		{"duplicate_probe_name", "backends:\n  - url: http://h:1\n    probes:\n" +
			"      - {name: a, signal: running, prom: {path: /metrics, metric: m}}\n" +
			"      - {name: a, signal: waiting, prom: {path: /metrics, metric: n}}\n", "a"},
		{"unknown_signal", probe("name: a\nsignal: busy\nprom: {path: /metrics, metric: m}"), "busy"},
		{"neither_prom_nor_log", probe("name: a\nsignal: running"), "prom or log"},
		{"both_prom_and_log", probe("name: a\nsignal: running\nprom: {path: /metrics, metric: m}\nlog: {match: 'r=(?P<value>\\d+)'}"), "prom or log"},
		{"prom_signal_not_a_load_signal", probe("name: a\nsignal: residency\nprom: {path: /metrics, metric: m}"), "residency"},
		{"prom_without_metric", probe("name: a\nsignal: running\nprom: {path: /metrics}"), "metric"},
		{"prom_path_not_absolute", probe("name: a\nsignal: running\nprom: {path: metrics, metric: m}"), "path"},
		{"log_pattern_does_not_compile", probe("name: a\nsignal: running\nlog: {match: 'r=(?P<value>\\d+'}"), "a"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.yaml))
			if err == nil || !strings.Contains(err.Error(), tt.mention) {
				t.Errorf("err = %v, want it to mention %q", err, tt.mention)
			}
		})
	}
}

func TestParsePeers(t *testing.T) {
	c, err := Parse([]byte(`
state_file: /data/pharos.state
peers:
  listen: :8081
  secret_file: /run/secrets/pharos-peer
  members: [pharos-a:8081, pharos-b:8081]
`))
	if err != nil {
		t.Fatal(err)
	}
	if c.StateFile != "/data/pharos.state" || c.Peers == nil || c.Peers.Listen != ":8081" || len(c.Peers.Members) != 2 {
		t.Errorf("%+v %+v", c, c.Peers)
	}
	c, err = Parse([]byte("peers: {listen: ':8081', secret_file: s, dns: pharos-peers.default.svc.cluster.local}"))
	if err != nil || c.Peers.DNS != "pharos-peers.default.svc.cluster.local:8081" {
		t.Errorf("dns without a port takes the listen port: %+v, %v", c.Peers, err)
	}
	if c, _ := Parse([]byte("listen: :8080")); c.Peers != nil {
		t.Error("no peers section is a single instance")
	}
	for _, tt := range []struct{ name, yaml, err string }{
		{"no_secret", "peers: {listen: ':8081', members: [a:8081]}", "secret_file"},
		{"no_listen", "peers: {secret_file: s, members: [a:8081]}", "peers.listen"},
		{"no_members", "peers: {listen: ':8081', secret_file: s}", "exactly one of members and dns"},
		{"both", "peers: {listen: ':8081', secret_file: s, members: [a:8081], dns: x}", "exactly one of members and dns"},
		{"member_without_port", "peers: {listen: ':8081', secret_file: s, members: [a]}", "want host:port"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Parse([]byte(tt.yaml)); err == nil || !strings.Contains(err.Error(), tt.err) {
				t.Errorf("err = %v, want it to mention %q", err, tt.err)
			}
		})
	}
}
