// Package config reads the Pharos YAML config (ARCHITECTURE §10). An unknown
// field is an error, so a typo doesn't silently drop a setting.
package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/policy"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Listen    string // default ":8080"
	Policy    string // policy.Cost (default) or policy.LeastLoad
	Backends  []Backend
	StateFile string // "" = none
	Peers     *Peers // nil = a single instance
	Keys      []Key  // none = every client is let in
	Usage     Usage
	Drain     Drain
}

// Key is one API key (ARCHITECTURE §11). Only its SHA-256 is stored.
type Key struct {
	Name         string // unique; usage is recorded by name
	SHA256       [32]byte
	RPM          int      // requests per minute; 0 = no limit
	TokensPerDay uint64   // prompt + completion tokens; 0 = no limit
	Weight       int      // fair-queue share; default 1
	Models       []string // allow-list; empty = every model
	Admin        bool     // may read /status and /usage
}

// HashKey is the SHA-256 a key entry stores.
func HashKey(key string) [32]byte { return sha256.Sum256([]byte(key)) }

type Usage struct {
	Location      *time.Location // where a day ends for tokens_per_day and history; default UTC
	RetentionDays int            // default 400
}

// Drain is the graceful shutdown (ARCHITECTURE §9).
type Drain struct {
	Grace   time.Duration // /healthz fails but requests are still accepted; default 5 s
	Timeout time.Duration // then in-flight requests get this long to finish; default 10 min
}

// Peers is the peer-sync setup (ARCHITECTURE §12).
type Peers struct {
	Listen     string   // the private peer listener, e.g. ":8081"
	SecretFile string   // holds the shared secret
	Members    []string // host:port of every instance, may include this one
	DNS        string   // host:port re-resolved every 10 s, instead of Members
}

type Backend struct {
	URL         string
	Kind        engine.Kind
	Logs        string         // log feed, "docker://<container>"; "" = none
	Probes      []engine.Probe // own probes, in config order; they go ahead of the recipe
	MemoryBytes int64          // memory_gb: host memory for models; 0 = unknown
	Capacity    int            // slots per target when the engine doesn't report them; 0 = unset
}

// Load reads and parses the config file at path.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	return Parse(data)
}

type rawConfig struct {
	Listen    string `yaml:"listen"`
	Policy    string `yaml:"policy"`
	StateFile string `yaml:"state_file"`
	Peers     *struct {
		Listen     string   `yaml:"listen"`
		SecretFile string   `yaml:"secret_file"`
		Members    []string `yaml:"members"`
		DNS        string   `yaml:"dns"`
	} `yaml:"peers"`
	Backends []struct {
		URL      string     `yaml:"url"`
		Kind     string     `yaml:"kind"`
		Logs     string     `yaml:"logs"`
		MemoryGB float64    `yaml:"memory_gb"`
		Capacity int        `yaml:"capacity"`
		Probes   []rawProbe `yaml:"probes"`
	} `yaml:"backends"`
	Keys []struct {
		Name         string   `yaml:"name"`
		SHA256       string   `yaml:"sha256"`
		RPM          int      `yaml:"rpm"`
		TokensPerDay int64    `yaml:"tokens_per_day"`
		Weight       int      `yaml:"weight"`
		Models       []string `yaml:"models"`
		Admin        bool     `yaml:"admin"`
	} `yaml:"keys"`
	Usage struct {
		Timezone      string `yaml:"timezone"`
		RetentionDays int    `yaml:"retention_days"`
	} `yaml:"usage"`
	Drain struct {
		Grace   time.Duration `yaml:"grace"`
		Timeout time.Duration `yaml:"timeout"`
	} `yaml:"drain"`
}

type rawProbe struct {
	Name   string `yaml:"name"`
	Signal string `yaml:"signal"`
	Prom   *struct {
		Path     string  `yaml:"path"`
		Metric   string  `yaml:"metric"`
		PerModel string  `yaml:"per_model"`
		Scale    float64 `yaml:"scale"`
	} `yaml:"prom"`
	Log *struct {
		Match string `yaml:"match"`
		Value string `yaml:"value"`
	} `yaml:"log"`
}

func Parse(data []byte) (Config, error) {
	var raw rawConfig
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&raw); err != nil && !errors.Is(err, io.EOF) {
		return Config{}, fmt.Errorf("config: yaml: %w", err)
	}
	library := map[string]bool{}
	for _, p := range engine.Library {
		library[p.Name] = true
	}
	c := Config{Listen: raw.Listen, Policy: raw.Policy, StateFile: raw.StateFile}
	var errs []error
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	switch c.Policy {
	case "":
		c.Policy = policy.Cost
	case policy.Cost, policy.LeastLoad:
	default:
		errs = append(errs, fmt.Errorf("config: policy %q: want %s or %s", c.Policy, policy.Cost, policy.LeastLoad))
	}
	if rp := raw.Peers; rp != nil {
		p := &Peers{Listen: rp.Listen, SecretFile: rp.SecretFile, Members: rp.Members, DNS: rp.DNS}
		_, port, err := net.SplitHostPort(p.Listen)
		switch {
		case err != nil:
			errs = append(errs, fmt.Errorf("config: peers.listen %q: want [host]:port", p.Listen))
		case p.SecretFile == "":
			errs = append(errs, errors.New("config: peers.secret_file is required: peers refuse to run without a secret"))
		case (len(p.Members) == 0) == (p.DNS == ""):
			errs = append(errs, errors.New("config: peers: want exactly one of members and dns"))
		}
		if _, _, err := net.SplitHostPort(p.DNS); p.DNS != "" && err != nil {
			p.DNS = net.JoinHostPort(p.DNS, port) // a headless Service name: peers listen on our port
		}
		for _, m := range p.Members {
			if _, _, err := net.SplitHostPort(m); err != nil {
				errs = append(errs, fmt.Errorf("config: peers.members %q: want host:port", m))
			}
		}
		c.Peers = p
	}
	for i, rb := range raw.Backends {
		b := Backend{URL: strings.TrimRight(rb.URL, "/"), Kind: engine.Kind(rb.Kind), Logs: rb.Logs,
			MemoryBytes: int64(rb.MemoryGB * (1 << 30)), Capacity: rb.Capacity}
		fail := func(format string, a ...any) {
			errs = append(errs, fmt.Errorf("config: backends[%d] %s: %s", i, rb.URL, fmt.Sprintf(format, a...)))
		}
		if u, err := url.Parse(b.URL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			fail("url must be http(s)://host[:port]")
		}
		if b.Kind == "" {
			b.Kind = engine.Auto
		}
		if _, ok := engine.Recipes[b.Kind]; !ok && b.Kind != engine.Auto {
			fail("unknown kind %q", rb.Kind)
		}
		if rb.MemoryGB < 0 || rb.Capacity < 0 {
			fail("memory_gb and capacity can't be negative")
		}
		if b.Logs != "" && !strings.HasPrefix(b.Logs, "docker://") {
			fail("logs %q: only docker://<container> feeds exist", b.Logs)
		}
		seen := map[string]bool{}
		for j, rp := range rb.Probes {
			switch {
			case rp.Name == "":
				fail("probes[%d]: name is required", j)
				continue
			case library[rp.Name]:
				fail("probe %s: name is taken by a library probe", rp.Name)
			case seen[rp.Name]:
				fail("probe %s: name used twice", rp.Name)
			}
			seen[rp.Name] = true
			p, err := ownProbe(rp)
			if err != nil {
				fail("probe %s: %v", rp.Name, err)
				continue
			}
			b.Probes = append(b.Probes, p)
		}
		c.Backends = append(c.Backends, b)
	}

	names, sums := map[string]bool{}, map[[32]byte]bool{}
	for i, rk := range raw.Keys {
		k := Key{Name: rk.Name, RPM: rk.RPM, TokensPerDay: uint64(max(rk.TokensPerDay, 0)), Weight: rk.Weight, Models: rk.Models, Admin: rk.Admin}
		fail := func(format string, a ...any) {
			errs = append(errs, fmt.Errorf("config: keys[%d] %s: %s", i, rk.Name, fmt.Sprintf(format, a...)))
		}
		if b, err := hex.DecodeString(rk.SHA256); err != nil || len(b) != 32 {
			fail("sha256 must be 64 hex characters, as printed by pharos keys new")
		} else {
			copy(k.SHA256[:], b)
		}
		switch {
		case k.Name == "":
			fail("name is required")
		case names[k.Name]:
			fail("name used twice")
		case sums[k.SHA256]:
			fail("sha256 used twice")
		}
		if rk.RPM < 0 || rk.TokensPerDay < 0 || rk.Weight < 0 {
			fail("rpm, tokens_per_day and weight can't be negative")
		}
		if k.Weight == 0 {
			k.Weight = 1
		}
		names[k.Name], sums[k.SHA256] = true, true
		c.Keys = append(c.Keys, k)
	}

	c.Usage.Location = time.UTC
	if tz := raw.Usage.Timezone; tz != "" {
		loc, err := time.LoadLocation(tz)
		if err != nil {
			errs = append(errs, fmt.Errorf("config: usage.timezone %q: %v", tz, err))
		} else {
			c.Usage.Location = loc
		}
	}
	c.Usage.RetentionDays = raw.Usage.RetentionDays
	if c.Usage.RetentionDays <= 0 {
		c.Usage.RetentionDays = 400
	}
	c.Drain = Drain{Grace: raw.Drain.Grace, Timeout: raw.Drain.Timeout}
	if c.Drain.Grace < 0 || c.Drain.Timeout < 0 {
		errs = append(errs, errors.New("config: drain.grace and drain.timeout can't be negative"))
	}
	if raw.Drain.Grace == 0 {
		c.Drain.Grace = 5 * time.Second
	}
	if raw.Drain.Timeout == 0 {
		c.Drain.Timeout = 10 * time.Minute
	}
	return c, errors.Join(errs...)
}

func ownProbe(rp rawProbe) (engine.Probe, error) {
	sig, ok := engine.ParseSignal(rp.Signal)
	if !ok {
		return engine.Probe{}, fmt.Errorf("unknown signal %q", rp.Signal)
	}
	if (rp.Prom == nil) == (rp.Log == nil) {
		return engine.Probe{}, errors.New("want exactly one of prom or log")
	}
	if rp.Log != nil {
		return engine.LogLine(rp.Name, sig, rp.Log.Match, rp.Log.Value)
	}
	switch sig {
	case engine.Running, engine.Waiting, engine.Capacity, engine.KVUsage:
	default:
		return engine.Probe{}, fmt.Errorf("prom reads running, waiting, capacity or kv_usage, not %s", sig)
	}
	pr := rp.Prom
	if !strings.HasPrefix(pr.Path, "/") || pr.Metric == "" {
		return engine.Probe{}, errors.New("prom needs an absolute path and a metric")
	}
	var opts []engine.PromOpt
	if pr.PerModel != "" {
		opts = append(opts, engine.PerModel(pr.PerModel))
	}
	if pr.Scale != 0 {
		opts = append(opts, engine.Scale(pr.Scale))
	}
	return engine.Prom(rp.Name, pr.Path, sig, pr.Metric, opts...), nil
}
