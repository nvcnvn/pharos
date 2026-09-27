// Package config reads the Pharos YAML config (ARCHITECTURE §10). Only the
// parts built so far are parsed; other fields are ignored.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/policy"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Listen   string // default ":8080"
	Policy   string // policy.Cost (default) or policy.LeastLoad
	Backends []Backend
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

// ponytail: unknown fields are ignored because most of §10 isn't built yet; turn on
// yaml KnownFields once it is, so a typo like per-model fails instead of being dropped.
type rawConfig struct {
	Listen   string `yaml:"listen"`
	Policy   string `yaml:"policy"`
	Backends []struct {
		URL      string     `yaml:"url"`
		Kind     string     `yaml:"kind"`
		Logs     string     `yaml:"logs"`
		MemoryGB float64    `yaml:"memory_gb"`
		Capacity int        `yaml:"capacity"`
		Probes   []rawProbe `yaml:"probes"`
	} `yaml:"backends"`
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
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return Config{}, fmt.Errorf("config: yaml: %w", err)
	}
	library := map[string]bool{}
	for _, p := range engine.Library {
		library[p.Name] = true
	}
	c := Config{Listen: raw.Listen, Policy: raw.Policy}
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
