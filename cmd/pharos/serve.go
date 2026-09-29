package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nvcnvn/pharos/internal/config"
	"github.com/nvcnvn/pharos/internal/discovery"
	"github.com/nvcnvn/pharos/internal/obs"
	"github.com/nvcnvn/pharos/internal/peer"
	"github.com/nvcnvn/pharos/internal/policy"
	"github.com/nvcnvn/pharos/internal/prefix"
	"github.com/nvcnvn/pharos/internal/proxy"
	"github.com/nvcnvn/pharos/internal/sched"
	"github.com/nvcnvn/pharos/internal/state"
	"github.com/nvcnvn/pharos/internal/usage"
)

// prefixEntries caps the prefix index: ~100 B each, ~20 MB (ARCHITECTURE §6).
const prefixEntries = 200_000

// stateFileEvery is how often the state file is written (and on shutdown).
const stateFileEvery = 30 * time.Second

// configEvery is how often the config file is checked for changes; a var so
// tests needn't wait 10 s.
var configEvery = 10 * time.Second

// serve runs until ctx is done, then drains (ARCHITECTURE §9).
func serve(ctx context.Context, args []string, stderr io.Writer) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfgPath := fs.String("config", "pharos.yaml", "config file (examples in README.md, every field in docs/ARCHITECTURE.md §10); keys and backends reload live")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: pharos serve [-config file]\n\nRoutes OpenAI and Ollama API requests to the config's backends and to Docker\ncontainers labeled pharos.enable=true, until SIGINT or SIGTERM, then drains.")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	static := specs(cfg.Backends)
	var secret string
	if cfg.Peers != nil {
		b, err := os.ReadFile(cfg.Peers.SecretFile)
		if secret = strings.TrimSpace(string(b)); err != nil || secret == "" {
			return fmt.Errorf("peers.secret_file %s: %v: peers refuse to run without a secret", cfg.Peers.SecretFile, cmp.Or(err, errors.New("empty")))
		}
	}

	docker := discovery.DockerAvailable(ctx)
	if len(static) == 0 && !docker {
		return fmt.Errorf("%s: no backends, and no Docker socket to discover them", *cfgPath)
	}

	var sc *sched.Sched
	var node *peer.Node
	var o *obs.Obs
	// The backend set is the static list plus what Docker labels discover;
	// either can change (a config reload, a container event).
	var bmu sync.Mutex
	var found []state.BackendSpec
	st := state.New(static, state.Options{
		Client:     &http.Client{Timeout: 5 * time.Second},
		OnUpdate:   func() { sc.Kick() },
		OnDecision: func(backend, stage, outcome string) { o.Background(backend, stage, outcome) },
		OpenLogs: func(ctx context.Context, feed string) (io.ReadCloser, error) {
			return discovery.OpenLog(ctx, feed, true)
		},
	})
	pc := policy.Defaults
	pc.Policy = cfg.Policy
	px := prefix.New(prefixEntries)
	sc = sched.New(st, px, sched.Config{Policy: pc, OnPrefix: func(op prefix.Op) { node.Push(op) }})
	pcfg := peer.Config{Origin: rand.Uint64(), Secret: secret, Fingerprint: fingerprint(static, cfg.Keys)}
	if cfg.Peers != nil {
		pcfg.Members, pcfg.DNS = cfg.Peers.Members, cfg.Peers.DNS
	}
	u := usage.New(usage.Config{Origin: pcfg.Origin, Location: cfg.Usage.Location, RetentionDays: cfg.Usage.RetentionDays})
	node = peer.New(pcfg, st, sc, px, u) // a single instance is a cluster of one
	o = obs.New(st, sc, node, u)
	p := proxy.New(st, sc, proxy.Options{Usage: u, OnDone: o.Done})
	p.SetKeys(cfg.Keys)
	if len(cfg.Keys) == 0 {
		slog.Warn("no keys in the config: every client is let in, and /status and /usage are open")
	}
	go st.Run(ctx)

	discovered := make(chan struct{})
	if docker {
		first := true
		go discovery.DockerLabels(ctx, func(specs []state.BackendSpec) {
			bmu.Lock()
			found = specs
			st.SetBackends(append(slices.Clip(static), found...))
			bmu.Unlock()
			if first {
				first = false
				close(discovered)
			}
		})
		slog.Info("discovering backends from Docker labels (pharos.enable=true)")
	} else {
		close(discovered)
	}

	// Restore only once targets exist, so prefix entries find them.
	select {
	case <-discovered:
	case <-time.After(5 * time.Second):
	}
	for !st.Ready() && ctx.Err() == nil {
		time.Sleep(50 * time.Millisecond)
	}
	if cfg.StateFile != "" {
		s, err := peer.ReadFile(cfg.StateFile)
		if err != nil {
			slog.Warn("state file not restored", "err", err)
		}
		node.Merge(s)
	}
	errc := make(chan error, 2)
	if cfg.Peers != nil {
		psrv := &http.Server{Addr: cfg.Peers.Listen, Handler: node.Handler()}
		go func() { errc <- psrv.ListenAndServe() }()
		defer psrv.Close()
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		node.Restore(rctx)
		cancel()
		go node.Run(ctx)
	}
	if cfg.StateFile != "" {
		go func() {
			t := time.NewTicker(stateFileEvery)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					if err := peer.WriteFile(cfg.StateFile, node.Snapshot()); err != nil {
						slog.Warn("state file", "err", err)
					}
				}
			}
		}()
	}

	go watchConfig(ctx, *cfgPath, cfg, func(next config.Config) {
		p.SetKeys(next.Keys)
		bmu.Lock()
		static = specs(next.Backends)
		st.SetBackends(append(slices.Clip(static), found...))
		node.SetFingerprint(fingerprint(static, next.Keys))
		bmu.Unlock()
		slog.Info("config reloaded", "keys", len(next.Keys), "backends", len(next.Backends))
	})

	mux := http.NewServeMux()
	mux.Handle("/", p)
	mux.HandleFunc("GET /metrics", o.Metrics)
	mux.Handle("GET /status", p.Admin(http.HandlerFunc(o.Status)))
	mux.Handle("GET /usage", p.Admin(http.HandlerFunc(o.Usage)))
	srv := &http.Server{Addr: cfg.Listen, Handler: mux}
	go func() { errc <- srv.ListenAndServe() }()
	slog.Info("pharos serving", "listen", cfg.Listen, "backends", len(cfg.Backends), "keys", len(cfg.Keys), "policy", cfg.Policy, "peers", cfg.Peers != nil)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	// Drain: /healthz fails while requests are still served, so the load
	// balancer moves away; then stop accepting and let streams finish.
	slog.Info("draining: /healthz fails, still serving", "grace", cfg.Drain.Grace)
	p.Drain()
	time.Sleep(cfg.Drain.Grace)
	shutdown, cancel := context.WithTimeout(context.Background(), cfg.Drain.Timeout)
	defer cancel()
	slog.Info("shutting down; waiting for in-flight requests", "timeout", cfg.Drain.Timeout)
	err = srv.Shutdown(shutdown)
	node.Leave(shutdown)
	if cfg.StateFile != "" {
		if werr := peer.WriteFile(cfg.StateFile, node.Snapshot()); werr != nil {
			slog.Warn("state file", "err", werr)
		}
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// fingerprint hashes the static backend set and the key set, so peers can
// flag a config that differs. Docker-discovered backends are per host and
// left out.
func fingerprint(specs []state.BackendSpec, keys []config.Key) uint64 {
	var lines []string
	for _, s := range specs {
		lines = append(lines, s.URL)
	}
	for _, k := range keys {
		lines = append(lines, fmt.Sprintf("key %s %x %d %d %d %v %v", k.Name, k.SHA256, k.RPM, k.TokensPerDay, k.Weight, k.Models, k.Admin))
	}
	slices.Sort(lines)
	h := fnv.New64a()
	for _, l := range lines {
		io.WriteString(h, l+"\n")
	}
	return h.Sum64()
}

func specs(backends []config.Backend) []state.BackendSpec {
	var out []state.BackendSpec
	for _, b := range backends {
		out = append(out, state.BackendSpec{URL: b.URL, Kind: b.Kind, Own: b.Probes, MemoryBytes: b.MemoryBytes,
			Capacity: b.Capacity, Logs: b.Logs})
	}
	return out
}

// watchConfig re-reads the config when its modification time changes,
// checked every configEvery, and applies it: keys, quotas and static backends take
// effect live; other fields need a restart, which is logged. A config that
// doesn't parse is logged and ignored.
func watchConfig(ctx context.Context, path string, cur config.Config, apply func(config.Config)) {
	fi, err := os.Stat(path)
	if err != nil {
		return
	}
	mod := fi.ModTime()
	t := time.NewTicker(configEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		fi, err := os.Stat(path)
		if err != nil || fi.ModTime().Equal(mod) {
			continue
		}
		mod = fi.ModTime()
		next, err := config.Load(path)
		if err != nil {
			slog.Warn("config changed but doesn't load; keeping the running one", "err", err)
			continue
		}
		if fields := restartNeeded(cur, next); len(fields) > 0 {
			slog.Warn("config: these changes apply only after a restart", "fields", fields)
		}
		apply(next)
		cur = next
	}
}

// restartNeeded lists the fields that changed but aren't applied live.
func restartNeeded(a, b config.Config) []string {
	var out []string
	for _, f := range []struct {
		name string
		same bool
	}{
		{"listen", a.Listen == b.Listen},
		{"policy", a.Policy == b.Policy},
		{"state_file", a.StateFile == b.StateFile},
		{"peers", reflect.DeepEqual(a.Peers, b.Peers)},
		{"usage", a.Usage.Location.String() == b.Usage.Location.String() && a.Usage.RetentionDays == b.Usage.RetentionDays},
		{"drain", a.Drain == b.Drain},
	} {
		if !f.same {
			out = append(out, f.name)
		}
	}
	return out
}
