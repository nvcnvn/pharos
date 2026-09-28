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
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/nvcnvn/pharos/internal/config"
	"github.com/nvcnvn/pharos/internal/discovery"
	"github.com/nvcnvn/pharos/internal/peer"
	"github.com/nvcnvn/pharos/internal/policy"
	"github.com/nvcnvn/pharos/internal/prefix"
	"github.com/nvcnvn/pharos/internal/proxy"
	"github.com/nvcnvn/pharos/internal/sched"
	"github.com/nvcnvn/pharos/internal/state"
)

// prefixEntries caps the prefix index: ~100 B each, ~20 MB (ARCHITECTURE §6).
const prefixEntries = 200_000

// stateFileEvery is how often the state file is written (and on shutdown).
const stateFileEvery = 30 * time.Second

func serve(args []string, stderr io.Writer) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfgPath := fs.String("config", "pharos.yaml", "config file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	var static []state.BackendSpec
	for _, b := range cfg.Backends {
		static = append(static, state.BackendSpec{URL: b.URL, Kind: b.Kind, Own: b.Probes, MemoryBytes: b.MemoryBytes,
			Capacity: b.Capacity, Logs: strings.TrimPrefix(b.Logs, "docker://")})
	}
	var secret string
	if cfg.Peers != nil {
		b, err := os.ReadFile(cfg.Peers.SecretFile)
		if secret = strings.TrimSpace(string(b)); err != nil || secret == "" {
			return fmt.Errorf("peers.secret_file %s: %v: peers refuse to run without a secret", cfg.Peers.SecretFile, cmp.Or(err, errors.New("empty")))
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	docker := discovery.DockerAvailable(ctx)
	if len(static) == 0 && !docker {
		return fmt.Errorf("%s: no backends, and no Docker socket to discover them", *cfgPath)
	}

	var sc *sched.Sched
	var node *peer.Node
	st := state.New(static, state.Options{
		Client:   &http.Client{Timeout: 5 * time.Second},
		OnUpdate: func() { sc.Kick() },
		OpenLogs: func(ctx context.Context, container string) (io.ReadCloser, error) {
			return discovery.DockerLogs(ctx, container, true)
		},
	})
	pc := policy.Defaults
	pc.Policy = cfg.Policy
	px := prefix.New(prefixEntries)
	sc = sched.New(st, px, sched.Config{Policy: pc, OnPrefix: func(op prefix.Op) { node.Push(op) }})
	pcfg := peer.Config{Origin: rand.Uint64(), Secret: secret, Fingerprint: fingerprint(static)}
	if cfg.Peers != nil {
		pcfg.Members, pcfg.DNS = cfg.Peers.Members, cfg.Peers.DNS
	}
	node = peer.New(pcfg, st, sc, px) // a single instance is a cluster of one
	go st.Run(ctx)

	discovered := make(chan struct{})
	if docker {
		first := true
		go discovery.DockerLabels(ctx, func(found []state.BackendSpec) {
			st.SetBackends(append(slices.Clip(static), found...))
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

	srv := &http.Server{Addr: cfg.Listen, Handler: proxy.New(st, sc, nil)}
	go func() { errc <- srv.ListenAndServe() }()
	slog.Info("pharos serving", "listen", cfg.Listen, "backends", len(static), "policy", cfg.Policy, "peers", cfg.Peers != nil)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	// ponytail: stop accepting and wait for in-flight streams; the healthz
	// grace period comes with build step 4.
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	slog.Info("shutting down; waiting for in-flight requests")
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

// fingerprint hashes the static backend set, so peers can flag a config
// that differs. Docker-discovered backends are per host and left out.
func fingerprint(specs []state.BackendSpec) uint64 {
	var urls []string
	for _, s := range specs {
		urls = append(urls, s.URL)
	}
	slices.Sort(urls)
	h := fnv.New64a()
	for _, u := range urls {
		io.WriteString(h, u+"\n")
	}
	return h.Sum64()
}
