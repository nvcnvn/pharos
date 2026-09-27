package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/nvcnvn/pharos/internal/config"
	"github.com/nvcnvn/pharos/internal/policy"
	"github.com/nvcnvn/pharos/internal/prefix"
	"github.com/nvcnvn/pharos/internal/proxy"
	"github.com/nvcnvn/pharos/internal/sched"
	"github.com/nvcnvn/pharos/internal/state"
)

// prefixEntries caps the prefix index: ~100 B each, ~20 MB (ARCHITECTURE §6).
const prefixEntries = 200_000

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
	if len(cfg.Backends) == 0 {
		return fmt.Errorf("%s: no backends", *cfgPath)
	}
	var specs []state.BackendSpec
	for _, b := range cfg.Backends {
		if b.Logs != "" {
			slog.Warn("log feeds aren't followed yet (build step 6); log probes stay unknown", "backend", b.URL)
		}
		specs = append(specs, state.BackendSpec{URL: b.URL, Kind: b.Kind, Own: b.Probes, MemoryBytes: b.MemoryBytes, Capacity: b.Capacity})
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var sc *sched.Sched
	st := state.New(specs, state.Options{
		Client:   &http.Client{Timeout: 5 * time.Second},
		OnUpdate: func() { sc.Kick() },
	})
	pc := policy.Defaults
	pc.Policy = cfg.Policy
	sc = sched.New(st, prefix.New(prefixEntries), sched.Config{Policy: pc})
	go st.Run(ctx)

	srv := &http.Server{Addr: cfg.Listen, Handler: proxy.New(st, sc, nil)}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	slog.Info("pharos serving", "listen", cfg.Listen, "backends", len(specs), "policy", cfg.Policy)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	// ponytail: stop accepting and wait for in-flight streams; the full drain
	// (healthz grace period, state file) comes with build step 4.
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	slog.Info("shutting down; waiting for in-flight requests")
	if err := srv.Shutdown(shutdown); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
