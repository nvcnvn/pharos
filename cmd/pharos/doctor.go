package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/nvcnvn/pharos/internal/config"
	"github.com/nvcnvn/pharos/internal/discovery"
	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/proxy"
)

func doctor(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfgPath := fs.String("config", "pharos.yaml", "config file; every backend in it is checked")
	url := fs.String("url", "", "check this one backend instead of the config's")
	kind := fs.String("kind", string(engine.Auto), "with -url: the backend's kind")
	feed := fs.String("log-feed", "", "with -url: the backend's log feed, docker://<container> or file:///<path>")
	record := fs.String("record", "", "save the raw body of every candidate path to `dir`, in the layer-2 fixture layout (one backend only)")
	withLogs := fs.Bool("logs", false, "with -record: also save the backend's log to dir/engine.log (can contain prompts)")
	timeout := fs.Duration("timeout", 10*time.Second, "per request")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "usage: pharos doctor [-config file | -url URL [-kind K] [-log-feed docker://C | file:///path]] [-record dir [-logs]]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	var backends []config.Backend
	if *url != "" {
		backends = []config.Backend{{URL: strings.TrimRight(*url, "/"), Kind: engine.Kind(*kind), Logs: *feed}}
	} else {
		c, err := config.Load(*cfgPath)
		if err != nil {
			return err
		}
		backends = c.Backends
	}
	if *withLogs && *record == "" {
		return errors.New("-logs needs -record")
	}
	ctx := context.Background()
	client := &http.Client{Timeout: *timeout}

	if *record != "" {
		if len(backends) != 1 {
			return fmt.Errorf("-record takes one backend, got %d (use -url)", len(backends))
		}
		b := backends[0]
		if err := engine.Record(ctx, client, b.URL, *record); err != nil {
			return err
		}
		if *withLogs {
			fmt.Fprintln(stderr, "warning: engine logs can contain prompt text; review", filepath.Join(*record, "engine.log"), "before sharing it")
			if err := recordLogs(ctx, b.Logs, filepath.Join(*record, "engine.log")); err != nil {
				return err
			}
		}
	}

	failed := 0
	for _, b := range backends {
		if err := check(ctx, client, b, stdout); err != nil {
			fmt.Fprintf(stdout, "  error: %v\n\n", err)
			failed++
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d backends did not answer", failed, len(backends))
	}
	return nil
}

// check resolves one backend and prints its plan: kind, version, active probes
// with their values, dropped probes with the reason, and unknown signals.
func check(ctx context.Context, c *http.Client, b config.Backend, w io.Writer) error {
	fmt.Fprintf(w, "== %s\n", b.URL)
	plan, s, err := engine.Resolve(ctx, c, b.URL, b.Kind, b.Probes, b.Logs != "")
	if err != nil {
		return err
	}
	kind := string(plan.Kind)
	if b.Kind == engine.Auto {
		kind += " (detected)"
	}
	version := "unknown"
	if plan.Version.OK {
		version = plan.Version.V
	}

	// Log probes: run the plan over the log since the container started.
	logVals := map[engine.Signal]string{}
	logState := ""
	if b.Logs != "" {
		logState = readLogs(ctx, plan, b.Logs, logVals)
	}

	if v := logVals[engine.Version]; !plan.Version.OK && v != "" {
		version = v + " (from log; version guards don't see it)"
	}

	own := map[string]bool{}
	for _, p := range b.Probes {
		own[p.Name] = true
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "  kind\t%s\n  version\t%s\n", kind, version)
	if b.Logs != "" {
		fmt.Fprintf(tw, "  log feed\t%s%s\n", b.Logs, logState)
	}
	known := map[engine.Signal]bool{}
	for i, p := range plan.Active {
		label := "  active"
		if i > 0 {
			label = ""
		}
		val := s.Show(p.Signal)
		if p.Feed.Log {
			val = "no match yet"
			if v, ok := logVals[p.Signal]; ok {
				val = v
			}
		}
		known[p.Signal] = s.From[p.Signal] != "" || logVals[p.Signal] != ""
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", label, p.Signal, p.Name, source(p, own), val)
	}
	for i, name := range slices.Sorted(maps.Keys(plan.Dropped)) {
		label := "  dropped"
		if i > 0 {
			label = ""
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\n", label, name, plan.Dropped[name])
	}
	var unknown []string
	for sig := engine.Version; sig <= engine.KVUsage; sig++ {
		if !known[sig] {
			unknown = append(unknown, sig.String())
		}
	}
	fmt.Fprintf(tw, "  unknown\t%s\n", strings.Join(unknown, ", "))
	if b.Capacity > 0 {
		use := "fallback, used while the engine reports none"
		if known[engine.Capacity] {
			use = "unused: the engine reports its own"
		}
		fmt.Fprintf(tw, "  config\tcapacity %d (%s)\n", b.Capacity, use)
	}
	for i, served := range slices.Sorted(maps.Keys(b.Aliases)) {
		label := "  aliases"
		if i > 0 {
			label = ""
		}
		note := ""
		if _, ok := s.Models[served]; s.Models != nil && !ok {
			note = "\t(the engine doesn't list " + served + ")"
		}
		fmt.Fprintf(tw, "%s\t%s -> %s%s\n", label, served, b.Aliases[served], note)
	}
	if plan.Kind == engine.Ollama {
		for i, line := range remoteModels(ctx, c, b.URL) {
			label := "  remote"
			if i > 0 {
				label = ""
			}
			fmt.Fprintf(tw, "%s\t%s\n", label, line)
		}
	}
	fmt.Fprintf(tw, "  routes\t%s\n", agentRoutes(ctx, c, b.URL))
	tw.Flush()
	fmt.Fprintln(w)
	return nil
}

// remoteModels lists the Ollama models that run elsewhere: a :cloud model is
// a stub whose /api/tags entry has remote_host (0.32.15, capture). Pharos
// lists and routes it like a local one, and the prompts leave this network.
func remoteModels(ctx context.Context, c *http.Client, base string) []string {
	var tags struct {
		Models []struct {
			Name       string `json:"name"`
			RemoteHost string `json:"remote_host"`
		} `json:"models"`
	}
	if err := proxy.GetJSON(ctx, c, base+"/api/tags", &tags); err != nil {
		return []string{"unknown: " + err.Error()}
	}
	var out []string
	for _, m := range tags.Models {
		if m.RemoteHost != "" {
			out = append(out, fmt.Sprintf("%s runs on %s: Pharos routes to it like a local model, and its prompts leave this network", m.Name, m.RemoteHost))
		}
	}
	return out
}

// agentRoutes says which coding-agent APIs the backend answers. An empty body
// names no model, so a served route answers 400 and a missing one 404; with a
// model, Ollama answers 404 for one it doesn't have (spike 2026-10-03: Ollama
// 0.32.15 400, llama.cpp b6890 and mlx-lm 0.31.3 404).
func agentRoutes(ctx context.Context, c *http.Client, base string) string {
	var out []string
	for _, path := range proxy.AgentRoutes {
		answer := "yes"
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, base+path, strings.NewReader("{}"))
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.Do(req)
		switch {
		case err != nil:
			answer = "unknown (" + err.Error() + ")"
		case resp.StatusCode == http.StatusNotFound:
			answer = "no"
		}
		if err == nil {
			resp.Body.Close()
		}
		out = append(out, path+" "+answer)
	}
	return strings.Join(out, ", ")
}

func source(p engine.Probe, own map[string]bool) string {
	src := p.Feed.Path
	if p.Feed.Log {
		src = "log"
	}
	if own[p.Name] {
		src += " (own)"
	}
	return src
}

// readLogs runs the plan's log probes over the log so far and keeps the last
// value per signal. It returns a note for the log feed line.
// ponytail: a per-model signal shows the last line's model only; merge per model if doctor needs all.
func readLogs(ctx context.Context, plan engine.Plan, feed string, vals map[engine.Signal]string) string {
	r, err := discovery.OpenLog(ctx, feed, false)
	if err != nil {
		return fmt.Sprintf(" (error: %v)", err)
	}
	defer r.Close()
	err = plan.Follow(ctx, r, func(s engine.Snapshot) {
		for sig := range s.From {
			vals[sig] = s.Show(sig)
		}
	})
	if err != nil {
		return fmt.Sprintf(" (error: %v)", err)
	}
	return ""
}

func recordLogs(ctx context.Context, feed, file string) error {
	if feed == "" {
		return errors.New("-logs: the backend has no log feed (-log-feed or logs: in config)")
	}
	r, err := discovery.OpenLog(ctx, feed, false)
	if err != nil {
		return err
	}
	defer r.Close()
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, r); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
