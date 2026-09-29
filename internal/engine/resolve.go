package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

// Plan is the probes that answered on one backend, in priority order.
type Plan struct {
	Kind    Kind
	Version Opt[string]
	Active  []Probe           // own probes first, then recipe order; one per signal
	Dropped map[string]string // probe name -> why (404, status, fetch error, parse error, no value, redundant, version guard, no log feed)
}

// fingerprints are tried in order; the first kind whose probes all read a
// value wins, else OpenAI. llama-swap comes before Ollama because it also
// answers /api/version (spike 2026-09-27).
var fingerprints = []struct {
	kind   Kind
	probes []Probe
}{
	{LlamaSwap, []Probe{llamaswapRunning}},
	{Ollama, []Probe{ollamaVersion}},
	{LlamaCpp, []Probe{llamacppPropsVersion}},
	{VLLM, []Probe{vllmVersion, vllmRunning}},
	{SGLang, []Probe{sglangVersion}}, // only SGLang answers /get_server_info (captures)
}

// Resolve runs every probe of the kind's recipe, with own probes ahead of it,
// and keeps the first probe per signal that reads a known value. Kind Auto is
// detected first. A log probe can't be checked here: it is kept while the
// backend has a log feed (logs), unless an earlier probe reads its signal, and
// its signal stays unknown until Follow sees a matching line. Resolve fails
// only when the backend answers no request at all.
func Resolve(ctx context.Context, c *http.Client, base string, k Kind, own []Probe, logs bool) (Plan, Snapshot, error) {
	if k == Auto {
		var err error
		if k, err = detect(ctx, c, base); err != nil {
			return Plan{}, Snapshot{}, err
		}
	}
	recipe, ok := Recipes[k]
	if !ok {
		return Plan{}, Snapshot{}, fmt.Errorf("engine: unknown kind %q", k)
	}
	probes := append(slices.Clip(own), recipe...)
	outs, err := run(ctx, c, base, probes, logs)
	if err != nil {
		return Plan{Kind: k}, Snapshot{}, err
	}

	plan := Plan{Kind: k, Dropped: map[string]string{}}
	for i, p := range probes {
		if p.Signal == Version && p.When == nil && outs[i].fail == "" {
			plan.Version = outs[i].s.Version
			break
		}
	}
	filled := map[Signal]string{}
	var active []outcome
	for i, p := range probes {
		switch {
		case p.When != nil && (!plan.Version.OK || !p.When(plan.Version.V)):
			this := "the version is unknown"
			if plan.Version.OK {
				this = "this is " + plan.Version.V
			}
			plan.Dropped[p.Name] = fmt.Sprintf("version guard: needs %s (%s)", p.Needs, this)
		case outs[i].fail != "":
			plan.Dropped[p.Name] = outs[i].fail
		case filled[p.Signal] != "":
			plan.Dropped[p.Name] = fmt.Sprintf("redundant: %s reads %s", filled[p.Signal], p.Signal)
		default:
			filled[p.Signal] = p.Name
			plan.Active = append(plan.Active, p)
			active = append(active, outs[i])
		}
	}
	return plan, merge(plan.Active, active), nil
}

// Scrape runs the plan's probes, one GET per path. A planned probe that no
// longer reads a value leaves its signal unknown (fallbacks were dropped as
// redundant) and is returned in the error, so the caller can resolve again.
func (p Plan) Scrape(ctx context.Context, c *http.Client, base string) (Snapshot, error) {
	outs, err := run(ctx, c, base, p.Active, true) // a planned log probe had a feed
	if err != nil {
		return Snapshot{}, err
	}
	var errs []error
	for i, o := range outs {
		if o.fail != "" {
			errs = append(errs, fmt.Errorf("engine: %s: %s", p.Active[i].Name, o.fail))
		}
	}
	return merge(p.Active, outs), errors.Join(errs...)
}

func detect(ctx context.Context, c *http.Client, base string) (Kind, error) {
	var probes []Probe
	for _, f := range fingerprints {
		probes = append(probes, f.probes...)
	}
	outs, err := run(ctx, c, base, probes, false)
	if err != nil {
		return "", err
	}
	i := 0
	var unanswered error
	for _, f := range fingerprints {
		ok := true
		for _, p := range f.probes {
			// 503: the engine is up but not ready (llama.cpp v0.5.0 answers every
			// path with it while loading its model, capture): it rules nothing out.
			if (strings.HasPrefix(outs[i].fail, "fetch error") || outs[i].fail == "status 503") && unanswered == nil {
				unanswered = fmt.Errorf("engine: detect: %s: %s", p.Name, outs[i].fail)
			}
			ok = ok && outs[i].fail == ""
			i++
		}
		if ok {
			return f.kind, nil
		}
	}
	// A fingerprint that got no answer, or a 503, can't be ruled out, so the
	// generic kind would be a guess: an engine under load drops some requests
	// (llama.cpp before b8772 answers only some paths with 8 requests on 2
	// slots), and one that is loading answers 503.
	if unanswered != nil {
		return "", unanswered
	}
	return OpenAI, nil
}

// outcome is one probe's result in a round. fail is "" when the probe read a
// known value, else why it didn't.
type outcome struct {
	s    Snapshot
	fail string
}

const maxBody = 16 << 20

// run GETs each distinct path once and parses every probe. A log probe reads
// nothing here; without a feed (logs) it fails. run fails only when no GET got
// a response.
// ponytail: sequential GETs; parallelize if a round exceeds the scrape budget (§15).
func run(ctx context.Context, c *http.Client, base string, probes []Probe, logs bool) ([]outcome, error) {
	type response struct {
		status int
		body   []byte
		err    error
	}
	got := map[string]response{}
	get := func(path string) response {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
		if err != nil {
			return response{err: err}
		}
		resp, err := c.Do(req)
		if err != nil {
			return response{err: err}
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
		if err == nil && len(body) > maxBody {
			err = fmt.Errorf("body over %d bytes", maxBody)
		}
		return response{resp.StatusCode, body, err}
	}

	outs := make([]outcome, len(probes))
	var lastErr error
	reached := false
	for i, p := range probes {
		if p.Feed.Log {
			if !logs {
				outs[i].fail = "no log feed"
			}
			continue
		}
		r, ok := got[p.Feed.Path]
		if !ok {
			r = get(p.Feed.Path)
			got[p.Feed.Path] = r
		}
		reached = reached || r.status != 0
		switch {
		case r.err != nil:
			lastErr = r.err
			outs[i].fail = "fetch error: " + r.err.Error()
		case r.status == http.StatusNotFound:
			outs[i].fail = "404"
		case r.status != http.StatusOK:
			outs[i].fail = fmt.Sprintf("status %d", r.status)
		default:
			s, err := p.Parse(r.body)
			switch {
			case err != nil:
				outs[i].fail = "parse error: " + err.Error()
			case !hasSignal(s, p.Signal):
				outs[i].fail = "no value"
			default:
				outs[i].s = s
			}
		}
	}
	if !reached && lastErr != nil {
		// Nothing answered: say so for the backend, not for the last path tried.
		if ue, ok := errors.AsType[*url.Error](lastErr); ok {
			lastErr = ue.Err
		}
		return nil, fmt.Errorf("engine: %s: %w", base, lastErr)
	}
	return outs, nil
}

// merge fills each signal from the probe that read it (a plan has one per
// signal). A Residency probe lists the models in memory, so a known model it
// leaves out is Cold.
func merge(probes []Probe, outs []outcome) Snapshot {
	var s Snapshot
	var inMemory map[string]ModelInfo
	model := func(name string, set func(*ModelInfo)) {
		if s.Models == nil {
			s.Models = map[string]ModelInfo{}
		}
		m := s.Models[name]
		set(&m)
		s.Models[name] = m
	}
	for i, p := range probes {
		o := outs[i]
		if o.fail != "" || !hasSignal(o.s, p.Signal) { // a log probe in a scrape round
			continue
		}
		if s.From == nil {
			s.From = map[Signal]string{}
		}
		s.From[p.Signal] = p.Name
		switch p.Signal {
		case Version:
			s.Version = o.s.Version
		case Models, Residency, VRAMBytes, SizeBytes:
			if p.Signal == Residency {
				inMemory = o.s.Models
			}
			if len(o.s.Models) == 0 && s.Models == nil {
				s.Models = map[string]ModelInfo{} // known: no models
			}
			for name, in := range o.s.Models {
				model(name, func(m *ModelInfo) {
					switch p.Signal {
					case Residency:
						m.State = in.State
					case VRAMBytes:
						m.VRAMBytes = in.VRAMBytes
					case SizeBytes:
						m.SizeBytes = in.SizeBytes
					}
				})
			}
		default:
			for key, in := range o.s.Load {
				if s.Load == nil {
					s.Load = map[string]Load{}
				}
				l := s.Load[key]
				if setLoad(&l, in, p.Signal) {
					s.Load[key] = l
				}
			}
		}
	}
	if s.From[Residency] != "" {
		for name := range s.Models {
			if _, ok := inMemory[name]; !ok {
				model(name, func(m *ModelInfo) { m.State = Cold })
			}
		}
	}
	return s
}

// hasSignal reports whether s knows sig. A model list that is present but
// empty is known (no models); a list whose sizes are all unknown is not.
func hasSignal(s Snapshot, sig Signal) bool {
	switch sig {
	case Version:
		return s.Version.OK
	case Models, Residency:
		return s.Models != nil
	case VRAMBytes, SizeBytes:
		if s.Models == nil {
			return false
		}
		for _, m := range s.Models {
			if sig == VRAMBytes && m.VRAMBytes.OK || sig == SizeBytes && m.SizeBytes.OK {
				return true
			}
		}
		return len(s.Models) == 0
	default:
		for _, l := range s.Load {
			if setLoad(&Load{}, l, sig) {
				return true
			}
		}
		return false
	}
}

// setLoad copies the Load signal sig from src into dst, if src knows it.
func setLoad(dst *Load, src Load, sig Signal) bool {
	switch sig {
	case Running:
		if src.Running.OK {
			dst.Running = src.Running
			return true
		}
	case Waiting:
		if src.Waiting.OK {
			dst.Waiting = src.Waiting
			return true
		}
	case Capacity:
		if src.Capacity.OK {
			dst.Capacity = src.Capacity
			return true
		}
	case KVUsage:
		if src.KVUsage.OK {
			dst.KVUsage = src.KVUsage
			return true
		}
	}
	return false
}
