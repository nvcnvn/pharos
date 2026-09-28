// Package fakeengine is an in-process engine double for layer-3 component
// tests and the layer-5 simulator (ARCHITECTURE §14).
//
// Every body it serves is cloned from a recorded capture of the engine's
// pinned version (internal/engine/testdata), with the fake's own numbers put
// in, so Pharos's probes and stream tap read it as they read the real engine.
// Only the latency, memory and prefix-cache model is synthetic: prefill time
// per uncached token, a load delay when cold, least-recently-used unloading
// under memory pressure, and an LRU prefix cache. It serves the replies the
// capture recorded (replyFiles): streamed chat, non-streamed chat and
// completions, embeddings, and for Ollama its native API. A request the capture
// refused (e.g. embeddings on a chat server) gets the recorded refusal.
package fakeengine

import (
	"bufio"
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
)

// captures are the recordings each kind's bodies are cloned from.
var captures = map[engine.Kind]string{
	engine.Ollama:   "ollama/v0.34.4",
	engine.LlamaCpp: "llamacpp/v0.5.0",
	engine.VLLM:     "vllm/v0.30.0",
}

type Config struct {
	Kind   engine.Kind // Ollama, LlamaCpp or VLLM
	Models []Model     // Ollama: every pulled model, all cold at start; others: one, always loaded
	Slots  int         // requests running at once per model (default 2); more wait in the engine
	// Ollama: loaded models must fit; loading another unloads the least
	// recently used idle one. 0 = unlimited. Synthetic, not measured [U].
	MemoryBytes int64

	// Synthetic latency and cache model, in simulated seconds.
	PrefillSecTok float64 // per uncached prompt token
	DecodeSecTok  float64 // per generated token
	LoadSec       float64 // loading a cold model
	CacheTokens   int     // prefix cache per model; 0 = none
	Speed         float64 // simulated seconds per real second; 0 = no delays at all
}

type Model struct {
	Name      string
	SizeBytes int64
}

// Counters are totals since start, for tests and the simulator.
type Counters struct {
	Requests, Loads, Unloads   int
	Running, Waiting           int // now, over all models
	PromptTokens, CachedTokens int
}

type Engine struct {
	cfg Config
	srv *httptest.Server
	tpl templates

	mu      sync.Mutex
	models  map[string]*model
	order   []string // model names in config order
	c       Counters
	clock   int           // orders model use for LRU unloading
	gate    chan struct{} // non-nil: requests pause after prefill until it closes
	changed chan struct{} // closed and replaced on every change
}

type model struct {
	Model
	loaded           bool
	used             int
	running, waiting int
	slots            chan struct{}
	cache            *cache
}

// New starts a fake engine. Close it when done.
func New(cfg Config) *Engine {
	if cfg.Slots <= 0 {
		cfg.Slots = 2
	}
	e := &Engine{cfg: cfg, models: map[string]*model{}, changed: make(chan struct{}), tpl: loadTemplates(cfg.Kind)}
	for _, m := range cfg.Models {
		e.models[m.Name] = &model{Model: m, loaded: cfg.Kind != engine.Ollama, slots: make(chan struct{}, cfg.Slots), cache: newCache(cfg.CacheTokens)}
		e.order = append(e.order, m.Name)
	}
	mux := http.NewServeMux()
	for path, h := range e.handlers() {
		mux.HandleFunc(path, h)
	}
	e.srv = httptest.NewServer(mux)
	return e
}

func (e *Engine) URL() string { return e.srv.URL }

// Close stops the server, cutting open streams: the engine died.
func (e *Engine) Close() {
	e.srv.CloseClientConnections()
	e.srv.Close()
}

// Counters returns the totals so far.
func (e *Engine) Counters() Counters {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.c
}

// WaitFor blocks until ok holds for the counters, or fails after timeout.
func (e *Engine) WaitFor(ok func(Counters) bool, timeout time.Duration) error {
	deadline := time.After(timeout)
	for {
		e.mu.Lock()
		c, ch := e.c, e.changed
		e.mu.Unlock()
		if ok(c) {
			return nil
		}
		select {
		case <-ch:
		case <-deadline:
			return fmt.Errorf("fakeengine: condition not met in %v; counters %+v", timeout, c)
		}
	}
}

// Hold pauses every request that starts generating from now on right after
// its first token, until release is called. Tests use it to keep requests
// running without sleeping, and to see a reply arrive token by token.
func (e *Engine) Hold() (release func()) {
	g := make(chan struct{})
	e.mu.Lock()
	e.gate = g
	e.mu.Unlock()
	return func() {
		e.mu.Lock()
		if e.gate == g {
			e.gate = nil
		}
		e.mu.Unlock()
		close(g)
	}
}

// Unload drops the model from memory, as keep-alive expiry does.
func (e *Engine) Unload(name string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if m := e.models[name]; m != nil && e.cfg.Kind == engine.Ollama {
		e.unloadLocked(m)
	}
}

func (e *Engine) unloadLocked(m *model) {
	if m.loaded {
		m.loaded = false
		m.cache = newCache(e.cfg.CacheTokens)
		e.c.Unloads++
		e.notifyLocked()
	}
}

func (e *Engine) notifyLocked() {
	close(e.changed)
	e.changed = make(chan struct{})
}

// sleep waits sec simulated seconds, or not at all at Speed 0.
func (e *Engine) sleep(ctx context.Context, sec float64) error {
	if e.cfg.Speed <= 0 || sec <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(time.Duration(sec / e.cfg.Speed * float64(time.Second)))
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Engine) handlers() map[string]http.HandlerFunc {
	h := map[string]http.HandlerFunc{
		"GET /v1/models": e.serveModels,
		"/":              func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) },
	}
	for path := range replyFiles {
		if e.cfg.Kind == engine.Ollama || !strings.HasPrefix(path, "/api/") {
			h["POST "+path] = e.serveInference
		}
	}
	switch e.cfg.Kind {
	case engine.Ollama:
		h["GET /api/version"] = e.serveRecorded("api_version")
		h["GET /api/ps"] = e.servePS
		h["GET /api/tags"] = e.serveTags
	case engine.VLLM:
		h["GET /version"] = e.serveRecorded("version")
		h["GET /metrics"] = e.serveMetrics
	case engine.LlamaCpp:
		h["GET /props"] = e.serveProps
		h["GET /slots"] = e.serveSlots
		h["GET /metrics"] = e.serveMetrics
	}
	return h
}

func (e *Engine) serveRecorded(file string) http.HandlerFunc {
	body := e.tpl.file(file)
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(body)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func (e *Engine) serveModels(w http.ResponseWriter, r *http.Request) {
	var data []any
	for _, name := range e.order {
		data = append(data, e.tpl.clone("v1_models", "data", map[string]any{"id": name}))
	}
	writeJSON(w, map[string]any{"object": "list", "data": data})
}

func (e *Engine) servePS(w http.ResponseWriter, r *http.Request) {
	e.mu.Lock()
	defer e.mu.Unlock()
	models := []any{}
	for _, name := range e.order {
		if m := e.models[name]; m.loaded {
			models = append(models, e.tpl.clone("api_ps", "models", map[string]any{"name": name, "model": name, "size": m.SizeBytes, "size_vram": m.SizeBytes}))
		}
	}
	writeJSON(w, map[string]any{"models": models})
}

func (e *Engine) serveTags(w http.ResponseWriter, r *http.Request) {
	var models []any
	for _, name := range e.order {
		models = append(models, e.tpl.clone("api_tags", "models", map[string]any{"name": name, "model": name, "size": e.models[name].SizeBytes}))
	}
	writeJSON(w, map[string]any{"models": models})
}

func (e *Engine) serveProps(w http.ResponseWriter, r *http.Request) {
	var props map[string]any
	json.Unmarshal(e.tpl.file("props"), &props)
	props["total_slots"] = e.cfg.Slots
	writeJSON(w, props)
}

func (e *Engine) serveSlots(w http.ResponseWriter, r *http.Request) {
	e.mu.Lock()
	running := e.c.Running
	e.mu.Unlock()
	var slots []any
	for i := range e.cfg.Slots {
		slots = append(slots, e.tpl.clone("slots", "", map[string]any{"id": i, "is_processing": i < running}))
	}
	writeJSON(w, slots)
}

// metricValue matches one sample line of a gauge the probes read.
var metricValue = regexp.MustCompile(`(?m)^((?:vllm:num_requests_running|vllm:num_requests_waiting|vllm:kv_cache_usage_perc|llamacpp:requests_processing|llamacpp:requests_deferred)(?:\{[^}]*\})?) .*$`)

func (e *Engine) serveMetrics(w http.ResponseWriter, r *http.Request) {
	e.mu.Lock()
	m := e.models[e.order[0]] // single-model kinds
	values := map[string]string{
		"vllm:num_requests_running":    fmt.Sprint(m.running),
		"vllm:num_requests_waiting":    fmt.Sprint(m.waiting),
		"vllm:kv_cache_usage_perc":     fmt.Sprint(m.cache.fill()),
		"llamacpp:requests_processing": fmt.Sprint(m.running),
		"llamacpp:requests_deferred":   fmt.Sprint(m.waiting),
	}
	e.mu.Unlock()
	body := strings.ReplaceAll(string(e.tpl.file("metrics")), `model_name="`+e.tpl.model+`"`, `model_name="`+m.Name+`"`)
	body = metricValue.ReplaceAllStringFunc(body, func(line string) string {
		series, _, _ := strings.Cut(line, " ")
		name, _, _ := strings.Cut(series, "{")
		return series + " " + values[name]
	})
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.Write([]byte(body))
}

// replyFiles names the recorded reply to each path: streamed, then not. ""
// = not recorded, so the fake refuses it.
var replyFiles = map[string][2]string{
	"/v1/chat/completions": {"openai-chat.2.sse", "openai-chat.json"},
	"/v1/completions":      {"", "openai-completion.json"},
	"/v1/embeddings":       {"", "openai-embeddings.json"},
	"/api/chat":            {"ollama-chat.2.ndjson", "ollama-chat.json"},
	"/api/generate":        {"ollama-generate.ndjson", ""},
	"/api/embed":           {"", "ollama-embed.json"},
}

type inferenceRequest struct {
	Model    string `json:"model"`
	Messages []struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
	Prompt        json.RawMessage `json:"prompt"`
	Input         json.RawMessage `json:"input"` // embeddings
	Stream        *bool           `json:"stream"`
	StreamOptions struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options"`
	MaxTokens int `json:"max_tokens"`
	Options   struct {
		NumPredict int `json:"num_predict"`
	} `json:"options"`
}

// serveInference serves chat, completions and embeddings: it waits for a
// slot, loads and prefills, then writes the recorded reply with the fake's
// counts in it. Embeddings generate nothing, so Hold doesn't pause them.
func (e *Engine) serveInference(w http.ResponseWriter, r *http.Request) {
	var req inferenceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	embed := r.URL.Path == "/v1/embeddings" || r.URL.Path == "/api/embed"
	stream := req.Stream != nil && *req.Stream || req.Stream == nil && strings.HasPrefix(r.URL.Path, "/api/") && !embed
	file := replyFiles[r.URL.Path][map[bool]int{true: 0, false: 1}[stream]]
	if file == "" {
		http.Error(w, fmt.Sprintf("fakeengine: no reply to %s with stream=%v is recorded", r.URL.Path, stream), http.StatusBadRequest)
		return
	}
	if rec := e.tpl.replies[file]; !stream && rec.status != http.StatusOK { // the engine refused it
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rec.status)
		w.Write(rec.raw)
		return
	}
	e.mu.Lock()
	m := e.models[req.Model]
	e.mu.Unlock()
	if m == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, `{"error":{"message":"model '%s' not found"}}`, req.Model)
		return
	}
	var text bytes.Buffer
	for _, msg := range req.Messages {
		text.WriteString(msg.Role + "\n")
		text.Write(msg.Content)
		text.WriteString("\n")
	}
	text.Write(req.Prompt)
	text.Write(req.Input)
	promptTok := text.Len()/4 + 1
	outTok := cmp(req.MaxTokens, cmp(req.Options.NumPredict, 8))
	if embed {
		outTok = 0
	}

	// Wait for a slot, as the engine's own queue does.
	ctx := r.Context()
	e.mu.Lock()
	m.waiting++
	e.c.Waiting++
	e.notifyLocked()
	e.mu.Unlock()
	select {
	case m.slots <- struct{}{}:
	case <-ctx.Done():
		e.mu.Lock()
		m.waiting--
		e.c.Waiting--
		e.notifyLocked()
		e.mu.Unlock()
		return
	}
	e.mu.Lock()
	m.waiting--
	e.c.Waiting--
	m.running++
	e.c.Running++
	e.c.Requests++
	cold := !m.loaded
	if cold {
		e.loadLocked(m)
	}
	e.clock++
	m.used = e.clock
	cached := min(m.cache.match(text.Bytes()), promptTok-1)
	e.notifyLocked()
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		m.running--
		e.c.Running--
		e.notifyLocked()
		e.mu.Unlock()
		<-m.slots
	}()

	start := time.Now()
	if cold && e.sleep(ctx, e.cfg.LoadSec) != nil {
		return
	}
	loadDur := time.Since(start)
	if e.sleep(ctx, float64(promptTok-cached)*e.cfg.PrefillSecTok) != nil {
		return
	}
	prefillDur := time.Since(start) - loadDur
	e.mu.Lock()
	m.cache.insert(text.Bytes())
	e.c.PromptTokens += promptTok
	e.c.CachedTokens += cached
	gate := e.gate
	e.mu.Unlock()

	usage := map[string]any{
		"usage.prompt_tokens":                       promptTok,
		"usage.prompt_tokens_details.cached_tokens": cached,
		"timings.cache_n":                           cached,
		"timings.prompt_n":                          promptTok - cached,
		"timings.prompt_ms":                         prefillDur.Seconds() * 1e3,
		"prompt_eval_count":                         promptTok,
		"prompt_eval_cached_count":                  cached,
		"prompt_eval_duration":                      prefillDur.Nanoseconds(),
		"load_duration":                             loadDur.Nanoseconds(),
	}
	ndjson := strings.HasSuffix(file, ".ndjson")
	switch {
	case !stream:
		w.Header().Set("Content-Type", "application/json")
	case ndjson:
		w.Header().Set("Content-Type", "application/x-ndjson")
	default:
		w.Header().Set("Content-Type", "text/event-stream")
	}
	flusher, _ := w.(http.Flusher)
	for i := range outTok {
		if e.sleep(ctx, e.cfg.DecodeSecTok) != nil {
			return
		}
		if stream {
			e.tpl.writeLine(w, file, "content", nil)
			if flusher != nil {
				flusher.Flush()
			}
		}
		if i == 0 && gate != nil {
			select {
			case <-gate:
			case <-ctx.Done():
				return
			}
		}
	}
	switch {
	case !stream:
		w.Write(e.tpl.body(file, usage))
	case ndjson || req.StreamOptions.IncludeUsage:
		e.tpl.writeLine(w, file, "usage", usage)
	}
	if stream && !ndjson {
		fmt.Fprint(w, "data: [DONE]\n\n")
	}
}

func cmp(v, def int) int {
	if v > 0 {
		return v
	}
	return def
}

// loadLocked loads m, first unloading least recently used idle models until it
// fits in memory. If it still doesn't fit, it loads anyway.
func (e *Engine) loadLocked(m *model) {
	for e.cfg.MemoryBytes > 0 {
		used, victim := m.SizeBytes, (*model)(nil)
		for _, o := range e.models {
			if o.loaded {
				used += o.SizeBytes
				if o.running == 0 && o != m && (victim == nil || o.used < victim.used) {
					victim = o
				}
			}
		}
		if used <= e.cfg.MemoryBytes || victim == nil {
			break
		}
		e.unloadLocked(victim)
	}
	m.loaded = true
	e.c.Loads++
}

// cache is an LRU of prompt blocks keyed by the hash of everything up to and
// including the block, so a block only matches after the same prefix.
type cache struct {
	cap    int // blocks
	blocks map[uint64]*list.Element
	lru    list.List
}

const blockBytes = 64 // 16 tokens at 4 bytes per token

func newCache(tokens int) *cache {
	return &cache{cap: tokens * 4 / blockBytes, blocks: map[uint64]*list.Element{}}
}

func chain(text []byte) []uint64 {
	h := fnv.New64a()
	var hs []uint64
	for len(text) >= blockBytes {
		h.Write(text[:blockBytes])
		hs = append(hs, h.Sum64())
		text = text[blockBytes:]
	}
	return hs
}

// match returns the cached prefix of text, in tokens.
func (c *cache) match(text []byte) int {
	n := 0
	for _, h := range chain(text) {
		if _, ok := c.blocks[h]; !ok {
			break
		}
		n++
	}
	return n * blockBytes / 4
}

func (c *cache) insert(text []byte) {
	for _, h := range chain(text) {
		if el, ok := c.blocks[h]; ok {
			c.lru.MoveToFront(el)
		} else {
			c.blocks[h] = c.lru.PushFront(h)
		}
	}
	for c.lru.Len() > c.cap {
		delete(c.blocks, c.lru.Remove(c.lru.Back()).(uint64))
	}
}

func (c *cache) fill() float64 {
	if c.cap == 0 {
		return 0
	}
	return float64(c.lru.Len()) / float64(c.cap)
}

// templates are the recorded bodies and replies of one capture.
type templates struct {
	dir     string
	model   string                    // the model name in the recording
	lines   map[string]map[string]any // "<stream file>/content" and "/usage"
	replies map[string]recorded       // one-line replies by file name
}

type recorded struct {
	status int
	raw    []byte
}

func loadTemplates(k engine.Kind) templates {
	_, self, _, _ := runtime.Caller(0)
	rel, ok := captures[k]
	if !ok {
		panic(fmt.Sprintf("fakeengine: no capture for kind %q", k))
	}
	t := templates{dir: filepath.Join(filepath.Dir(self), "..", "engine", "testdata", rel), lines: map[string]map[string]any{}, replies: map[string]recorded{}}
	var models struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	json.Unmarshal(t.file("v1_models"), &models)
	t.model = models.Data[0].ID
	tsv, err := os.ReadFile(filepath.Join(t.dir, "streams", "replies.tsv"))
	if err != nil {
		panic(err)
	}
	for line := range strings.Lines(string(tsv)) {
		file, status, _ := strings.Cut(strings.TrimSpace(line), "\t")
		raw, err := os.ReadFile(filepath.Join(t.dir, "streams", file))
		if err != nil {
			panic(err)
		}
		t.replies[file] = recorded{atoi(status), bytes.TrimSpace(raw)}
	}
	for _, files := range replyFiles {
		if f := files[0]; f != "" && (k == engine.Ollama || !strings.HasPrefix(f, "ollama-")) {
			t.lines[f+"/content"], t.lines[f+"/usage"] = t.streamLines(f)
		}
	}
	return t
}

func atoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return n
}

// file reads a recorded body from the capture's busy state.
func (t templates) file(name string) []byte {
	b, err := os.ReadFile(filepath.Join(t.dir, "busy", name))
	if err != nil {
		panic(err)
	}
	return b
}

// streamLines returns the first content line and the last usage line of a
// recorded stream.
func (t templates) streamLines(name string) (content, usage map[string]any) {
	f, err := os.Open(filepath.Join(t.dir, "streams", name))
	if err != nil {
		panic(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		line := bytes.TrimPrefix(sc.Bytes(), []byte("data: "))
		var m map[string]any
		if json.Unmarshal(line, &m) != nil {
			continue
		}
		if _, ok := engine.ParseUsage(line); ok {
			usage = m
		} else if content == nil && (bytes.Contains(line, []byte(`"content":"`)) || bytes.Contains(line, []byte(`"response":"`))) {
			content = m
		}
	}
	if content == nil || usage == nil {
		panic("fakeengine: " + name + " has no content or usage line")
	}
	return content, usage
}

// clone copies the first element of the recorded list at key (or the body
// itself if it's a list and key is "") and sets fields in it.
func (t templates) clone(file, key string, fields map[string]any) map[string]any {
	var body any
	json.Unmarshal(t.file(file), &body)
	if key != "" {
		body = body.(map[string]any)[key]
	}
	var m map[string]any
	b, _ := json.Marshal(body.([]any)[0])
	json.Unmarshal(b, &m)
	for k, v := range fields {
		m[k] = v
	}
	return m
}

// writeLine writes a copy of a recorded stream line with each dotted field
// that the recording has set to its value.
func (t templates) writeLine(w http.ResponseWriter, file, kind string, fields map[string]any) {
	b := withFields(t.lines[file+"/"+kind], fields)
	if strings.HasSuffix(file, ".ndjson") {
		fmt.Fprintf(w, "%s\n", b)
	} else {
		fmt.Fprintf(w, "data: %s\n\n", b)
	}
}

// body is a copy of a recorded one-line reply with fields set as in writeLine.
func (t templates) body(file string, fields map[string]any) []byte {
	var m map[string]any
	json.Unmarshal(t.replies[file].raw, &m)
	return withFields(m, fields)
}

func withFields(rec map[string]any, fields map[string]any) []byte {
	b, _ := json.Marshal(rec)
	var m map[string]any
	json.Unmarshal(b, &m)
	for _, path := range slices.Sorted(maps.Keys(fields)) {
		setIfPresent(m, strings.Split(path, "."), fields[path])
	}
	b, _ = json.Marshal(m)
	return b
}

// setIfPresent sets path in m only if the recording has it, so the fake
// reports exactly the fields that engine version reports.
func setIfPresent(m map[string]any, path []string, v any) {
	for _, k := range path[:len(path)-1] {
		next, ok := m[k].(map[string]any)
		if !ok {
			return
		}
		m = next
	}
	if _, ok := m[path[len(path)-1]]; ok {
		m[path[len(path)-1]] = v
	}
}
