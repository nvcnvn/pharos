// Package proxy serves the OpenAI-compatible and Ollama APIs: it parses each
// request, takes a lease from the scheduler, forwards the request and streams
// the reply back token by token, reading usage on the way (ARCHITECTURE §9).
package proxy

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nvcnvn/pharos/internal/config"
	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/prefix"
	"github.com/nvcnvn/pharos/internal/sched"
	"github.com/nvcnvn/pharos/internal/state"
	"github.com/nvcnvn/pharos/internal/usage"
)

// maxBody bounds a request body; base64 images make them large.
const maxBody = 32 << 20

// promptBlock is the block size a plain prompt is hashed in.
const promptBlock = 1 << 10

type Proxy struct {
	st       *state.State
	sc       *sched.Sched
	usage    *usage.Counter
	onDone   func(Done)
	client   *http.Client
	mux      *http.ServeMux
	keys     atomic.Pointer[keySet]
	draining atomic.Bool
}

type Options struct {
	Client *http.Client   // forwards to engines; nil = UpstreamClient()
	Usage  *usage.Counter // nil = a counter of its own
	OnDone func(Done)     // every finished request, for /metrics and /status; it must not block
}

// Done is one finished request.
type Done struct {
	At             time.Time
	Key, Model     string // key name ("" with no keys configured); model "" before the body was read
	Path           string
	Target, Reason string // where it went and why; "" if it was never routed
	Status         int    // sent to the client; 499 = the client left before a reply
	TTFT, Duration time.Duration
	Usage          engine.Usage
	Decisions      []Decision    // every branch it took, in order
	PrefixSource   string        // where the prefix prediction its "prefix" decision judged came from; "" = none
	Overhead       time.Duration // Pharos's own time before the first upstream send, queue wait excluded; 0 = never sent
}

// Decision is one branch a request took. Stage and Outcome come from a fixed
// set, so they can be metric labels (ARCHITECTURE §13).
type Decision struct{ Stage, Outcome string }

func (d *Done) note(stage, outcome string) {
	d.Decisions = append(d.Decisions, Decision{stage, outcome})
}

// keySet maps each key's SHA-256 to its entry. With no keys, every client is
// let in, as one anonymous key.
type keySet struct {
	byHash map[[32]byte]*config.Key
}

// open is the key of every request when no keys are configured.
var open = &config.Key{Weight: 1}

// New returns the public handler.
func New(st *state.State, sc *sched.Sched, o Options) *Proxy {
	if o.Client == nil {
		o.Client = UpstreamClient()
	}
	if o.Usage == nil {
		o.Usage = usage.New(usage.Config{})
	}
	if o.OnDone == nil {
		o.OnDone = func(Done) {}
	}
	p := &Proxy{st: st, sc: sc, usage: o.Usage, onDone: o.OnDone, client: o.Client, mux: http.NewServeMux()}
	p.SetKeys(nil)
	for _, path := range []string{"/v1/chat/completions", "/v1/completions", "/v1/embeddings"} {
		p.mux.HandleFunc("POST "+path, p.auth(p.route("")))
	}
	for _, path := range []string{"/api/chat", "/api/generate", "/api/embed"} {
		p.mux.HandleFunc("POST "+path, p.auth(p.route(engine.Ollama))) // v1: no API translation
	}
	p.mux.HandleFunc("GET /v1/models", p.auth(p.models))
	p.mux.HandleFunc("GET /v1/models/{id...}", p.auth(p.model))
	p.mux.HandleFunc("GET /api/tags", p.auth(p.ollamaList("/api/tags")))
	p.mux.HandleFunc("GET /api/ps", p.auth(p.ollamaList("/api/ps")))
	p.mux.HandleFunc("POST /api/show", p.auth(p.ollamaShow))
	// The ollama CLI sends HEAD / before every command, without a key (0.32.15, spike).
	p.mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "Ollama is running") })
	p.mux.HandleFunc("GET /api/version", p.ollamaVersion)
	p.mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		ollamaError(w, http.StatusNotImplemented, "Pharos routes requests and doesn't serve %s %s: run it on a backend", r.Method, r.URL.Path)
	})
	p.mux.HandleFunc("GET /healthz", p.healthz)
	return p
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) { p.mux.ServeHTTP(w, r) }

// SetKeys replaces the API keys, e.g. on a config reload. Requests already
// admitted keep the key they had.
func (p *Proxy) SetKeys(keys []config.Key) {
	ks := &keySet{byHash: map[[32]byte]*config.Key{}}
	for _, k := range keys {
		ks.byHash[k.SHA256] = &k
	}
	p.keys.Store(ks)
}

// Drain makes /healthz fail, so load balancers stop sending new requests.
// Requests are still served.
func (p *Proxy) Drain() { p.draining.Store(true) }

type keyCtx struct{}

// keyOf is the request's key, set by auth.
func keyOf(r *http.Request) *config.Key { return r.Context().Value(keyCtx{}).(*config.Key) }

// auth looks up the bearer key. With no keys configured, everyone is let in.
func (p *Proxy) auth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		k, ok := p.lookup(r)
		if !ok {
			p.onDone(Done{At: time.Now(), Path: r.URL.Path, Status: http.StatusUnauthorized, Decisions: []Decision{{"admission", "unauthorized"}}})
			apiError(w, http.StatusUnauthorized, "invalid_api_key", "missing or unknown API key: send Authorization: Bearer <key>")
			return
		}
		h(w, r.WithContext(context.WithValue(r.Context(), keyCtx{}, k)))
	}
}

func (p *Proxy) lookup(r *http.Request) (*config.Key, bool) {
	ks := p.keys.Load()
	if len(ks.byHash) == 0 {
		return open, true
	}
	bearer, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		return nil, false
	}
	k := ks.byHash[config.HashKey(strings.TrimSpace(bearer))]
	return k, k != nil
}

// Admin lets only admin keys through to h (or everyone, with no keys configured).
func (p *Proxy) Admin(h http.Handler) http.Handler {
	return p.auth(func(w http.ResponseWriter, r *http.Request) {
		if k := keyOf(r); k != open && !k.Admin {
			apiError(w, http.StatusForbidden, "permission_denied", "key %s is not an admin key", k.Name)
			return
		}
		h.ServeHTTP(w, r)
	})
}

// allowed reports whether k may use model.
func allowed(k *config.Key, model string) bool {
	return len(k.Models) == 0 || slices.Contains(k.Models, model)
}

// UpstreamClient keeps connections to engines alive and asks for identity
// encoding, so the tap sees plain bytes.
// ponytail: no response-header timeout; add per-decision timeouts (long for
// cold loads) once engines' header timing is measured.
func UpstreamClient() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConnsPerHost = 64
	t.DisableCompression = true
	return &http.Client{Transport: t}
}

type request struct {
	Model    string `json:"model"`
	Messages []struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
	Tools  json.RawMessage `json:"tools"`
	Prompt json.RawMessage `json:"prompt"`
	Stream *bool           `json:"stream"`
}

// text is the part of a message that engines tokenize: a JSON string's text,
// so a client that escapes non-ASCII as \uXXXX (Open WebUI, Python's requests
// and aiohttp) and one that sends UTF-8 hash and count the same prompt. An
// escaped Cyrillic character is 6 bytes where the text has 2, which made a
// cache hit look like a wrong prediction.
// ponytail: an array of content parts stays as sent, escapes included; decode
// its text parts if clients that send arrays turn out to escape.
func text(raw json.RawMessage) []byte {
	if len(raw) < 2 || raw[0] != '"' {
		return raw
	}
	if bytes.IndexByte(raw, '\\') < 0 {
		return raw[1 : len(raw)-1] // nothing to decode
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return raw
	}
	return []byte(s)
}

// withUsage returns body with stream_options.include_usage set, or false when
// the client already asked for usage or stream_options isn't an object (the
// engine answers that). Quotas need the token counts of every stream.
// ponytail: re-encodes the body; do a targeted byte insert if it shows in profiles.
func withUsage(body []byte) ([]byte, bool) {
	var req map[string]json.RawMessage
	if json.Unmarshal(body, &req) != nil {
		return body, false
	}
	var opts map[string]json.RawMessage
	if o, ok := req["stream_options"]; ok && json.Unmarshal(o, &opts) != nil || string(opts["include_usage"]) == "true" {
		return body, false
	}
	if opts == nil {
		opts = map[string]json.RawMessage{}
	}
	opts["include_usage"] = json.RawMessage("true")
	req["stream_options"], _ = json.Marshal(opts)
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if enc.Encode(req) != nil {
		return body, false
	}
	return b.Bytes(), true
}

func (p *Proxy) route(kind engine.Kind) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		k := keyOf(r)
		w := &statusWriter{ResponseWriter: rw}
		d := &Done{At: time.Now(), Key: k.Name, Path: r.URL.Path}
		defer func() {
			d.Status, d.Duration = cmp.Or(w.status, 499), time.Since(d.At)
			p.onDone(*d)
		}()
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
		if err != nil {
			if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
				d.note("admission", "too_large")
				apiError(w, http.StatusRequestEntityTooLarge, "invalid_request_error", "request body over %d bytes", maxBody)
				return
			}
			d.note("admission", "bad_request")
			apiError(w, http.StatusBadRequest, "invalid_request_error", "reading body: %v", err)
			return
		}
		var req request
		if err := json.Unmarshal(body, &req); err != nil || req.Model == "" {
			d.note("admission", "bad_request")
			apiError(w, http.StatusBadRequest, "invalid_request_error", "want a JSON body with a model")
			return
		}
		d.Model = req.Model
		if !allowed(k, req.Model) {
			d.note("admission", "model_not_allowed")
			apiError(w, http.StatusNotFound, "model_not_found", "model %q does not exist or key %s may not use it", req.Model, k.Name)
			return
		}
		if wait, err := p.usage.Admit(k.Name, usage.Limits{RPM: k.RPM, TokensPerDay: k.TokensPerDay}); err != nil {
			w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(wait.Seconds()))))
			code, outcome := "rate_limit_exceeded", "rate_limited"
			if errors.Is(err, usage.ErrQuotaExceeded) {
				code, outcome = "insufficient_quota", "quota_exceeded"
			}
			d.note("admission", outcome)
			apiError(w, http.StatusTooManyRequests, code, "key %s: %v", k.Name, err)
			return
		}
		d.note("admission", "ok")
		var parts [][]byte
		for _, m := range req.Messages {
			parts = append(parts, append([]byte(m.Role+"\x00"), text(m.Content)...))
		}
		if len(req.Prompt) > 0 {
			parts = append(parts, prefix.Blocks(text(req.Prompt), promptBlock)...)
		}
		chain := prefix.Chain(req.Model, req.Tools, parts)
		tokens := len(req.Tools)
		if len(chain) > 0 {
			tokens = chain[len(chain)-1].Bytes
		}
		// Ollama's native chat and generate stream unless told not to.
		streamed := req.Stream != nil && *req.Stream || req.Stream == nil && kind == engine.Ollama && r.URL.Path != "/api/embed"
		// Ask for the usage chunk the client didn't, and strip it from the reply.
		// Only chat streams: every recorded engine answers include_usage there.
		strip := false
		if streamed && r.URL.Path == "/v1/chat/completions" {
			body, strip = withUsage(body)
			d.note("usage", map[bool]string{true: "pharos_asked", false: "client_asked"}[strip])
		}
		sr := sched.Request{Model: req.Model, Kind: kind, Chain: chain, PromptTokens: tokens / 4, Weight: k.Weight}
		c := &call{key: k.Name, model: req.Model, body: body, streamed: streamed, strip: strip,
			embed: strings.HasSuffix(r.URL.Path, "/embeddings") || strings.HasSuffix(r.URL.Path, "/embed")}
		// Ollama loads or unloads the model (keep_alive decides) for a chat or
		// generate with nothing to run: the ollama CLI's rm and stop send one.
		// It measures neither load nor service time.
		if kind == engine.Ollama && !c.embed && len(req.Messages) == 0 && (len(req.Prompt) == 0 || string(req.Prompt) == `""`) {
			c.control = true
			d.note("load", "control")
		}
		for attempt := 1; ; attempt++ {
			acquired := time.Now()
			lease, err := p.sc.Acquire(r.Context(), k.Name, sr)
			if err != nil {
				schedError(w, r, d, req.Model, kind, err)
				return
			}
			d.note("route", lease.Why)
			d.note("queue", map[bool]string{true: "waited", false: "immediate"}[lease.Queued])
			if lease.Cold {
				d.note("load", "cold_start")
			}
			if d.Overhead == 0 {
				d.Overhead = time.Since(d.At)
				if lease.Queued {
					d.Overhead -= time.Since(acquired)
				}
			}
			if !p.forward(w, r, lease, c, d, attempt == 2) {
				return
			}
			sr.Avoid = append(sr.Avoid, lease.Target.ID)
		}
	}
}

// call is one client request on its way to an engine.
type call struct {
	key, model      string
	body            []byte
	streamed, strip bool
	embed           bool // generates no tokens
	control         bool // loads or unloads the model: nothing to learn from
}

// forward sends the request to the lease's target and copies the reply back.
// It returns true, having written nothing, when the request should be retried
// on another target: the engine refused the connection or answered 503.
// There is no retry once a byte has reached the client.
func (p *Proxy) forward(w http.ResponseWriter, r *http.Request, l *sched.Lease, c *call, d *Done, last bool) (retry bool) {
	start := time.Now()
	d.Target, d.Reason = l.Target.Key, l.Reason
	url := l.Target.Backend.Spec.URL + r.URL.Path
	if r.URL.RawQuery != "" {
		url += "?" + r.URL.RawQuery
	}
	up, err := http.NewRequestWithContext(r.Context(), r.Method, url, bytes.NewReader(c.body))
	if err != nil {
		l.Release(sched.Feedback{})
		d.note("upstream", "pharos_error")
		apiError(w, http.StatusInternalServerError, "pharos_error", "%v", err)
		return false
	}
	for k, vs := range r.Header {
		if !hopByHop[k] && k != "Authorization" && k != "Accept-Encoding" && k != "Content-Length" {
			up.Header[k] = vs
		}
	}
	resp, err := p.client.Do(up)
	if err != nil {
		l.Release(sched.Feedback{})
		if r.Context().Err() != nil {
			d.note("upstream", "client_left")
			return false
		}
		d.note("upstream", "connect_failed") // ejected; retried unless last
		slog.Warn("upstream failed; ejecting backend", "target", l.Target.Key, "err", err)
		l.Target.Backend.Eject()
		if last {
			apiError(w, http.StatusBadGateway, "upstream_error", "upstream %s: %v", l.Target.Backend.Spec.URL, err)
		}
		return !last
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusServiceUnavailable && !last {
		l.Release(sched.Feedback{})
		d.note("upstream", "busy_retried")
		return true
	}
	h := w.Header()
	for k, vs := range resp.Header {
		if !hopByHop[k] {
			h[k] = vs
		}
	}
	w.WriteHeader(resp.StatusCode)

	t := tap{strip: c.strip}
	ttft, readErr, writeErr := copyFlush(w, resp.Body, &t, start)
	if rest := t.close(); len(rest) > 0 && writeErr == nil {
		_, writeErr = w.Write(rest)
	}
	ok := resp.StatusCode == http.StatusOK && readErr == nil && writeErr == nil
	if c.embed && !t.usage.CompletionTokens.OK {
		t.usage.CompletionTokens = engine.Opt[int]{V: 0, OK: true}
	}
	p.usage.Record(c.key, c.model, t.usage)
	d.TTFT, d.Usage = ttft, t.usage
	if po := l.Release(sched.Feedback{OK: ok && !c.control, Streamed: c.streamed, TTFT: ttft, Duration: time.Since(start), Usage: t.usage}); po != "" {
		d.note("prefix", po)
		d.PrefixSource = l.PrefixSource
	}
	switch {
	case r.Context().Err() != nil:
		d.note("upstream", "client_left")
	case readErr != nil:
		d.note("upstream", "died_mid_reply") // ejected
		slog.Warn("upstream died mid-reply; ejecting backend", "target", l.Target.Key, "err", readErr)
		l.Target.Backend.Eject()
	case writeErr != nil:
		d.note("upstream", "client_left")
	case resp.StatusCode != http.StatusOK:
		d.note("upstream", "error_status")
	default:
		d.note("upstream", "ok")
	}
	return false
}

var hopByHop = map[string]bool{
	"Connection": true, "Keep-Alive": true, "Proxy-Connection": true, "Proxy-Authenticate": true,
	"Proxy-Authorization": true, "Te": true, "Trailer": true, "Transfer-Encoding": true, "Upgrade": true,
}

var bufs = sync.Pool{New: func() any { b := make([]byte, 32<<10); return &b }}

// copyFlush copies body to w, flushing after every read so each token reaches
// the client as soon as the engine sends it. ttft is the time to the first byte.
func copyFlush(w http.ResponseWriter, body io.Reader, t *tap, start time.Time) (ttft time.Duration, readErr, writeErr error) {
	bp := bufs.Get().(*[]byte)
	defer bufs.Put(bp)
	buf := *bp
	rc := http.NewResponseController(w)
	for {
		n, err := body.Read(buf)
		if n > 0 {
			if ttft == 0 {
				ttft = time.Since(start)
			}
			if out := t.write(buf[:n]); len(out) > 0 {
				if _, err := w.Write(out); err != nil {
					return ttft, nil, err
				}
				rc.Flush()
			}
		}
		if err == io.EOF {
			return ttft, nil, nil
		}
		if err != nil {
			return ttft, err, nil
		}
	}
}

func schedError(w http.ResponseWriter, r *http.Request, d *Done, model string, kind engine.Kind, err error) {
	switch {
	case r.Context().Err() != nil:
		d.note("route", "client_left") // while queued
	case errors.Is(err, sched.ErrUnknownModel):
		d.note("route", "unknown_model")
		apiError(w, http.StatusNotFound, "model_not_found", "model %q is not served by any backend", model)
	case errors.Is(err, sched.ErrUnavailable) && kind != "":
		d.note("route", "unavailable")
		apiError(w, http.StatusServiceUnavailable, "model_unavailable", "model %q: no %s backend serving it is up (%s goes to %s backends only)", model, kind, r.URL.Path, kind)
	case errors.Is(err, sched.ErrQueueFull):
		d.note("route", "queue_full")
		apiError(w, http.StatusTooManyRequests, "queue_full", "%v", err)
	default:
		d.note("route", "unavailable")
		apiError(w, http.StatusServiceUnavailable, "model_unavailable", "model %q: %v", model, err)
	}
}

// statusWriter remembers the status sent to the client.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController flush the stream through it.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// apiError writes an OpenAI-style error body. SDKs read code: OpenAI's own
// codes where one fits (invalid_api_key, model_not_found,
// rate_limit_exceeded, insufficient_quota).
func apiError(w http.ResponseWriter, status int, code, format string, a ...any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": fmt.Sprintf(format, a...), "type": "pharos", "code": code}})
}

// models lists every model an up backend serves that the key may use.
func (p *Proxy) models(w http.ResponseWriter, r *http.Request) {
	data := []any{}
	for _, name := range slices.Sorted(maps.Keys(p.st.Models(p.st.Now()))) {
		if allowed(keyOf(r), name) {
			data = append(data, map[string]any{"id": name, "object": "model", "owned_by": "pharos"})
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data})
}

// model is /v1/models/{id}: the model, if an up backend serves it and the key may use it.
func (p *Proxy) model(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, ok := p.st.Models(p.st.Now())[id]; !ok || !allowed(keyOf(r), id) {
		apiError(w, http.StatusNotFound, "model_not_found", "model %q is not served by any backend", id)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"id": id, "object": "model", "owned_by": "pharos"})
}

// ollamaError writes Ollama's error body, which the ollama CLI prints.
func ollamaError(w http.ResponseWriter, status int, format string, a ...any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": fmt.Sprintf(format, a...)})
}

// ollamaVersion answers with the oldest version among the up Ollama backends:
// clients gate features on it, and a request can land on any of them.
func (p *Proxy) ollamaVersion(w http.ResponseWriter, r *http.Request) {
	now, oldest := p.st.Now(), ""
	for _, b := range p.st.Backends() {
		if i := b.Info(now); i.Up && i.Kind == engine.Ollama && i.Plan.Version.OK && (oldest == "" || older(i.Plan.Version.V, oldest)) {
			oldest = i.Plan.Version.V
		}
	}
	if oldest == "" {
		ollamaError(w, http.StatusServiceUnavailable, "no Ollama backend is up")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"version": oldest})
}

// older compares dotted versions number by number; a part that isn't a number counts as 0.
func older(a, b string) bool {
	return slices.CompareFunc(strings.Split(a, "."), strings.Split(b, "."), func(x, y string) int {
		xi, _ := strconv.Atoi(x)
		yi, _ := strconv.Atoi(y)
		return cmp.Compare(xi, yi)
	}) < 0
}

// ollamaShow forwards /api/show (model details) to an up Ollama backend
// serving the model. It takes no slot: nothing runs on the engine.
func (p *Proxy) ollamaShow(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
	var req struct{ Model, Name string } // name: older clients
	if err != nil || json.Unmarshal(body, &req) != nil {
		ollamaError(w, http.StatusBadRequest, "want a JSON body with a model")
		return
	}
	model, now := cmp.Or(req.Model, req.Name), p.st.Now()
	targets := p.st.Targets(model)
	i := slices.IndexFunc(targets, func(t *state.Target) bool { v := t.View(now); return v.Up && v.Kind == engine.Ollama })
	if i < 0 || !allowed(keyOf(r), model) {
		ollamaError(w, http.StatusNotFound, "model %q not found on any Ollama backend", model) // the CLI offers to pull it
		return
	}
	url := targets[i].Backend.Spec.URL + r.URL.Path
	up, err := http.NewRequestWithContext(r.Context(), http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		ollamaError(w, http.StatusInternalServerError, "%v", err)
		return
	}
	up.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(up)
	if err != nil {
		ollamaError(w, http.StatusBadGateway, "upstream %s: %v", url, err)
		return
	}
	defer resp.Body.Close()
	for k, vs := range resp.Header {
		if !hopByHop[k] {
			w.Header()[k] = vs
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// ollamaList merges an Ollama model list (/api/tags or /api/ps) across the
// Ollama backends, one entry per model name.
func (p *Proxy) ollamaList(path string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		seen := map[string]bool{}
		merged := []json.RawMessage{}
		for _, b := range p.st.Backends() {
			if b.Kind() != engine.Ollama {
				continue
			}
			var list struct {
				Models []json.RawMessage `json:"models"`
			}
			if err := getJSON(ctx, p.client, b.Spec.URL+path, &list); err != nil {
				slog.Warn("ollama list", "backend", b.Spec.URL, "path", path, "err", err)
				continue
			}
			for _, m := range list.Models {
				var id struct {
					Name string `json:"name"`
				}
				if json.Unmarshal(m, &id) == nil && !seen[id.Name] {
					seen[id.Name] = true
					merged = append(merged, m)
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"models": merged})
	}
}

func getJSON(ctx context.Context, c *http.Client, url string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return json.NewDecoder(io.LimitReader(resp.Body, maxBody)).Decode(v)
}

// healthz is 503 until every backend finished a first scrape round, so a new
// instance never routes blind, and again while draining.
func (p *Proxy) healthz(w http.ResponseWriter, r *http.Request) {
	switch {
	case p.draining.Load():
		http.Error(w, "draining", http.StatusServiceUnavailable)
		return
	case !p.st.Ready():
		http.Error(w, "starting", http.StatusServiceUnavailable)
		return
	}
	fmt.Fprintln(w, "ok")
}
