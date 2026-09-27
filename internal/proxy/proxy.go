// Package proxy serves the OpenAI-compatible and Ollama APIs: it parses each
// request, takes a lease from the scheduler, forwards the request and streams
// the reply back token by token, reading usage on the way (ARCHITECTURE §9).
package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/prefix"
	"github.com/nvcnvn/pharos/internal/sched"
	"github.com/nvcnvn/pharos/internal/state"
)

// maxBody bounds a request body; base64 images make them large.
const maxBody = 32 << 20

// promptBlock is the block size a plain prompt is hashed in.
const promptBlock = 1 << 10

type Proxy struct {
	st     *state.State
	sc     *sched.Sched
	client *http.Client
	mux    *http.ServeMux
}

// New returns the public handler. client forwards to engines; nil = a
// keep-alive client without timeouts (streams can be long).
func New(st *state.State, sc *sched.Sched, client *http.Client) *Proxy {
	if client == nil {
		client = UpstreamClient()
	}
	p := &Proxy{st: st, sc: sc, client: client, mux: http.NewServeMux()}
	for _, path := range []string{"/v1/chat/completions", "/v1/completions", "/v1/embeddings"} {
		p.mux.HandleFunc("POST "+path, p.route(""))
	}
	for _, path := range []string{"/api/chat", "/api/generate", "/api/embed"} {
		p.mux.HandleFunc("POST "+path, p.route(engine.Ollama)) // v1: no API translation
	}
	p.mux.HandleFunc("GET /v1/models", p.models)
	p.mux.HandleFunc("GET /api/tags", p.ollamaList("/api/tags"))
	p.mux.HandleFunc("GET /api/ps", p.ollamaList("/api/ps"))
	p.mux.HandleFunc("GET /healthz", p.healthz)
	return p
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) { p.mux.ServeHTTP(w, r) }

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

func (p *Proxy) route(kind engine.Kind) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
		if err != nil {
			if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
				apiError(w, http.StatusRequestEntityTooLarge, "request body over %d bytes", maxBody)
				return
			}
			apiError(w, http.StatusBadRequest, "reading body: %v", err)
			return
		}
		var req request
		if err := json.Unmarshal(body, &req); err != nil || req.Model == "" {
			apiError(w, http.StatusBadRequest, "want a JSON body with a model")
			return
		}
		var parts [][]byte
		for _, m := range req.Messages {
			parts = append(parts, append([]byte(m.Role+"\x00"), m.Content...))
		}
		if len(req.Prompt) > 0 {
			parts = append(parts, prefix.Blocks(req.Prompt, promptBlock)...)
		}
		chain := prefix.Chain(req.Model, req.Tools, parts)
		tokens := len(req.Tools)
		if len(chain) > 0 {
			tokens = chain[len(chain)-1].Bytes
		}
		// Ollama's native chat and generate stream unless told not to.
		streamed := req.Stream != nil && *req.Stream || req.Stream == nil && kind == engine.Ollama && r.URL.Path != "/api/embed"
		sr := sched.Request{Model: req.Model, Kind: kind, Chain: chain, PromptTokens: tokens / 4}

		key := "" // ponytail: one fair-queue key until API keys arrive (build step 4)
		for attempt := 1; ; attempt++ {
			lease, err := p.sc.Acquire(r.Context(), key, sr)
			if err != nil {
				schedError(w, r, req.Model, kind, err)
				return
			}
			if !p.forward(w, r, lease, body, streamed, attempt == 2) {
				return
			}
			sr.Avoid = append(sr.Avoid, lease.Target.ID)
		}
	}
}

// forward sends the request to the lease's target and copies the reply back.
// It returns true, having written nothing, when the request should be retried
// on another target: the engine refused the connection or answered 503.
// There is no retry once a byte has reached the client.
func (p *Proxy) forward(w http.ResponseWriter, r *http.Request, l *sched.Lease, body []byte, streamed, last bool) (retry bool) {
	start := time.Now()
	url := l.Target.Backend.Spec.URL + r.URL.Path
	if r.URL.RawQuery != "" {
		url += "?" + r.URL.RawQuery
	}
	up, err := http.NewRequestWithContext(r.Context(), r.Method, url, bytes.NewReader(body))
	if err != nil {
		l.Release(sched.Feedback{})
		apiError(w, http.StatusInternalServerError, "%v", err)
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
			return false // the client left
		}
		slog.Warn("upstream failed; ejecting backend", "target", l.Target.Key, "err", err)
		l.Target.Backend.Eject()
		if last {
			apiError(w, http.StatusBadGateway, "upstream %s: %v", l.Target.Backend.Spec.URL, err)
		}
		return !last
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusServiceUnavailable && !last {
		l.Release(sched.Feedback{})
		return true
	}
	h := w.Header()
	for k, vs := range resp.Header {
		if !hopByHop[k] {
			h[k] = vs
		}
	}
	w.WriteHeader(resp.StatusCode)

	var t tap
	ttft, readErr, writeErr := copyFlush(w, resp.Body, &t, start)
	t.end()
	ok := resp.StatusCode == http.StatusOK && readErr == nil && writeErr == nil
	l.Release(sched.Feedback{OK: ok, Streamed: streamed, TTFT: ttft, Duration: time.Since(start), Usage: t.usage})
	if readErr != nil && r.Context().Err() == nil {
		slog.Warn("upstream died mid-reply; ejecting backend", "target", l.Target.Key, "err", readErr)
		l.Target.Backend.Eject()
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
			t.write(buf[:n])
			if _, err := w.Write(buf[:n]); err != nil {
				return ttft, nil, err
			}
			rc.Flush()
		}
		if err == io.EOF {
			return ttft, nil, nil
		}
		if err != nil {
			return ttft, err, nil
		}
	}
}

func schedError(w http.ResponseWriter, r *http.Request, model string, kind engine.Kind, err error) {
	switch {
	case r.Context().Err() != nil: // the client left while queued
	case errors.Is(err, sched.ErrUnknownModel):
		apiError(w, http.StatusNotFound, "model %q is not served by any backend", model)
	case errors.Is(err, sched.ErrUnavailable) && kind != "":
		apiError(w, http.StatusServiceUnavailable, "model %q: no %s backend serving it is up (%s goes to %s backends only)", model, kind, r.URL.Path, kind)
	case errors.Is(err, sched.ErrQueueFull):
		apiError(w, http.StatusTooManyRequests, "%v", err)
	default:
		apiError(w, http.StatusServiceUnavailable, "model %q: %v", model, err)
	}
}

// apiError writes an OpenAI-style error body.
func apiError(w http.ResponseWriter, status int, format string, a ...any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": fmt.Sprintf(format, a...), "type": "pharos"}})
}

// models lists every model an up backend serves.
func (p *Proxy) models(w http.ResponseWriter, r *http.Request) {
	data := []any{}
	for _, name := range slices.Sorted(maps.Keys(p.st.Models(p.st.Now()))) {
		data = append(data, map[string]any{"id": name, "object": "model", "owned_by": "pharos"})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data})
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
// instance never routes blind.
func (p *Proxy) healthz(w http.ResponseWriter, r *http.Request) {
	if !p.st.Ready() {
		http.Error(w, "starting", http.StatusServiceUnavailable)
		return
	}
	fmt.Fprintln(w, "ok")
}
