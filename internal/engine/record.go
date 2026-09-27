package engine

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// RecordPaths is every path Record saves: each library probe's path,
// plus candidates from STRATEGY §4 that no probe reads yet, so a recording can
// back a future probe and prove one engine's probe never matches another's.
var RecordPaths = []string{
	"/health", "/version", "/api/version", "/api/ps", "/api/tags", "/props", "/slots", "/metrics", "/models", "/running",
	"/server_info", "/get_server_info", "/get_model_info", "/v1/loads", "/v1/models", "/api/v1/models", "/prometheus/metrics",
}

// Record GETs every RecordPaths path from base and writes one capture state
// dir, the layer-2 fixture layout that serveCapture replays:
// one raw body per path (/api/ps → api_ps) and paths.tsv (path, status,
// content type). A 404 or empty body has no file; a failed GET is status 000.
func Record(ctx context.Context, c *http.Client, base, dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	var tsv strings.Builder
	for _, p := range RecordPaths {
		status, ctype, body := "000", "-", []byte(nil)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+p, nil)
		if err != nil {
			return err
		}
		if resp, err := c.Do(req); err == nil {
			body, err = io.ReadAll(resp.Body)
			resp.Body.Close()
			if err == nil {
				status, ctype = fmt.Sprintf("%03d", resp.StatusCode), resp.Header.Get("Content-Type")
			}
		}
		fmt.Fprintf(&tsv, "%s\t%s\t%s\n", p, status, ctype)
		if len(body) == 0 || status == "404" {
			continue
		}
		if err := os.WriteFile(filepath.Join(dir, captureFile(p)), body, 0o644); err != nil {
			return err
		}
	}
	return os.WriteFile(filepath.Join(dir, "paths.tsv"), []byte(tsv.String()), 0o644)
}

// captureFile is the body file name Record writes for a path: /api/ps → api_ps.
func captureFile(p string) string {
	return strings.ReplaceAll(strings.TrimPrefix(p, "/"), "/", "_")
}
