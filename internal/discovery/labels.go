package discovery

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nvcnvn/pharos/internal/engine"
	"github.com/nvcnvn/pharos/internal/state"
)

// labelFilter selects the containers Pharos routes to.
const labelFilter = `{"label":["pharos.enable=true"]}`

// DockerAvailable reports whether the local Docker socket answers.
func DockerAvailable(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker/_ping", nil)
	if err != nil {
		return false
	}
	resp, err := dockerClient().Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// DockerLabels watches the local Docker for running containers labeled
// pharos.enable=true and calls update with one backend per container: once
// at start, then after every event of such a container. Each backend's log
// feed is its container. A container whose labels don't make a backend is
// left out and logged. It reconnects with backoff until ctx is done.
//
// Labels: pharos.enable=true (required); pharos.url, else
// http://<container IP>:<pharos.port, else the one exposed TCP port>;
// pharos.kind (default auto); pharos.memory_gb; pharos.capacity.
func DockerLabels(ctx context.Context, update func([]state.BackendSpec)) error {
	c := dockerClient()
	logged := map[string]string{} // container ID -> the error last logged for it
	backoff := time.Second
	for {
		err := watch(ctx, c, func() error {
			specs, err := list(ctx, c, logged)
			if err == nil {
				update(specs)
				backoff = time.Second
			}
			return err
		})
		if ctx.Err() != nil {
			return ctx.Err()
		}
		slog.Warn("docker discovery: reconnecting", "in", backoff, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 30*time.Second)
	}
}

// watch opens the event stream, then lists (so no event between the two is
// missed), then lists again after each event, until the stream ends.
func watch(ctx context.Context, c *http.Client, relist func() error) error {
	q := url.Values{"filters": {`{"type":["container"],"label":["pharos.enable=true"]}`}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker/events?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := c.Do(req)
	if err != nil {
		return fmt.Errorf("docker events: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("docker events: status %d", resp.StatusCode)
	}
	if err := relist(); err != nil {
		return err
	}
	dec := json.NewDecoder(bufio.NewReader(resp.Body))
	for {
		var ev struct{ Action string }
		if err := dec.Decode(&ev); err != nil {
			return fmt.Errorf("docker events: %w", err)
		}
		// Any action can change the set (start, die, destroy, …); a list is cheap.
		if err := relist(); err != nil {
			return err
		}
	}
}

// container is the part of GET /containers/json Pharos reads
// (testdata/docker-29.1.3-containers.json).
type container struct {
	ID     string `json:"Id"`
	Names  []string
	Labels map[string]string
	Ports  []struct {
		PrivatePort int
		Type        string
	}
	NetworkSettings struct {
		Networks map[string]struct{ IPAddress string }
	}
}

func list(ctx context.Context, c *http.Client, logged map[string]string) ([]state.BackendSpec, error) {
	var cs []container
	if err := dockerGet(ctx, c, "/containers/json?"+url.Values{"filters": {labelFilter}}.Encode(), &cs); err != nil {
		return nil, err
	}
	var specs []state.BackendSpec
	seen := map[string]bool{}
	for _, ct := range cs {
		seen[ct.ID] = true
		spec, err := labelSpec(ct)
		if err != nil {
			if logged[ct.ID] != err.Error() {
				logged[ct.ID] = err.Error()
				slog.Warn("docker discovery: container left out", "container", name(ct), "err", err)
			}
			continue
		}
		delete(logged, ct.ID)
		specs = append(specs, spec)
	}
	maps.DeleteFunc(logged, func(id, _ string) bool { return !seen[id] })
	slices.SortFunc(specs, func(a, b state.BackendSpec) int { return strings.Compare(a.URL, b.URL) })
	return specs, nil
}

func name(c container) string {
	if len(c.Names) > 0 {
		return strings.TrimPrefix(c.Names[0], "/")
	}
	return c.ID
}

// labelSpec turns a container's labels into a backend.
// ponytail: the container IP is taken from its first network by name; set
// pharos.url when Pharos shares another network with it or runs on the host.
func labelSpec(c container) (state.BackendSpec, error) {
	l := c.Labels
	spec := state.BackendSpec{URL: strings.TrimRight(l["pharos.url"], "/"), Kind: engine.Kind(l["pharos.kind"]), Logs: c.ID}
	var errs []error
	if spec.URL == "" {
		port := l["pharos.port"]
		if port == "" {
			var tcp []int
			for _, p := range c.Ports {
				if p.Type == "tcp" && !slices.Contains(tcp, p.PrivatePort) {
					tcp = append(tcp, p.PrivatePort)
				}
			}
			if len(tcp) == 1 {
				port = strconv.Itoa(tcp[0])
			} else {
				errs = append(errs, fmt.Errorf("%d exposed TCP ports: set pharos.port", len(tcp)))
			}
		} else if n, err := strconv.Atoi(port); err != nil || n <= 0 || n > 65535 {
			errs = append(errs, fmt.Errorf("pharos.port %q is not a port", port))
		}
		var ip string
		for _, net := range slices.Sorted(maps.Keys(c.NetworkSettings.Networks)) {
			if ip = c.NetworkSettings.Networks[net].IPAddress; ip != "" {
				break
			}
		}
		if ip == "" {
			errs = append(errs, errors.New("no network address: set pharos.url"))
		}
		spec.URL = "http://" + ip + ":" + port
	} else if u, err := url.Parse(spec.URL); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		errs = append(errs, fmt.Errorf("pharos.url %q: want http(s)://host[:port]", spec.URL))
	}
	if spec.Kind == "" {
		spec.Kind = engine.Auto
	}
	if _, ok := engine.Recipes[spec.Kind]; !ok && spec.Kind != engine.Auto {
		errs = append(errs, fmt.Errorf("pharos.kind %q: unknown kind", spec.Kind))
	}
	if v, ok := l["pharos.memory_gb"]; ok {
		gb, err := strconv.ParseFloat(v, 64)
		if err != nil || gb < 0 {
			errs = append(errs, fmt.Errorf("pharos.memory_gb %q is not a size", v))
		}
		spec.MemoryBytes = int64(gb * (1 << 30))
	}
	if v, ok := l["pharos.capacity"]; ok {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			errs = append(errs, fmt.Errorf("pharos.capacity %q is not a slot count", v))
		}
		spec.Capacity = n
	}
	return spec, errors.Join(errs...)
}
