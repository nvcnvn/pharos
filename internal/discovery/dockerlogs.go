// Package discovery finds backends and opens their log feeds (ARCHITECTURE §10).
// So far: the Docker log feed.
package discovery

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// DockerLogs opens the log of a container through the Docker Engine API on
// the local socket, starting at the container's current start, so lines an
// engine prints once at startup count. With follow it stays open for new
// lines. The caller closes it.
func DockerLogs(ctx context.Context, container string, follow bool) (io.ReadCloser, error) {
	c := dockerClient()
	var info struct {
		Config struct{ Tty bool }
		State  struct{ StartedAt time.Time }
	}
	if err := dockerGet(ctx, c, "/containers/"+url.PathEscape(container)+"/json", &info); err != nil {
		return nil, err
	}
	q := url.Values{"stdout": {"1"}, "stderr": {"1"}}
	if t := info.State.StartedAt; !t.IsZero() {
		q.Set("since", fmt.Sprintf("%d.%09d", t.Unix(), t.Nanosecond()))
	}
	if follow {
		q.Set("follow", "1")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker/containers/"+url.PathEscape(container)+"/logs?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("docker logs %s: %w", container, err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("docker logs %s: status %d", container, resp.StatusCode)
	}
	if info.Config.Tty {
		return resp.Body, nil // a TTY stream is raw
	}
	return demuxed{Reader: &demux{r: bufio.NewReader(resp.Body)}, Closer: resp.Body}, nil
}

// dockerClient talks to the socket in DOCKER_HOST (unix:// only), or /var/run/docker.sock.
func dockerClient() *http.Client {
	sock := "/var/run/docker.sock"
	if h, ok := strings.CutPrefix(os.Getenv("DOCKER_HOST"), "unix://"); ok {
		sock = h
	}
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}}
}

func dockerGet(ctx context.Context, c *http.Client, path string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker"+path, nil)
	if err != nil {
		return err
	}
	resp, err := c.Do(req)
	if err != nil {
		return fmt.Errorf("docker %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("docker %s: status %d", path, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}

type demuxed struct {
	io.Reader
	io.Closer
}

// demux strips the 8-byte frame headers Docker puts on a non-TTY log stream
// ([stream, 0, 0, 0, size uint32 big-endian] then size bytes), joining stdout
// and stderr.
// ponytail: frames are joined in arrival order; if an engine writes partial
// lines to both streams at once they can interleave mid-line.
type demux struct {
	r    *bufio.Reader
	left uint32
}

func (d *demux) Read(p []byte) (int, error) {
	for d.left == 0 {
		var h [8]byte
		if _, err := io.ReadFull(d.r, h[:]); err != nil {
			if err == io.ErrUnexpectedEOF {
				err = fmt.Errorf("docker log: truncated frame header")
			}
			return 0, err
		}
		d.left = binary.BigEndian.Uint32(h[4:])
	}
	if uint32(len(p)) > d.left {
		p = p[:d.left]
	}
	n, err := d.r.Read(p)
	d.left -= uint32(n)
	if err == io.EOF && d.left > 0 {
		err = fmt.Errorf("docker log: truncated frame")
	}
	return n, err
}
