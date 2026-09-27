package discovery

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
)

// testdata/ollama-0.34.4-logs.raw is GET /containers/{id}/logs?stdout=1&stderr=1
// on an ollama/ollama:0.34.4 container without a TTY (Docker 29.1.3), saved raw.
func TestDemuxRecordedStream(t *testing.T) {
	raw, err := os.ReadFile("testdata/ollama-0.34.4-logs.raw")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(&demux{r: bufio.NewReader(bytes.NewReader(raw))})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.ContainsAny(got, "\x00\x01\x02") {
		t.Error("frame header bytes left in the text")
	}
	var lines []string
	for sc := bufio.NewScanner(bytes.NewReader(got)); sc.Scan(); {
		lines = append(lines, sc.Text())
	}
	if lines[0] != "Couldn't find '/root/.ollama/id_ed25519'. Generating new private key." {
		t.Errorf("first line = %q", lines[0])
	}
	found := false
	for _, l := range lines {
		found = found || strings.HasPrefix(l, "time=") && strings.Contains(l, `msg="server config"`) && strings.Contains(l, "OLLAMA_NUM_PARALLEL:2 ")
	}
	if !found {
		t.Error(`no whole "server config" line`)
	}
}

func TestDemuxTruncatedStreamIsAnError(t *testing.T) {
	raw, err := os.ReadFile("testdata/ollama-0.34.4-logs.raw")
	if err != nil {
		t.Fatal(err)
	}
	for _, cut := range []int{4, 20} { // inside the first header; inside the first frame
		if _, err := io.ReadAll(&demux{r: bufio.NewReader(bytes.NewReader(raw[:cut]))}); err == nil {
			t.Errorf("cut at %d: err = nil", cut)
		}
	}
}
