package main

import (
	"slices"
	"testing"

	"github.com/nvcnvn/pharos/internal/config"
)

// Keys and static backends apply live on a reload; the other fields are
// reported as needing a restart.
func TestRestartNeeded(t *testing.T) {
	parse := func(yaml string) config.Config {
		c, err := config.Parse([]byte(yaml))
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	a := parse("listen: :8080\nbackends: [{url: 'http://a:1'}]")
	live := parse("listen: :8080\nbackends: [{url: 'http://b:1'}]\nkeys: [{name: k, sha256: 9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08}]")
	if got := restartNeeded(a, live); len(got) != 0 {
		t.Errorf("keys and backends: %v", got)
	}
	restart := parse("listen: :9090\nusage: {timezone: Asia/Tokyo}\ndrain: {grace: 1s}\npeers: {listen: ':8081', secret_file: s, members: [a:8081]}")
	if got := restartNeeded(a, restart); !slices.Equal(got, []string{"listen", "peers", "usage", "drain"}) {
		t.Errorf("got %v", got)
	}
}
