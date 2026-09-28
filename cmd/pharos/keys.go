package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/nvcnvn/pharos/internal/config"
)

// keys new prints a new API key once, with the config entry that holds its
// SHA-256. The key itself is never stored.
func keys(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] != "new" {
		return errors.New("usage: pharos keys new -name NAME")
	}
	fs := flag.NewFlagSet("keys new", flag.ContinueOnError)
	fs.SetOutput(stderr)
	name := fs.String("name", "", "the key's name; usage is recorded by it")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *name == "" {
		return errors.New("keys new: -name is required")
	}
	b := make([]byte, 32)
	rand.Read(b)
	key := "pk-" + base64.RawURLEncoding.EncodeToString(b)
	sum := config.HashKey(key)
	fmt.Fprintf(stdout, "key (shown once; give it to %s): %s\n\nadd to keys: in the config:\n  - name: %s\n    sha256: %s\n", *name, key, *name, hex.EncodeToString(sum[:]))
	return nil
}
