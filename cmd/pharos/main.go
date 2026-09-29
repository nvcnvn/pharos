// Command pharos is the LLM router.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
)

const help = `usage: pharos <command> [flags]

  serve    route requests to the configured backends (pharos serve -h)
  doctor   check backends and show which probes answer (pharos doctor -h)
  keys     keys new -name NAME: make an API key and print its config entry
  version  print the version`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, help)
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "-h", "-help", "--help", "help":
		fmt.Println(help)
		return
	case "version", "-version", "--version":
		fmt.Println("pharos", version())
		return
	case "serve":
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		context.AfterFunc(ctx, stop) // a second signal ends the process at once
		err = serve(ctx, os.Args[2:], os.Stderr)
	case "doctor":
		err = doctor(os.Args[2:], os.Stdout, os.Stderr)
	case "keys":
		err = keys(os.Args[2:], os.Stdout, os.Stderr)
	default:
		fmt.Fprintln(os.Stderr, help)
		os.Exit(2)
	}
	if errors.Is(err, flag.ErrHelp) {
		return // the flag set printed the usage
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "pharos:", err)
		os.Exit(1)
	}
}

// version is the module version (a go install of a tag) or the commit it was
// built from.
func version() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	v, rev, dirty := info.Main.Version, "", ""
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value[:min(len(s.Value), 12)]
		case "vcs.modified":
			if s.Value == "true" {
				dirty = "+dirty"
			}
		}
	}
	if rev != "" && (v == "" || v == "(devel)") {
		return "commit " + rev + dirty
	}
	return v
}
