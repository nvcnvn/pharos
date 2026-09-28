// Command pharos is the LLM router.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

const help = `usage: pharos <command> [flags]

  serve    route requests to the configured backends (pharos serve -h)
  doctor   check backends and show which probes answer (pharos doctor -h)
  keys     keys new -name NAME: make an API key and print its config entry`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, help)
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
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
	if err != nil {
		fmt.Fprintln(os.Stderr, "pharos:", err)
		os.Exit(1)
	}
}
