// Command pharos is the LLM router.
package main

import (
	"fmt"
	"os"
)

const usage = `usage: pharos <command> [flags]

  serve    route requests to the configured backends (pharos serve -h)
  doctor   check backends and show which probes answer (pharos doctor -h)`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(os.Args[2:], os.Stderr)
	case "doctor":
		err = doctor(os.Args[2:], os.Stdout, os.Stderr)
	default:
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "pharos:", err)
		os.Exit(1)
	}
}
