// Command pharos is the LLM router. So far it has one subcommand, doctor.
package main

import (
	"fmt"
	"os"
)

const usage = `usage: pharos doctor [flags]   (pharos doctor -h for flags)`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
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
