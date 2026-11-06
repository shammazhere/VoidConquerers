// Command kvdb is the command-line interface for the KIPI database.
package main

import (
	"fmt"
	"io"
	"os"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		usage(stderr)
		return 2
	}

	var err error
	switch args[0] {
	case "open":
		err = runOpen(args[1:], stdout)
	case "put":
		err = runPut(args[1:], stdout)
	case "get":
		err = runGet(args[1:], stdout)
	case "del":
		err = runDel(args[1:], stdout)
	case "batch":
		err = runBatch(args[1:], stdout)
	case "dump":
		err = runDump(args[1:], stdout)
	case "check":
		err = runCheck(args[1:], stdout)
	case "serve":
		err = runServe(args[1:], stdin, stdout)
	default:
		err = fmt.Errorf("unknown command %q", args[0])
	}
	if err != nil {
		fmt.Fprintf(stderr, "ERROR: %v\n", err)
		return 1
	}
	return 0
}

func usage(w io.Writer) {
	fmt.Fprintln(w, `Usage:
  kvdb open  <path> [--max-bytes N]
  kvdb put   <path> <key> <value>
  kvdb get   <path> <key>
  kvdb del   <path> <key>
  kvdb batch <path> <file>
  kvdb dump  <path>
  kvdb check <path>
  kvdb serve <path> [--max-bytes N] [--step-delay-us N]`)
}
