// cli/kvdb/main.go — Entry point for the kvdb CLI pipeline.
//
// Pipeline: kvdb
//   Routes top-level sub-commands to their handlers.
//
// Commands:
//   kvdb open  <path> [--max-bytes N]
//   kvdb put   <path> <key> <value>
//   kvdb get   <path> <key>
//   kvdb del   <path> <key>
//   kvdb batch <path> <batchfile>
//   kvdb dump  <path>
//   kvdb check <path>
//   kvdb serve <path> [--max-bytes N] [--step-delay-us N] [--compact-min-bytes N]

package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "open":
		runOpen(args)
	case "put":
		runPut(args)
	case "get":
		runGet(args)
	case "del":
		runDel(args)
	case "batch":
		runBatch(args)
	case "dump":
		runDump(args)
	case "check":
		runCheck(args)
	case "serve":
		runServe(args)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `Usage:
  kvdb open  <path> [--max-bytes N]
  kvdb put   <path> <key> <value>
  kvdb get   <path> <key>
  kvdb del   <path> <key>
  kvdb batch <path> <batchfile>
  kvdb dump  <path>
  kvdb check <path>
  kvdb serve <path> [--max-bytes N] [--compact-min-bytes N] [--step-delay-us N]`)
}

