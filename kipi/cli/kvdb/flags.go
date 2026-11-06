// cli/kvdb/flags.go — Flag parsing helpers shared across all kvdb sub-commands.

package main

import (
	"strconv"
	"strings"
)

// parseFlags extracts --key value pairs from args, returning remaining positional args.
func parseFlags(args []string) (map[string]string, []string) {
	flags := make(map[string]string)
	var pos []string
	for i := 0; i < len(args); i++ {
		if strings.HasPrefix(args[i], "--") {
			key := strings.TrimPrefix(args[i], "--")
			if i+1 < len(args) {
				flags[key] = args[i+1]
				i++
			}
		} else {
			pos = append(pos, args[i])
		}
	}
	return flags, pos
}

// parseInt64Flag returns the int64 value of a named flag, or def if absent/invalid.
func parseInt64Flag(flags map[string]string, key string, def int64) int64 {
	if s, ok := flags[key]; ok {
		v, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			return v
		}
	}
	return def
}

