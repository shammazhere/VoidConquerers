package main

import (
	"fmt"
	"strconv"
	"strings"
)

func parsePositionals(args []string) ([]string, error) {
	_, pos, err := parseArgs(args)
	return pos, err
}

func parseArgs(args []string, allowedFlags ...string) (map[string]string, []string, error) {
	allowed := make(map[string]bool, len(allowedFlags))
	for _, name := range allowedFlags {
		allowed[name] = true
	}

	flags := make(map[string]string, len(allowed))
	pos := make([]string, 0, len(args))
	flagMode := true
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if flagMode && arg == "--" {
			flagMode = false
			continue
		}
		if !flagMode || !strings.HasPrefix(arg, "--") {
			pos = append(pos, arg)
			continue
		}
		name := strings.TrimPrefix(arg, "--")
		if !allowed[name] {
			return nil, nil, fmt.Errorf("unknown option %q", arg)
		}
		if _, exists := flags[name]; exists {
			return nil, nil, fmt.Errorf("option %q specified more than once", arg)
		}
		if i+1 >= len(args) || args[i+1] == "--" {
			return nil, nil, fmt.Errorf("option %q requires a value", arg)
		}
		flags[name] = args[i+1]
		i++
	}
	return flags, pos, nil
}

func flagInt64(flags map[string]string, name string) (int64, error) {
	value, present := flags[name]
	if !present {
		return 0, nil
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("option --%s requires a non-negative integer, got %q", name, value)
	}
	return n, nil
}
