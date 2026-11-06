// cmd/torture/oracle.go — Independent oracle: records every ACKed operation,
// and verify() checks that the database matches the oracle.

package main

import (
	"fmt"
	"sync"
)

// Oracle is a thread-safe in-memory model of all acknowledged operations.
// It is independent from the database; it records only what was ACKed.
type Oracle struct {
	mu      sync.Mutex
	state   map[string]string // key -> value (deletes remove the key)
	acked   []oracleEntry
}

// NewOracle creates an empty oracle.
func NewOracle() *Oracle {
	return &Oracle{state: make(map[string]string)}
}

// Apply records an ACKed operation into the oracle.
func (o *Oracle) Apply(entry oracleEntry) {
	o.mu.Lock()
	defer o.mu.Unlock()
	switch entry.opType {
	case "put":
		o.state[entry.key] = entry.value
	case "del":
		delete(o.state, entry.key)
	}
	o.acked = append(o.acked, entry)
}

// ApplyBatch atomically applies a batch (all or nothing — batches that were
// ACKed as a unit must all be present).
func (o *Oracle) ApplyBatch(entries []oracleEntry) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, e := range entries {
		switch e.opType {
		case "put":
			o.state[e.key] = e.value
		case "del":
			delete(o.state, e.key)
		}
		o.acked = append(o.acked, e)
	}
}

// Verify checks the given db dump (key->value map) against the oracle state.
// Returns nil if correct, or a descriptive error on first mismatch.
func (o *Oracle) Verify(dbDump map[string]string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Every key in oracle.state must be in dbDump with the right value.
	for k, expected := range o.state {
		actual, ok := dbDump[k]
		if !ok {
			return fmt.Errorf("oracle key %q (value=%q) missing from database", k, expected)
		}
		if actual != expected {
			return fmt.Errorf("oracle key %q: expected value=%q, got %q", k, expected, actual)
		}
	}

	// Every key in dbDump that is NOT in oracle.state is an unknown value —
	// unless it was written but not yet ACKed (which is allowed).
	// We cannot distinguish "in-flight" from "corrupted" in a simple oracle,
	// so we only report truly alien keys (not in acked list at all).
	ackedKeys := make(map[string]bool)
	for _, e := range o.acked {
		ackedKeys[e.key] = true
	}
	for k := range dbDump {
		if _, expectedInOracle := o.state[k]; expectedInOracle {
			continue // already checked above
		}
		if ackedKeys[k] {
			continue // key was ACKed (maybe deleted) — could be an in-flight put before delete
		}
		return fmt.Errorf("database key %q is not known to the oracle (possible corruption)", k)
	}

	return nil
}

// State returns a copy of the current oracle state.
func (o *Oracle) State() map[string]string {
	o.mu.Lock()
	defer o.mu.Unlock()
	copy := make(map[string]string, len(o.state))
	for k, v := range o.state {
		copy[k] = v
	}
	return copy
}

