// cli/torture/oracle.go — Independent correctness oracle.
//
// The Oracle records every operation that received an ACK from the serve process.
// After each kill/restart cycle, Verify() compares the oracle's expected state
// against the actual database contents to detect durability or atomicity violations.

package main

import (
	"fmt"
	"sync"
)

// Oracle is a thread-safe in-memory model of all acknowledged operations.
// It is intentionally independent from the database implementation.
type Oracle struct {
	mu    sync.Mutex
	state map[string]string // current expected key → value (deletes remove the entry)
	acked []oracleEntry     // append-only history of every ACKed operation
}

// NewOracle creates an empty oracle.
func NewOracle() *Oracle {
	return &Oracle{state: make(map[string]string)}
}

// Apply records a single ACKed operation into the oracle.
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

// ApplyBatch atomically records a batch of ACKed operations.
// All entries in the batch were committed as a unit, so all must be reflected.
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

// Verify checks the given db dump (key → value map) against the oracle state.
// Returns nil if correct, or a descriptive error on the first detected mismatch.
//
// Invariants checked:
//   - Every key in oracle.state must be in dbDump with the matching value.
//   - No key in dbDump may be unknown to the oracle (not in the acked history).
func (o *Oracle) Verify(dbDump map[string]string) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Every oracle-expected key must be present with the right value.
	for k, expected := range o.state {
		actual, ok := dbDump[k]
		if !ok {
			return fmt.Errorf("oracle key %q (value=%q) missing from database", k, expected)
		}
		if actual != expected {
			return fmt.Errorf("oracle key %q: expected value=%q, got %q", k, expected, actual)
		}
	}

	// No key in dbDump may be unknown to the oracle.
	// Keys that were ACKed (even as a later delete) are allowed in the dump
	// as "in-flight" values from operations that were sent but not yet ACKed.
	ackedKeys := make(map[string]bool, len(o.acked))
	for _, e := range o.acked {
		ackedKeys[e.key] = true
	}
	for k := range dbDump {
		if _, expectedInOracle := o.state[k]; expectedInOracle {
			continue // already validated above
		}
		if ackedKeys[k] {
			continue // key was seen before (may have been deleted); in-flight puts are allowed
		}
		return fmt.Errorf("database key %q is not known to the oracle (possible corruption)", k)
	}

	return nil
}

// State returns a snapshot copy of the current oracle state.
func (o *Oracle) State() map[string]string {
	o.mu.Lock()
	defer o.mu.Unlock()
	snap := make(map[string]string, len(o.state))
	for k, v := range o.state {
		snap[k] = v
	}
	return snap
}

