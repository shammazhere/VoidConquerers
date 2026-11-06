//go:build sabotage

package kvdb

import "os"

// Planted-bug build (go build -tags sabotage). KVDB_SABOTAGE selects one bug:
//
//	ack-before-fsync   ACK is sent before anything is written (breaks A5/durability)
//	no-truncate        open() does not cut the torn tail and appends at the physical end,
//	                   i.e. behind the garbage (the classic torn-tail bug)
//	no-marker          recovery applies data records without waiting for the commit marker
//	compact-old-fd     after compaction's rename, writes keep going to the old, unlinked file
//
// The torture tool must catch every one of them. This proves the tool can
// fail; a tool that cannot fail proves nothing.
var sabotage = os.Getenv("KVDB_SABOTAGE")
