//go:build !sabotage

package kvdb

// sabotage is always "" in a normal build, so every `if sabotage == ...`
// branch is dead code that the compiler removes. Planted bugs exist only in
// a binary built with `-tags sabotage` (see sabotage_on.go and README).
const sabotage = ""
