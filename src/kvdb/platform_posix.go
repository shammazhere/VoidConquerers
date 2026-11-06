//go:build !windows

package kvdb

import (
	"fmt"
	"os"
	"syscall"
)

// lockFile takes an exclusive, non-blocking flock on the lock file. The
// kernel releases it when the process dies (including kill -9), so a crash
// never leaves a stale lock.
func lockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func unlockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}

// syncDir fsyncs a directory so a create or rename inside it is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// installCompacted is compaction steps 6 and 7 on POSIX.
// rename(2) atomically replaces db.kv with db.tmp. The open descriptor of
// db.tmp now refers to the file named db.kv, so it simply becomes the
// database's descriptor: the main file is never reopened.
func (db *DB) installCompacted(tmp *os.File) error {
	if err := os.Rename(db.tmpPath, db.path); err != nil {
		tmp.Close()
		os.Remove(db.tmpPath)
		db.trace("compact-aborted")
		return fmt.Errorf("%w: rename: %v", ErrIO, err)
	}
	if sabotage == "compact-old-fd" { // planted bug, dead code in normal builds
		tmp.Close()
		return nil
	}
	old := db.f
	db.f = tmp
	old.Close() // its inode is now unlinked; nothing writes to it again
	return nil
}
