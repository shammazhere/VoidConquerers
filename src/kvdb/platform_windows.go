//go:build windows

package kvdb

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

var (
	modkernel32      = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = modkernel32.NewProc("LockFileEx")
	procUnlockFileEx = modkernel32.NewProc("UnlockFileEx")
)

const (
	lockfileFailImmediately = 0x1
	lockfileExclusiveLock   = 0x2
)

// lockFile takes an exclusive, non-blocking LockFileEx lock on one byte of
// the lock file. Windows releases it when the process terminates
// (TerminateProcess included), possibly after a short delay, which is why
// Open retries for up to a second.
func lockFile(f *os.File) error {
	var ol syscall.Overlapped
	r1, _, e1 := procLockFileEx.Call(f.Fd(),
		uintptr(lockfileExclusiveLock|lockfileFailImmediately), 0, 1, 0,
		uintptr(unsafe.Pointer(&ol)))
	if r1 == 0 {
		return e1
	}
	return nil
}

func unlockFile(f *os.File) error {
	var ol syscall.Overlapped
	r1, _, e1 := procUnlockFileEx.Call(f.Fd(), 0, 1, 0, uintptr(unsafe.Pointer(&ol)))
	if r1 == 0 {
		return e1
	}
	return nil
}

// syncDir is a no-op: Windows has no directory fsync. A process kill cannot
// undo a completed MoveFileEx; only power loss could (see BREAKING.md).
func syncDir(dir string) error { return nil }

// installCompacted is compaction steps 6 and 7 on Windows.
// MoveFileEx cannot replace a file that has open handles, so we must close
// both handles first, rename, and reopen db.kv. A crash between the closes
// and the rename leaves db.kv (old, complete) and db.tmp (new, complete,
// fsynced); the next open() keeps db.kv and deletes db.tmp. If the reopen
// fails the engine is poisoned and refuses further work.
func (db *DB) installCompacted(tmp *os.File) error {
	tmp.Close()
	db.f.Close()
	db.f = nil
	if err := os.Rename(db.tmpPath, db.path); err != nil {
		os.Remove(db.tmpPath)
		db.trace("compact-aborted")
		f, err2 := os.OpenFile(db.path, os.O_RDWR, 0)
		if err2 != nil {
			db.poison()
			return fmt.Errorf("%w: rename failed (%v) and reopen failed (%v)", ErrIO, err, err2)
		}
		db.f = f
		return fmt.Errorf("%w: rename: %v", ErrIO, err)
	}
	f, err := os.OpenFile(db.path, os.O_RDWR, 0)
	if err != nil {
		db.poison()
		return fmt.Errorf("%w: reopen after rename: %v", ErrIO, err)
	}
	db.f = f
	return nil
}
