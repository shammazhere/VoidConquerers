//go:build windows

// platform_windows.go — Windows-specific: LockFileEx, close-rename-reopen.
// On Windows, rename requires closing open file handles first.
// Directory fsync is not needed because NTFS journal makes renames durable.

package kvdb

import (
	"os"
	"syscall"
	"unsafe"
)

var (
	modkernel32   = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx = modkernel32.NewProc("LockFileEx")
	procUnlockFileEx = modkernel32.NewProc("UnlockFileEx")
)

const (
	lockfileExclusiveLock   = 0x00000002
	lockfileFailImmediately = 0x00000001
)

// lockFile acquires an exclusive LockFileEx on the named file.
func lockFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	ol := new(syscall.Overlapped)
	r1, _, err := procLockFileEx.Call(
		uintptr(f.Fd()),
		uintptr(lockfileExclusiveLock|lockfileFailImmediately),
		0,
		1, 0,
		uintptr(unsafe.Pointer(ol)),
	)
	if r1 == 0 {
		f.Close()
		return nil, err
	}
	return f, nil
}

// unlockFile releases the LockFileEx.
func unlockFile(f *os.File) {
	ol := new(syscall.Overlapped)
	procUnlockFileEx.Call(
		uintptr(f.Fd()),
		0,
		1, 0,
		uintptr(unsafe.Pointer(ol)),
	)
}

// syncDir is a no-op on Windows (NTFS journal guarantees rename durability).
func syncDir(path string) error {
	return nil
}

// installCompacted performs the Windows-safe rename:
//  1. Close both old data file and tmp file (Windows needs handles closed to rename).
//  2. Rename tmp → db path.
//  3. Reopen the renamed file for reading/writing.
//  4. Return the new file descriptor and new file size.
func installCompacted(tmp *os.File, tmpPath, dbPath string, oldFile *os.File, lockFileFd *os.File) (*os.File, int64, error) {
	// Get size before closing
	fi, err := tmp.Stat()
	if err != nil {
		return nil, 0, err
	}
	newSize := fi.Size()

	// Close both handles
	tmp.Close()
	oldFile.Close()

	// Rename tmp → db  (CP12)
	if err := os.Rename(tmpPath, dbPath); err != nil {
		return nil, 0, err
	}

	// Reopen the renamed file
	newFile, err := os.OpenFile(dbPath, os.O_RDWR, 0644)
	if err != nil {
		return nil, 0, err
	}

	return newFile, newSize, nil
}

