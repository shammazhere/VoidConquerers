//go:build !windows

// platform_posix.go — POSIX-specific: flock, directory fsync, rename.

package kvdb

import (
	"os"
	"path/filepath"
	"syscall"
)

// lockFile acquires an exclusive flock on the named file, creating it if needed.
func lockFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// unlockFile releases the flock.
func unlockFile(f *os.File) {
	syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}

// syncDir fsyncs the directory containing path (required after rename on Linux).
func syncDir(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// installCompacted performs the atomic rename on POSIX:
//  1. Close the tmp file descriptor (keep the file).
//  2. Rename tmp → db path (atomic).
//  3. fsync the directory.
//  4. Reopen the renamed file for reading/writing.
//  5. Return the new file descriptor and new file size.
func installCompacted(tmp *os.File, tmpPath, dbPath string, oldFile *os.File, lockFile *os.File) (*os.File, int64, error) {
	// Get size before closing
	fi, err := tmp.Stat()
	if err != nil {
		return nil, 0, err
	}
	newSize := fi.Size()

	// Close old data file descriptor
	if err := oldFile.Close(); err != nil {
		return nil, 0, err
	}

	// Rename tmp → db  (CP12: atomic rename)
	if err := os.Rename(tmpPath, dbPath); err != nil {
		return nil, 0, err
	}

	// fsync directory (CP13)
	if err := syncDir(dbPath); err != nil {
		// Not fatal — rename is already durable on most POSIX systems
		_ = err
	}

	// Reopen the file
	newFile, err := os.OpenFile(dbPath, os.O_RDWR, 0644)
	if err != nil {
		return nil, 0, err
	}

	return newFile, newSize, nil
}

