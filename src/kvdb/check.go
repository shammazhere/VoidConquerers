package kvdb

import (
	"fmt"
	"os"
)

// CheckReport is the result of Check.
type CheckReport struct {
	OK             bool
	Reason         string // why the file is not OK
	Keys           int
	FileBytes      int64
	CommittedBytes int64  // end of the last valid commit marker
	TailBytes      int64  // bytes after it: an uncommitted or torn tail the next open() will cut
	TailReason     string // why the scan stopped there
	MarkerFix      int    // the last commit marker is missing this many constant bytes; open() restores them
	Records        int    // valid data records in the file (committed or not)
	Commits        int    // commit markers
	Applied        int    // data records committed by a marker
	Discarded      int    // complete records with no marker after them
	TmpExists      bool   // a leftover db.tmp the next open() will delete
}

// Check verifies a database file without changing it (no lock, no repair).
//
// It runs the same scan as recovery and then re-reads every record the
// index points at, verifying CRC, flags and key. A file is OK if it is a
// state this database can produce: a valid header, a chain of valid
// committed records, and optionally an uncommitted/torn tail (which is what
// a kill leaves behind and what the next open() truncates).
func Check(path string) CheckReport {
	var rep CheckReport
	f, err := os.Open(path)
	if err != nil {
		rep.Reason = fmt.Sprintf("cannot open: %v", err)
		return rep
	}
	defer f.Close()

	index := make(map[string]entry)
	res, err := scan(f, func(del bool, key string, e entry) {
		if del {
			delete(index, key)
		} else {
			index[key] = e
		}
	})
	if err != nil {
		rep.Reason = err.Error()
		return rep
	}
	rep.FileBytes = res.size
	rep.CommittedBytes = res.lastSafe
	rep.TailBytes = res.size - res.lastSafe
	rep.Records = res.records
	rep.Commits = res.commits
	rep.Applied = res.applied
	rep.Discarded = res.discarded
	rep.MarkerFix = len(res.markerFix)
	if rep.TailBytes > 0 {
		rep.TailReason = res.stopReason
	}

	var rec []byte
	for k, e := range index {
		if e.off < HeaderSize || e.off+int64(e.recSize) > res.lastSafe {
			rep.Reason = fmt.Sprintf("index entry for key %q points outside the committed region", k)
			return rep
		}
		if cap(rec) < int(e.recSize) {
			rec = make([]byte, e.recSize)
		}
		rec = rec[:e.recSize]
		if _, err := f.ReadAt(rec, e.off); err != nil {
			rep.Reason = fmt.Sprintf("read record at %d: %v", e.off, err)
			return rep
		}
		if _, err := verifyPut(rec, k); err != nil {
			rep.Reason = fmt.Sprintf("record for key %q at offset %d failed verification", k, e.off)
			return rep
		}
	}
	rep.Keys = len(index)
	if _, err := os.Stat(path + ".tmp"); err == nil {
		rep.TmpExists = true
	}
	rep.OK = true
	return rep
}
