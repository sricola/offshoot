package capture

import (
	"encoding/json"
	"os"
)

// State is the resume checkpoint persisted to disk after every state
// transition the engine cares about across a restart.
//
// Two shapes matter:
//
//   - In-progress capture: Off/Salt1/Salt2 describe the reader's position in
//     the live WAL generation (see drain(), which saves this after every
//     applied transaction). Clean is false. This is NOT a valid resume point
//     on its own — a WAL offset only means something in the context of a
//     specific reader with live running-checksum state, which does not
//     survive a process restart, so tryResume never trusts it. It exists so
//     an operator/debugger can see how far capture had gotten.
//
//   - Clean shutdown: Clean is true; Off/Salt1/Salt2 are left zero and
//     unused. This is produced ONLY by shutdown()'s successful
//     RESTART-then-TRUNCATE checkpoint sequence, which physically zeroes the
//     -wal file — it means "every captured-so-far frame is folded into the
//     main DB and the WAL is empty; nothing is pending". MainMTimeNS and
//     MainSize fingerprint the MAIN db file (not the WAL) at that same
//     instant: SQLite mtime (nanoseconds) and byte size, read right after
//     the TRUNCATE that folded everything in. tryResume treats Clean==true
//     as necessary but NOT sufficient to skip a rebase — it additionally
//     requires BOTH the on-disk -wal file being still physically empty AND
//     the main file's current mtime/size still matching this fingerprint
//     (see tryResume's doc comment for why both checks are independently
//     needed: an empty WAL alone cannot distinguish "nothing happened" from
//     "something happened and a third party — including SQLite's own
//     last-connection-close auto-checkpoint — folded it into the main file
//     and erased the WAL evidence in the process").
//
// rebase() saves State{} (Clean=false, Off=0), so an unqualified zero value
// continues to mean "no usable resume point" exactly as before this field
// was added.
type State struct {
	Off   int64  `json:"off"`
	Salt1 uint32 `json:"salt1"`
	Salt2 uint32 `json:"salt2"`
	Clean bool   `json:"clean"`

	// MainMTimeNS and MainSize are the main DB file's mtime (UnixNano) and
	// size at the moment of a verified-clean shutdown. Meaningful only when
	// Clean is true. See the type doc comment and tryResume's doc comment.
	MainMTimeNS int64 `json:"main_mtime_ns"`
	MainSize    int64 `json:"main_size"`
}

func LoadState(path string) (State, bool, error) {
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return State{}, false, nil
	}
	if err != nil {
		return State{}, false, err
	}
	var s State
	if err := json.Unmarshal(b, &s); err != nil {
		return State{}, false, nil // corrupt state ⇒ treat as absent ⇒ rebase
	}
	return s, true, nil
}

// SaveState writes state durably: write the temp file's bytes, fsync them to
// disk, close, THEN rename over the real path. Without the fsync, a crash
// between write and rename can leave the rename pointing at a temp file
// whose bytes never made it past the page cache — on some filesystems/OSes
// that surfaces as a truncated or stale file after a hard crash, exactly the
// kind of gap that would make tryResume's Clean marker unreliable.
func SaveState(path string, s State) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
