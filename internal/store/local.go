package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/sricola/offshoot/internal/fsutil"
	"github.com/sricola/offshoot/internal/reflink"
)

// Local is a directory-backed Backend. CAS is implemented with a per-key
// flock(2) on a lock file: acquire lock -> read+verify etag -> write temp,
// fsync, rename -> release lock. A bare rename alone is atomic REPLACE, not
// compare-and-swap; the lock provides the compare step.
type Local struct {
	root string
}

func NewLocal(root string) (*Local, error) {
	// 0700 throughout: objects are 0600 already, and a 0755 directory let
	// any other local user list database and branch names. MkdirAll never
	// tightens an existing directory, so a store created by an earlier
	// version keeps its mode until chmod'ed by hand (see SECURITY.md).
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	return &Local{root: root}, nil
}

func etagOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (l *Local) path(key string) (string, error) {
	if key == "" || strings.Contains(key, "..") || strings.HasPrefix(key, "/") {
		return "", fmt.Errorf("store: invalid key %q", key)
	}
	return filepath.Join(l.root, filepath.FromSlash(key)), nil
}

func (l *Local) Get(key string) ([]byte, string, error) {
	p, err := l.path(key)
	if err != nil {
		return nil, "", err
	}
	data, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		return nil, "", ErrNotFound
	}
	if err != nil {
		return nil, "", err
	}
	return data, etagOf(data), nil
}

// Head implements store.Header. A local etag is a sha256 over the content
// (etagOf), which every write records on the object as an extended
// attribute (setEtagAttr) so Head can answer from one open+fstat+fgetxattr
// instead of re-reading the object: ops.verifyOwnObject calls Head after
// every winning checkpoint CAS, and without the attribute each at-rest
// checkpoint on a local store paid a second full read and hash of the
// snapshot it had just written. When the attribute is missing (a
// filesystem without user xattrs, or an object written by an older
// version) or does not describe the file's current size, Head hashes the
// content, exactly as Get does. etag and size come from the same open
// descriptor, so they describe one inode even under a concurrent replace.
func (l *Local) Head(key string) (string, int64, error) {
	p, err := l.path(key)
	if err != nil {
		return "", 0, err
	}
	f, err := os.Open(p)
	if os.IsNotExist(err) {
		return "", 0, ErrNotFound
	}
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return "", 0, err
	}
	if etag, ok := readEtagAttr(f, fi.Size()); ok {
		return etag, fi.Size(), nil
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return "", 0, err
	}
	return etagOf(data), int64(len(data)), nil
}

// GetReader implements store.ReaderGetter: it opens the file directly
// rather than os.ReadFile-ing its full contents into memory (unlike Get),
// so a caller applying a large object (e.g. a snapshot/segment during chain
// materialization) holds only one open file descriptor, not the whole
// object's bytes. A missing file maps to store.ErrNotFound, same as Get.
// The caller MUST Close the returned file.
//
// etag is always "" here: computing the real content etag (etagOf, a
// sha256 over the full data) would require reading the whole file, which
// defeats the point of a streaming Get. A caller that needs the content
// etag should use Get instead.
func (l *Local) GetReader(key string) (io.ReadCloser, string, error) {
	p, err := l.path(key)
	if err != nil {
		return nil, "", err
	}
	f, err := os.Open(p)
	if os.IsNotExist(err) {
		return nil, "", ErrNotFound
	}
	if err != nil {
		return nil, "", err
	}
	return f, "", nil
}

// write does a write-to-temp-then-rename. The temp file gets a unique name
// per call (via os.CreateTemp) rather than a fixed p+".tmp": Put has no
// per-key lock (PutIf's lock guards its own read-then-write, but Put is used
// standalone wherever last-write-wins is intentional, e.g. session flush's
// overwrite of an orphan a crashed prior flush left, and GC's tombstone-list
// write), so two
// goroutines can legitimately call write() on the same p concurrently. A
// shared fixed temp name means one goroutine's os.Create (O_TRUNC) or
// os.Rename can clobber or disappear out from under the other mid-write,
// surfacing as a spurious "rename ... no such file or directory" — a real
// bug, not a benign race, since it aborts a write that should have quietly
// lost or won. A unique temp name per call makes each writer self-contained;
// the final os.Rename is still atomic, so the last one to rename wins
// cleanly with no torn or missing file in between.
//
// The returned etag (etagOf over data) is also recorded on the temp file
// as an extended attribute before the rename (see etagAttr), so the object
// lands with its etag attached and Head need not re-read it.
func (l *Local) write(p string, data []byte) (etag string, err error) {
	dir := filepath.Dir(p)
	f, err := createTempIn(dir, filepath.Base(p)+".tmp-*")
	if err != nil {
		return "", err
	}
	tmp := f.Name()
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", err
	}
	etag = etagOf(data)
	setEtagAttr(f, etag, int64(len(data)))
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return "", err
	}
	if err := os.Rename(tmp, p); err != nil {
		os.Remove(tmp)
		return "", err
	}
	syncDirAfterRename(dir)
	return etag, nil
}

// syncDirAfterRename runs the directory fsync that makes a landed rename
// survive power loss and reports a failure on stderr instead of returning
// it: the rename is already visible, and Local promises (WritesSettled)
// that an error from a write means nothing landed — callers such as
// ops.Destroy's claim handling and session flush decide what to re-read on
// that promise. A directory fsync that fails with anything but the
// "unsupported here" errnos is a failing disk; the next write will fail
// outright, and this line says why.
func syncDirAfterRename(dir string) {
	if err := fsutil.SyncDir(dir); err != nil {
		fmt.Fprintf(os.Stderr, "offshoot: store: directory sync after a landed write in %s failed: %v (the write is visible; its durability across power loss is not assured)\n", dir, err)
	}
}

func (l *Local) Put(key string, data []byte) error {
	p, err := l.path(key)
	if err != nil {
		return err
	}
	_, err = l.write(p, data)
	return err
}

// writeReader is write()'s streaming counterpart: it copies r (exactly size
// bytes) into a uniquely-named temp file in the same dir as p, fsyncs, and
// renames into place — same write-to-temp-then-rename discipline as write(),
// same unique-per-call temp name (see write()'s doc comment for why: no
// per-key lock here either, since PutReader mirrors Put's last-write-wins
// contract). The only difference is the data source: this never holds more
// than one io.Copy buffer's worth of the object in memory, where write()
// already has the whole []byte resident (its caller built it that way).
//
// The returned etag is a sha256 over the streamed content, computed
// incrementally via io.MultiWriter alongside the write to disk — the same
// digest etagOf(data) would produce over the same bytes read back, without
// a second full-file pass to compute it after the fact. Like write(), it
// records that etag on the temp file (etagAttr) before the rename.
func (l *Local) writeReader(p string, r io.Reader, size int64) (etag string, err error) {
	tmp, etag, err := l.writeReaderTemp(p, r, size)
	if err != nil {
		return "", err
	}
	return etag, commitTemp(tmp, p)
}

// writeReaderTemp is writeReader's streaming half: the whole object is
// streamed, hashed, stamped with its etag and fsynced into a uniquely named
// temp file beside p, and the temp path is returned for commitTemp. Split
// out so PutReaderIf can do the slow part (a multi-GB snapshot) BEFORE it
// takes the per-key lock, and hold the lock only around the compare and
// the rename, so other writers to the key wait milliseconds, not minutes.
func (l *Local) writeReaderTemp(p string, r io.Reader, size int64) (tmp, etag string, err error) {
	f, err := createTempIn(filepath.Dir(p), filepath.Base(p)+".tmp-*")
	if err != nil {
		return "", "", err
	}
	tmp = f.Name()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), r)
	if err != nil {
		f.Close()
		os.Remove(tmp)
		return "", "", err
	}
	if n != size {
		f.Close()
		os.Remove(tmp)
		return "", "", fmt.Errorf("store: streamed %d bytes for %s, want %d", n, p, size)
	}
	etag = hex.EncodeToString(h.Sum(nil))
	setEtagAttr(f, etag, n)
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return "", "", err
	}
	return tmp, etag, nil
}

// inDir makes sure dir exists and runs create in it. A delete in another
// process can empty dir and remove it (removeEmptyParents) between the
// MkdirAll and the create, which then fails with ENOENT. MkdirAll itself
// can fail with ENOENT the same way: a single delete's upward walk removes
// the epoch and then the lineage, so a writer can lose the epoch before
// its create and the lineage inside its next MkdirAll. One retry is not
// enough, so inDir re-creates the directory and retries while the error is
// ENOENT, up to maxDirVanishedRetries attempts. That cannot livelock short
// of an endless delete stream: each ENOENT needs some deleter's rmdir to
// have succeeded, which needs a fresh unlink that emptied the directory.
// A directory that keeps vanishing past the cap surfaces the error.
func inDir(dir string, create func() error) error {
	var err error
	for attempt := 0; attempt < maxDirVanishedRetries; attempt++ {
		err = os.MkdirAll(dir, 0o700)
		if err == nil {
			if afterMkdirHook != nil {
				afterMkdirHook(dir)
			}
			err = create()
		}
		if err == nil || !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return err
}

// createTempIn is os.CreateTemp(dir, pattern) under inDir's
// directory-vanished retry.
func createTempIn(dir, pattern string) (f *os.File, err error) {
	err = inDir(dir, func() error {
		f, err = os.CreateTemp(dir, pattern)
		return err
	})
	return f, err
}

// commitTemp renames a fully written temp file into place at p and syncs
// the directory so the rename survives power loss (see fsutil.SyncDir).
func commitTemp(tmp, p string) error {
	if err := os.Rename(tmp, p); err != nil {
		os.Remove(tmp)
		return err
	}
	syncDirAfterRename(filepath.Dir(p))
	return nil
}

// PutReader implements store.ReaderPutter's unconditional overwrite: same
// contract as Put, streamed via writeReader instead of write.
func (l *Local) PutReader(key string, r io.Reader, size int64) error {
	p, err := l.path(key)
	if err != nil {
		return err
	}
	_, err = l.writeReader(p, r, size)
	return err
}

// PutReaderIf implements store.ReaderPutter's CAS write: same contract and
// same per-key lock as PutIf, streamed via writeReader instead of write.
//
// The ifMatch == "" (create-only) case deliberately checks existence with
// os.Stat rather than PutIf's os.ReadFile: PutIf already holds its new
// payload buffered in the caller's []byte, so reading the old content too
// (needed for the ifMatch != "" comparison below) costs nothing extra
// end-to-end. Here the whole point of the call is to avoid buffering a
// large object — reading a potentially-large EXISTING orphan into memory
// just to discover it exists (flush.go's create-only retry after a crashed
// prior attempt is exactly this case: an existing, possibly multi-GB,
// object at objKey) would defeat that for the one case this method's
// caller actually uses. The ifMatch != "" branch still needs the old
// content's hash to compare, same as PutIf, and reads it the same way; no
// caller in this codebase exercises that branch on a large object today.
func (l *Local) PutReaderIf(key string, r io.Reader, size int64, ifMatch string) (string, error) {
	p, err := l.path(key)
	if err != nil {
		return "", err
	}
	// Stream the object before taking the lock: a large snapshot can take
	// minutes to write, and every other writer to this key waits on the
	// lock (5 s before it gives up). Holding it only for the compare and
	// the rename keeps the hold in the millisecond range.
	tmp, etag, err := l.writeReaderTemp(p, r, size)
	if err != nil {
		return "", err
	}
	release, err := l.lock(p)
	if err != nil {
		os.Remove(tmp)
		return "", err
	}
	defer release()
	fail := func(err error) (string, error) {
		os.Remove(tmp)
		return "", err
	}

	if ifMatch == "" {
		if _, statErr := os.Stat(p); statErr == nil {
			return fail(fmt.Errorf("%w: key exists", ErrCAS))
		} else if !os.IsNotExist(statErr) {
			return fail(statErr)
		}
	} else {
		cur, readErr := os.ReadFile(p)
		if os.IsNotExist(readErr) {
			return fail(fmt.Errorf("%w: key absent, expected etag %s", ErrCAS, ifMatch))
		}
		if readErr != nil {
			return fail(readErr)
		}
		if etagOf(cur) != ifMatch {
			return fail(fmt.Errorf("%w: etag mismatch", ErrCAS))
		}
	}
	if err := commitTemp(tmp, p); err != nil {
		return "", err
	}
	return etag, nil
}

// lock takes the per-key lock PutIf, PutReaderIf and DeleteIf serialize
// their read-compare-write under. The lock is an flock(2) on p+".lock", so
// holding it means holding an open descriptor: a process that dies inside
// the critical section releases it with its last descriptor, and a lock
// file left on disk is just a file. There is deliberately no "stale file"
// heuristic — the previous existence-plus-mtime design broke a lock by
// path, and two breakers racing the same stale file could delete each
// other's fresh locks and both enter the CAS (one update was then lost
// silently). Release unlinks the file while still holding the flock, and
// an acquirer re-checks that the inode it locked is still the one at
// lockPath, so a waiter that locked an unlinked inode retries on the new
// file rather than holding a lock nobody else can see. Unlinking on
// release is what lets removeEmptyParents clear an emptied directory.
func (l *Local) lock(p string) (release func(), err error) {
	lockPath := p + ".lock"
	deadline := time.Now().Add(5 * time.Second)
	// vanished tracks whether the most recent failed attempt lost to a
	// vanishing directory (fs.ErrNotExist) rather than a held lock, so a
	// timeout can name which case occurred.
	vanished := false
	for {
		var f *os.File
		// inDir: the directory can vanish between MkdirAll and the create
		// when a delete elsewhere has just emptied it.
		err := inDir(filepath.Dir(p), func() error {
			var err error
			f, err = os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
			return err
		})
		switch {
		case err == nil:
			vanished = false
			if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
				if sameFileAt(f, lockPath) {
					return func() {
						os.Remove(lockPath)
						syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
						f.Close()
					}, nil
				}
				// Locked an inode a releaser has already unlinked: the
				// live lock is the new file at lockPath. Retry at once.
				f.Close()
				continue
			} else if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
				f.Close()
				return nil, err
			}
			f.Close()
		case errors.Is(err, fs.ErrNotExist):
			// A directory still vanishing after inDir's retries is waited
			// out like a held lock, so lock's deadline (not inDir's cap)
			// bounds it.
			vanished = true
		default:
			return nil, err
		}
		if time.Now().After(deadline) {
			if vanished {
				return nil, fmt.Errorf("store: lock timeout on %s (its directory kept vanishing underneath the retry)", lockPath)
			}
			return nil, fmt.Errorf("store: lock timeout on %s (another offshoot process held this key's lock for over 5s)", lockPath)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// sameFileAt reports whether f is the file currently at path.
func sameFileAt(f *os.File, path string) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	pi, err := os.Stat(path)
	return err == nil && os.SameFile(fi, pi)
}

func (l *Local) PutIf(key string, data []byte, ifMatch string) (string, error) {
	p, err := l.path(key)
	if err != nil {
		return "", err
	}
	release, err := l.lock(p)
	if err != nil {
		return "", err
	}
	defer release()

	cur, err := os.ReadFile(p)
	switch {
	case os.IsNotExist(err):
		if ifMatch != "" {
			return "", fmt.Errorf("%w: key absent, expected etag %s", ErrCAS, ifMatch)
		}
	case err != nil:
		return "", err
	default:
		if ifMatch == "" {
			return "", fmt.Errorf("%w: key exists", ErrCAS)
		}
		if etagOf(cur) != ifMatch {
			return "", fmt.Errorf("%w: etag mismatch", ErrCAS)
		}
	}
	return l.write(p, data)
}

func (l *Local) List(prefix string) ([]string, error) {
	var keys []string
	root := l.root
	start := l.listStart(prefix)
	err := filepath.WalkDir(start, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				if p == start {
					return fs.SkipAll // nothing stored under this prefix
				}
				// A concurrent delete emptied and removed this directory
				// (removeEmptyParents) after WalkDir listed its parent and
				// before it read the directory itself. It held only deleted
				// objects, so skipping it is the answer a List ordered after
				// that delete gives.
				if d != nil && d.IsDir() {
					return fs.SkipDir
				}
				return nil
			}
			return err
		}
		if listVisitHook != nil {
			listVisitHook(p)
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(p, ".lock") || strings.Contains(filepath.Base(p), ".tmp-") {
			return nil
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(rel)
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(keys)
	return keys, nil
}

// listStart is the directory List walks for prefix: the deepest directory
// the prefix names in full (everything up to its last "/"), so a lineage
// List reads that lineage's own directory and a refs List only refs/. A
// local store's root also holds the checkouts, their shadows and the
// by-chain cache, which can outnumber the store's objects many times over;
// walking the root for every List made chain resolution — one List per base
// hop, several resolutions per fork, checkout and checkpoint — cost time in
// proportion to every file on disk. Every key under prefix lives under this
// directory, so the result is the same as a whole-root walk filtered by
// prefix.
func (l *Local) listStart(prefix string) string {
	i := strings.LastIndex(prefix, "/")
	if i <= 0 {
		return l.root
	}
	p, err := l.path(prefix[:i])
	if err != nil {
		// Not a key directory (e.g. ".." in it): keep the whole-root
		// walk's answer rather than inventing an error List never
		// returned.
		return l.root
	}
	return p
}

// tempName returns a unique path in dir (using base as the filename
// pattern's stem) that is guaranteed to have been free at least momentarily,
// without leaving a file behind. CopyObject needs this rather than write()'s
// os.CreateTemp-and-keep-the-handle approach: reflink.CopyFile (like the
// clonefile(2)/FICLONE syscalls it wraps) requires its destination to not
// already exist, so the temp path itself must be absent, not merely unique.
// The same benign race write()'s doc comment accepts for its own per-call
// temp names applies here identically.
func tempName(dir, base string) (string, error) {
	f, err := os.CreateTemp(dir, base+".tmp-*")
	if err != nil {
		return "", err
	}
	name := f.Name()
	if err := f.Close(); err != nil {
		os.Remove(name)
		return "", err
	}
	if err := os.Remove(name); err != nil {
		return "", err
	}
	return name, nil
}

// CopyObject makes dst a byte-identical copy of src, using reflink.CopyFile
// (a filesystem clone on APFS/btrfs/xfs-with-reflink, a plain byte copy
// otherwise) so the copy is near-constant-time when the filesystem supports
// it. Like write(), it copies to a uniquely-named temp file first and
// renames into place, so a reader of dst never observes a partial file and
// a failure partway through leaves dst untouched.
func (l *Local) CopyObject(dst, src string) error {
	dstPath, err := l.path(dst)
	if err != nil {
		return err
	}
	srcPath, err := l.path(src)
	if err != nil {
		return err
	}
	if _, err := os.Stat(srcPath); err != nil {
		if os.IsNotExist(err) {
			return ErrNotFound
		}
		return err
	}
	dir := filepath.Dir(dstPath)
	// tempName leaves dir momentarily empty (it removes the file it made),
	// so a racing delete can remove dir before the clone lands in it, and
	// the clone then fails ENOENT just as for a missing source. Only a
	// missing source maps to ErrNotFound; a vanished dir is retried by inDir.
	var tmp string
	err = inDir(dir, func() error {
		var err error
		if tmp, err = tempName(dir, filepath.Base(dstPath)); err != nil {
			return err
		}
		if _, err := reflink.CopyFile(tmp, srcPath); err != nil {
			os.Remove(tmp)
			if os.IsNotExist(err) {
				if _, serr := os.Stat(srcPath); os.IsNotExist(serr) {
					return ErrNotFound
				}
			}
			return err
		}
		// A clone inherits the source's 0600; reflink's plain-copy fallback
		// (ext4, most CI hosts) creates 0644. Every object is 0600.
		if err := os.Chmod(tmp, 0o600); err != nil {
			os.Remove(tmp)
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, dstPath); err != nil {
		os.Remove(tmp)
		return err
	}
	syncDirAfterRename(dir)
	return nil
}

// removeEmptyParentsHook, when non-nil (tests only), runs between a
// delete's unlink and its first directory removal, so a test can interleave
// a write into the directory being emptied.
var removeEmptyParentsHook func()

// afterMkdirHook, when non-nil (tests only), runs after every successful
// MkdirAll a writer does before creating its temp or lock file, so a test
// can remove the directory the way a racing delete in another process could.
var afterMkdirHook func(dir string)

// listVisitHook, when non-nil (tests only), runs for every entry List's
// walk visits without error, so a test can delete mid-walk.
var listVisitHook func(p string)

// maxDirVanishedRetries bounds inDir's attempts (see inDir).
const maxDirVanishedRetries = 32

func (l *Local) Delete(key string) error {
	p, err := l.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	l.removeEmptyParents(p)
	return nil
}

// removeEmptyParents removes the directories a delete of the object at p
// left empty: its own directory (an epoch, data/<lineage>/<epoch>), then,
// if that went, the next one up (the lineage), and so on, stopping below
// the store root's top-level directories, so the root, data/, refs/ and
// any other top-level directory (each a prefix root listStart walks from)
// are never removed. Every at-rest checkpoint mints an epoch, and List
// walks every directory under its prefix, so without this a lineage's
// listing cost grew with every epoch it ever had, live or GC'd. A removed
// directory is re-created by the next write into it (inDir), and a List
// whose start directory is gone answers empty (see List).
//
// It is best-effort and never fails the delete. os.Remove on a directory is
// rmdir(2), which removes only an empty directory and otherwise fails
// ENOTEMPTY/EEXIST; the first failure of any kind, including ENOENT
// (already removed), stops the walk. So a directory still holding another
// object, a live per-key lock (lock() puts p+".lock" beside the object) or
// an in-flight writer's temp file (write and writeReaderTemp create theirs
// in the object's directory) survives, and the removal is safe against
// lock and temp files without inspecting them: a writer's file either
// exists when rmdir runs, and the rmdir fails harmlessly, or does not yet
// exist, and the writer's O_CREAT finds the directory gone (ENOENT) and
// inDir re-creates it and retries. No interleaving loses a write or a
// lock. DeleteIf calls this only after releasing its own lock, which would
// otherwise keep the directory non-empty.
func (l *Local) removeEmptyParents(p string) {
	if removeEmptyParentsHook != nil {
		removeEmptyParentsHook()
	}
	root := filepath.Clean(l.root)
	inside := root + string(filepath.Separator)
	for dir := filepath.Dir(p); strings.HasPrefix(dir, inside) && filepath.Dir(dir) != root; dir = filepath.Dir(dir) {
		if err := os.Remove(dir); err != nil {
			return
		}
	}
}

// DeleteObjects implements store.BatchDeleter as a plain sequential loop
// over Delete: a local directory has no round trips to batch away, but
// implementing the capability keeps callers (ops' GC sweep) on one uniform
// code path across backends. Semantics follow the interface contract and
// Delete exactly — a missing file counts as deleted (os.Remove's
// IsNotExist is already success in Delete above); the first real error
// stops the loop and is returned alongside the keys deleted so far, which
// matches what a per-key fallback loop would have done.
func (l *Local) DeleteObjects(keys []string) (deleted []string, err error) {
	for _, k := range keys {
		if err := l.Delete(k); err != nil {
			return deleted, err
		}
		deleted = append(deleted, k)
	}
	return deleted, nil
}

// WritesSettled implements store.SettledWriter: PutIf and PutReaderIf
// compare and rename under the key's lock, and return an error only
// before the rename that would apply the write.
func (l *Local) WritesSettled() bool { return true }

// DeleteIf implements store.ConditionalDeleter: a true compare-and-delete,
// using the exact same per-key flock PutIf uses to implement its own
// compare-and-swap (see the package doc comment on Local).
// Absent (already deleted, or never existed) or content that no longer
// hashes to ifMatch both fail with ErrCAS — a caller (ops.Destroy) that
// raced this against a concurrent write to the same key sees exactly the
// same "your compare failed, retry" signal PutIf's own callers already
// know how to handle.
func (l *Local) DeleteIf(key, ifMatch string) error {
	p, err := l.path(key)
	if err != nil {
		return err
	}
	removed, err := l.deleteIfLocked(p, ifMatch)
	if removed {
		// Only after the lock is released: its file sits in the directory
		// removeEmptyParents would remove.
		l.removeEmptyParents(p)
	}
	return err
}

// deleteIfLocked is DeleteIf's compare-and-unlink under p's lock; removed
// reports whether this call unlinked the object.
func (l *Local) deleteIfLocked(p, ifMatch string) (removed bool, err error) {
	release, err := l.lock(p)
	if err != nil {
		return false, err
	}
	defer release()

	cur, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		return false, fmt.Errorf("%w: key absent, expected etag %s", ErrCAS, ifMatch)
	}
	if err != nil {
		return false, err
	}
	if etagOf(cur) != ifMatch {
		return false, fmt.Errorf("%w: etag mismatch", ErrCAS)
	}
	if err := os.Remove(p); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
