# ltx v0.5.1 fixture, 64 KiB pages, incompressible

Objects written by `github.com/superfly/ltx` **v0.5.1** (header `Version`
3, LZ4 *frame* page format: `lz4.NewWriter` with `Block64Kb`, `Fast`,
content checksum, one block per page), the format this repo wrote until
2026-09-26. The sibling `../ltx-v0.5.1` set covers 4 KiB pages; this set
covers SQLite's largest page size, 64 KiB, with a data page that LZ4
cannot shrink, so the page frame carries a *stored* block of exactly
65536 bytes: the largest frame the v0.5.1 shape can hold and the exact
edge of `frameGuard`'s per-block limit. `TestDecodesLTXv051FrameFormat`
decodes both sets and asserts this one's largest page frame is at least a
page long.

| file           | bytes  | what                                                   |
|----------------|--------|--------------------------------------------------------|
| `snapshot.ltx` | 66122  | txid 1, pages 1–2 (`Commit` 2), `Timestamp` 1           |
| `segment.ltx`  | 66124  | txid 2, pages 1–2 (`Commit` 2), `Timestamp` 2, `PreApplyChecksum` = snapshot's `PostApplyChecksum` |
| `final.sqlite` | 131072 | the database after txid 2                                |

`final.sqlite` is in rollback-journal mode (`journal_mode=DELETE`; header
bytes 18–19 are `01 01`) with no `-wal`/`-shm`, as the 4 KiB set's is.

## The database

```sql
PRAGMA page_size = 65536;
PRAGMA journal_mode = DELETE;
CREATE TABLE t (id INTEGER PRIMARY KEY, v BLOB);
INSERT INTO t (id, v) VALUES (1, ?);   -- txid 1: blob1, 65496 bytes
UPDATE t SET v = ? WHERE id = 1;       -- txid 2: blob2, 65496 bytes
```

65496 bytes is the largest blob the row can hold inside one 64 KiB table
leaf page (65501 bytes of local payload less the 5-byte record header),
so the file is two pages — page 1 the header and schema, page 2 the leaf
whose 65505-byte cell is all but 21 bytes of the page — and no overflow
page is needed. Both blobs come from Go's `math/rand` (the v1 package,
whose seeded stream is frozen by the Go 1 compatibility promise):

```go
rng := rand.New(rand.NewSource(64))
rng.Read(blob1) // 65496 bytes
rng.Read(blob2) // 65496 bytes
```

Transaction 2 rewrites page 2 and, as every rollback-journal commit does,
page 1's change counter, so the segment carries both pages.

## How it was generated

A one-off `package main` outside this repository, with this `go.mod`:

```
module ltx64kgen

go 1.26.0

require (
	github.com/mattn/go-sqlite3 v1.14.52
	github.com/superfly/ltx v0.5.1
)

require github.com/pierrec/lz4/v4 v4.1.22 // indirect
```

1. Build the database with the statements above through `database/sql`
   and `go-sqlite3` (SQLite 3.53.4, which is what `final.sqlite`'s header
   bytes 96–99 record), closing the connection after each transaction and
   reading the whole file as state 1 and state 2.
2. `snapshot.ltx`: `ltx.NewEncoder`, `EncodeHeader(ltx.Header{Version:
   ltx.Version, PageSize: 65536, Commit: 2, MinTXID: 1, MaxTXID: 1,
   Timestamp: 1})`, `EncodePage` for pages 1 and 2 of state 1,
   `SetPostApplyChecksum` with the XOR fold of `ltx.ChecksumPage` over
   those pages under `ltx.ChecksumFlag` (the lock page, 16385 at this
   page size, is out of range), `Close`.
3. `segment.ltx`: the same with `Commit: 2, MinTXID: 2, MaxTXID: 2,
   Timestamp: 2, PreApplyChecksum: <snapshot's PostApplyChecksum>`,
   carrying each page of state 2 that differs from state 1, and the fold
   over state 2 as its `PostApplyChecksum`.
4. `final.sqlite` is state 2 verbatim; the snapshot was decoded with
   v0.5.1's own `Decoder.DecodeDatabaseTo` and compared with state 1
   before anything was written.

Running the generator twice produces byte-identical files.

Checksums: snapshot `PostApplyChecksum` `f5e6b3fbb61763a7`, segment
`PostApplyChecksum` `ea3b0436e5eb20ca`. Page frames: snapshot page 1 is
414 bytes, page 2 is 65555 (7-byte descriptor, 4-byte block size with the
stored bit, 65536 bytes of page, EndMark, content checksum); the segment's
are 416 and 65555.
