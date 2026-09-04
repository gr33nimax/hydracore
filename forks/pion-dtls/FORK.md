# Fork: github.com/pion/dtls/v3

Vendored inside HydraCore so the transport stack owns its own packet path. Wired in through a
filesystem `replace` in the root `go.mod`; there is no external repository and no upstream PR.

## Upstream this was taken from

| | |
|---|---|
| module | `github.com/pion/dtls/v3` |
| version | `v3.1.2` |
| commit | `a621789e8dba850944500deda8eaa1c0dc4d92f0` (`refs/tags/v3.1.2`) |
| tagged | 2026-02-12 |
| module zip | `h1:gqEdOUXLtCGW+afsBLO0LtDD8GnuBBjEy6HRtyofZTc=` |
| go.mod | `h1:Hw/igcX4pdY69z1Hgv5x7wJFrUkdgHwAn/Q/uo7YHRo=` |

To resync: fetch the new tag, diff it against `v3.1.2`, and reapply the changes below. Nothing
else in the tree has been touched, so a three-way merge against that commit is enough.

## Local changes

One allocation removal on the inbound record path, found by heap profiling the Android client:
`ApplicationData.Unmarshal` and `Conn.handleIncomingPacket` together were the second largest
allocation source in the process.

### 1. `pkg/protocol/application_data.go` — pooled payload

Was: `a.Data = append([]byte{}, data...)` — a fresh allocation for every inbound application
record.

The copy is required. `data` is the connection's read buffer and the payload outlives the call: it
is handed to `c.decrypted` and read by a different goroutine. Only the source of the memory
changed — a `sync.Pool` of `pooledApplicationDataSize` (2048) byte buffers, with a plain
allocation for anything larger, which never enters the pool.

`ReleaseApplicationData` is exported so the record's owner can give the buffer back. It is
**best-effort by design**: a payload that is never released is collected exactly as before, so a
caller outside this package that holds on to `Data` cannot be corrupted by the change — it simply
gets no benefit.

### 2. `conn.go` — `Conn.Read` releases the payload

`c.decrypted` has exactly one consumer and delivers each payload once, and `val` is not referenced
after the copy into the caller's buffer. So the release goes there: after `copy(buff, val)` on the
success path, and before returning `errBufferTooSmall` on the path that copies nothing. Payloads
still queued when the connection closes are simply not returned, which a pool tolerates.

## Verification

`go test ./...` in this directory passes in full, including the `e2e` suite and the main `dtls`
suite.
