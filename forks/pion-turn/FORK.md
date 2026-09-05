# Fork: github.com/pion/turn/v4

Vendored inside HydraCore so the transport stack owns its own packet path. Wired in through a
filesystem `replace` in the root `go.mod`; there is no external repository and no upstream PR.

## Upstream this was taken from

| | |
|---|---|
| module | `github.com/pion/turn/v4` |
| version | `v4.1.4` |
| commit | `7ca9d6ab0d9491176cf8a22e0e054f4e74ef3ce8` (`refs/tags/v4.1.4`) |
| tagged | 2026-01-08 |
| module zip | `h1:EU11yMXKIsK43FhcUnjLlrhE4nboHZq+TXBIi3QpcxQ=` |
| go.mod | `h1:ES1DXVFKnOhuDkqn9hn5VJlSWmZPaRJLyBXoOeO/BmQ=` |

To resync: fetch the new tag, diff it against `v4.1.4`, and reapply the changes below. Nothing
else in the tree has been touched, so a three-way merge against that commit is enough.

## Local changes

All three are allocation removals, found by CPU/heap profiling the Android client. The first two
are on the inbound packet path, where `handleChannelData` was the largest single allocation site
in the process and `HandleInbound` the second; the third is the outbound counterpart.

### 1. `client.go` — `Client.handleChannelData` decodes in place

Was: a fresh `make([]byte, len(data))` plus a copy for every inbound datagram.

The copy was redundant. `data` is a slice of the single buffer owned by the read loop in
`Client.Listen`, and the call is synchronous, so it stays valid for the whole call.
`proto.ChannelData.Decode` only reads the four-byte header and reslices `Raw` into `Data` — it
retains nothing. The one consumer, `relayedConn.HandleInbound`, is synchronous as well and takes
its own copy before the datagram reaches another goroutine.

### 2. `internal/client/udp_conn.go` — pooled queue buffers

Here the copy is required: `data` belongs to the caller's read loop and is overwritten on its
next read, while the datagram waits in `readCh` for a different goroutine. Only the source of the
memory changed — a `sync.Pool` of `pooledInboundSize` (2048) byte buffers, with a plain
allocation for anything larger, which never enters the pool.

Released in `UDPConn.ReadFrom` once the payload has been copied out — on the success path and on
the `io.ErrShortBuffer` path — and in `HandleInbound`'s `default:` branch, where a full queue
means nobody will ever read the datagram. `readCh` has exactly one consumer and delivers each
entry once, so those are all the release points there are.

Deliberately not pooled at `maxDataBufferSize` (65535): `maxReadQueueSize` is 1024, so a full
queue of maximum-size buffers would hold 64 MB.

### 3. `internal/client/udp_conn.go` — pooled outbound ChannelData frame

Was: `sendChannelData` built a fresh `proto.ChannelData` whose `Raw` was nil, so `Encode` grew a
new slice and copied the whole payload into it for every outbound datagram.

Measured at 1416 B and two allocations per packet, against none with the buffer kept — 492 ns/op
against 27 ns/op for a 1287-byte payload, which is one QUIC packet inside a DTLS record.

The frame is encoded into a `sync.Pool` buffer of `pooledOutboundSize` (2048), with a plain
allocation for anything larger, which never enters the pool. `Client.WriteTo` hands the frame
straight to the socket and retains nothing, so the release point is immediately after it returns —
on the error path as well, since the frame is dead either way. A buffer that `Encode` had to grow
anyway is not returned, which is why the capacity is checked rather than the length.

## Verification

`go test ./...` in this directory passes in full, including `internal/client`,
`internal/allocation` and the top-level client suite.
