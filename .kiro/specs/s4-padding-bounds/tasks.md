# Tasks: AWG transport padding safety

## Progress

| Status | Task | Evidence |
| --- | --- | --- |
| Completed | TSK-001 | Red: H1/H4 collision classified transport as initiation (`go test ./device -run TestDeterminePacketTypeAndPadding_RandomTrailer`). |
| Completed | TSK-002 | `go test ./device -run TestEnsureOutboundBuffer -count=1` passes. |
| Completed | TSK-003 | `go test ./device -run TestDeterminePacketTypeAndPadding_RandomTrailer -count=1` passes. |
| Completed | TSK-004 | `go test ./device -count=1` passes. |
| Completed | TSK-005 | `go build ./...`, baseline verification, commit `a59657bf6`, and push to `origin/fix/s4-padding-bounds` passed. |

## Dependency graph

`TSK-001 → TSK-002 → TSK-003 → TSK-004 → TSK-005`

- [x] **TSK-001 — Add failing regression coverage**
  - Факт: `random_trailer_classification_test.go` fails against the current classifier exactly as expected.
  - Add focused test cases for the 256-byte/S4=278 allocation invariant and random-trailer H1/H4 collision.
  - Prove the relevant test is red before production code changes.
  - Requirement: outbound safety; ambiguous classification.

- [x] **TSK-002 — Repair the outbound encryption boundary**
  - Факт: a 256-byte legacy layout is replaced, plaintext is moved to the S4-aware offset, and impossible layouts are dropped.
  - Update `forks/wireguard-go/device/send.go` so `RoutineEncryption` sizes/replaces a too-small buffer before S4-based slicing.
  - Preserve plaintext and release replaced buffers; drop impossible layouts safely.
  - Requirement: outbound safety.

- [x] **TSK-003 — Resolve authenticated H1–H4 ambiguity**
  - Факт: invalid H1/H4 collision now selects transport; valid MAC1 initiation remains initiation.
  - Update `forks/wireguard-go/device/receive.go` with an H4 classifier and MAC1-aware fallback that does not mutate a packet before the decision.
  - Requirement: ambiguous classification.

- [x] **TSK-004 — Run targeted verification**
  - Факт: полный `go test ./device -count=1` прошёл.
  - Run focused `go test` cases, then the device package tests; diagnose any failure before continuing.
  - Requirement: all acceptance criteria.

- [x] **TSK-005 — Build and publish**
  - Факт: `go build ./...` и `bash release/verify_upstream_baseline.sh` прошли; ветка опубликована в `origin/fix/s4-padding-bounds`.
  - Build HydraCore's supported Go target, inspect the exact diff, commit only task files, then push branch `fix/s4-padding-bounds`.
  - Requirement: delivery.
