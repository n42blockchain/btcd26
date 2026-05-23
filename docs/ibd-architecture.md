# IBD Architecture: Parallel Block Fetch, Reorder Buffer, MDBX Storage

This document describes the Initial Block Download (IBD) pipeline introduced
between commits `191ff9c7`, `4592f0c4`, and `48914861`.  Upstream btcd ships
a single-sync-peer in-order downloader; the changes here turn it into a
parallel downloader with serial application, paired with an MDBX-backed
storage layer that compresses metadata and a chain layer that skips spend
journal writes for blocks before the last hard-coded checkpoint.

The goal is empirical:  on a 32-core Windows host with 24 outbound peers,
mainnet IBD from a near-empty database completed at roughly **25 blk/s**
sustained (with pre-checkpoint bursts to 80+ blk/s and post-checkpoint
sustained around 8-15 blk/s, network-dominated).

---

## 1. Module map

```
                     ┌──────────────────────────────────────┐
                     │           blockHandler (single goroutine)
                     │  (netsync/manager.go: all the below
                     │   functions run here under one lock)
                     ├──────────────────────────────────────┤
                     │                                      │
inv from peer  ──→   │  handleInvMsg                        │
header from peer ──→ │  handleHeadersMsg ── ProcessBlockHeader → blockchain
block from peer  ──→ │  handleBlockMsg                      │
1s tick          ──→ │  checkHeadOfLineStall                │
1s tick          ──→ │  scheduleParallelFetch               │
peer up/down    ──→  │  handleNewPeerMsg / handleDonePeerMsg│
                     │                                      │
                     │  ┌────────────────────────────────┐  │
                     │  │  Reorder buffer state          │  │
                     │  │    orderedBlocks[height]       │  │
                     │  │    orderedNext, orderedNextSince│ │
                     │  └────────────────────────────────┘  │
                     │                                      │
                     │  drainOrderedBlocks ──→ applyBlock   │
                     │                          │           │
                     │                          ▼           │
                     │                  ProcessBlock (chain)│
                     │                          │           │
                     │                          ▼           │
                     │                  utxoCache.connectTx │
                     │                          │           │
                     │                          ▼           │
                     │                  db.Update(...)      │
                     │                          │           │
                     └──────────────────────────┼───────────┘
                                                ▼
                              ┌───────────────────────────────┐
                              │ database/mdbxdb               │
                              │ ┌───────────────────────────┐ │
                              │ │ bucket.Put (codec.Encode) │ │
                              │ │ bucket.PutBatch (sort+    │ │
                              │ │           parallel encode)│ │
                              │ │ writeBlock → .fdb         │ │
                              │ └───────────────────────────┘ │
                              └───────────────────────────────┘
```

All netsync operations happen on **one** goroutine (`blockHandler`).  Peer
network goroutines marshal messages onto `sm.msgChan`; the block handler
dequeues and processes them serially.  This is the upstream contract and
all the new code preserves it.

---

## 2. Parallel download / serial apply pipeline

### 2.1 Why it exists

Upstream btcd downloads blocks from `sm.syncPeer` only.  A typical
residential peer caps a single TCP flow at 5–15 Mbps, so even on a
gigabit-capable host the chain advances at ~10 blk/s through high-density
heights.  The same chain on the same host can be served from many peers in
parallel; the bottleneck is then validation and disk, not network.

### 2.2 What the new pipeline does

1. **Fan out** the block-fetch step across `defaultTargetOutbound=24`
   peers.  Each peer is assigned an independent `assignChunkSize=16`-block
   chunk via `assignBlocksToPeer`; the global `sm.requestedBlocks` map
   guarantees no two peers are asked for the same height.
2. **Reorder on arrival**.  Blocks arrive in whatever order TCP and peer
   bandwidth deliver them.  `handleBlockMsg` puts each block in the
   `orderedBlocks` map keyed by height, then calls `drainOrderedBlocks`.
3. **Apply in order**.  `drainOrderedBlocks` walks consecutive heights
   starting at `orderedNext` and calls `applyBlock` (the bookkeeping
   wrapper around `ProcessBlock`) for each.  When it hits a gap, it
   returns and waits for the next arrival to fill the missing height.
4. **Speculative re-request** if a single peer holds the head-of-line
   block longer than `headOfLineTimeout=5 s`, `checkHeadOfLineStall`
   issues a duplicate `getdata` to another peer.  First delivery wins;
   late deliveries from the original peer are silently discarded by
   `handleBlockMsg`'s duplicate-buffer guard.
5. **Reclaim** stalled peers: any peer with no activity for
   `peerAssignmentTimeout=15 s` (or 5× that for *established* peers, see
   §2.4) gets their in-flight hashes returned to the global pool and
   the peer disconnected; `connmgr` will dial a replacement.

### 2.3 Back-pressure

`scheduleParallelFetch` refuses to issue new assignments when
`len(orderedBlocks) >= reorderBufferCap=2048`.  This bounds peak memory
to roughly `2048 × max_block_size` (~8 GiB worst case at 4 MiB
max-witness blocks; far less in practice because most blocks are
0.5-2 MiB).

The drain loop runs every block arrival AND every 1 s tick.  Under
sustained network load the buffer hovers at cap and the chain advancer
runs at the rate `ProcessBlock` can sustain; under network slack the
buffer drains and assignment resumes.

### 2.4 Peer pinning ("established" peers)

Once a peer has delivered `establishedPeerThreshold=32` blocks during
the current connection, it is considered *established* and gets three
distinct privileges:

- **10× longer reclaim timeout**: 150 s instead of 15 s before
  disconnection (`reclaimStalledAssignments`).
- **2× larger chunks**: 32 heights per assignment instead of 16.
- **Priority scheduling**: the assignment pass iterates established
  peers first (loop pass 0), then unproven peers (pass 1).  This
  matters because Go's map iteration order is random; without the
  two-pass split, an established IDC-grade peer and a new residential
  peer would compete for the head-of-line block on equal terms.

The threshold of 32 was chosen so a peer must demonstrate sustained
delivery (≈8-10 seconds of real work) before its connection is
"protected".  A single fast burst from a slow peer doesn't qualify.

### 2.5 `ibdMode` race fix at startup

Upstream `sm.ibdMode` defaults to `false`.  The `handleInvMsg` path
honours `if sm.ibdMode { continue }` to ignore peer block
announcements during IBD.  But during the brief window between
`Start()` and the first `startSync()` call, `ibdMode` was `false` even
on a clearly-behind node, so peers' inv messages were processed and
their blocks fetched and dropped into the *legacy* orphan pool —
producing chains of dozens of orphans on every restart.

`Start()` now sets `sm.ibdMode = true` defensively; `startSync()`
clears it back to `false` only if `isInIBDMode()` confirms the chain
is genuinely current.

---

## 3. MDBX storage layer (`database/mdbxdb`)

### 3.1 Wire-compatible block storage

`.fdb` files keep ffldb's exact byte format: `[4B network][4B blockLen][raw
block][4B CRC32]`.  Migration from ffldb hard-links the files when source
and destination share a filesystem, so the move is instant.

### 3.2 Metadata KV store

Everything except the raw block bytes (block index, UTXO set, spend
journal, addridx, cfindex when enabled, best-state, etc.) lives in the
MDBX-backed metadata store.  Buckets get optional per-bucket codecs:

| Bucket             | Codec        | Why                                |
|--------------------|--------------|------------------------------------|
| `utxosetv2`        | zstd-3       | High-volume, modest compressibility |
| `spendjournal`     | zstd-3       | Large per-block values              |
| `addridx`          | zstd-6       | Highly repetitive postings lists    |
| everything else    | identity     | Small values, compression overhead > saving |

### 3.3 `BatchPutter`: sort + parallel encode

`database.Bucket` gained an optional `BatchPutter` interface.
`mdbxdb.bucket.PutBatch`:

1. Sorts the input by key ascending (`bytes.Compare`).  MDBX writes
   in sorted order append to the right-most leaf instead of splitting
   pages randomly — this alone roughly halves UTXO-flush wall time
   on a populated database.
2. Encodes values in parallel: `putBatchChunkSize=4096` pairs per
   chunk, `runtime.NumCPU()` workers per chunk encoding into disjoint
   indices of a pre-allocated `[][]byte`, then a serial pass writes
   the encoded bytes through `tx.rawPut`.  zstd-3 compression of
   ~30 M UTXOs at flush time goes from single-threaded to
   `NumCPU`-way parallel.

`blockchain/utxocache.go`'s `writeCache` detects the optional
interface (`if batcher, ok := utxoBucket.(database.BatchPutter); ok`)
and falls back to per-entry `dbPutUtxoEntry` for ffldb.

### 3.4 `BTCD_MDBX_FAST_SYNC` environment variable

When set, the MDBX environment is opened with
`SafeNoSync|NoMetaSync` instead of `Durable`.  Commits return without
waiting for an `fsync`; the kernel page cache decides when bytes hit
disk.

After a hard process kill (OOM, blue screen) the last few seconds of
commits are lost.  On next start, `mdbxdb/reconcile.go` truncates
`.fdb` back to whatever MDBX's persisted `writeLoc` reports, and the
chain resumes from there.  The empirical recovery cost in this
project was 41 blocks on a 36-hour IBD — well under a minute of
re-sync.

For ordinary `btcctl stop` the flush completes normally and no data
is lost.

### 3.5 Pool buffer invariants

`encodeBufPool` and `keyBufPool` recycle the codec output buffer and
the bucketized-key buffer respectively.  Both rely on **mdbx_put**
copying its arguments into the btree page synchronously before
returning.  This is the documented contract of standard `mdbx_put`;
the only exception (`MDBX_RESERVE`) is unused in this code.  If a
future change introduces `MDBX_RESERVE` here, the pool recycling
must be re-evaluated.

---

## 4. `skipUndo`: spend journal short-circuit

`blockchain/chain.go::connectBlock` writes the per-block spend
journal entry — a list of every UTXO that block consumed, used during
chain reorg to recreate the pre-block UTXO state.

CPU profiling showed this single `mdbx_put` (a 30–100 KiB value, so
spanning several MDBX overflow pages) was 73 % of total IBD CPU at
heights past 480 k.

`CheckBlockHeaderContext` already enforces that no chain whose
disagreement point predates the last hard-coded checkpoint can ever
become the best chain.  Pre-checkpoint blocks therefore cannot be
reorged away, and their spend journals are unreachable.  `connectBlock`
now skips the write when `node.height <= LatestCheckpoint().Height`.

`disconnectBlock` has a defensive assertion that fires if it is ever
invoked for a pre-checkpoint height — that would indicate either an
edited-checkpoint workflow or a logic bug, both of which would
silently corrupt the UTXO set without the check.

Once the chain crosses the last checkpoint (mainnet: 810 000)
`connectBlock` resumes normal spend-journal writes.  The IBD speedup
applies only to the historical bulk; the post-checkpoint phase still
pays full per-block cost.

---

## 5. Empirical performance

All numbers from one full mainnet IBD on a Windows host, 32 logical
cores, NVMe storage, residential gigabit, May 2026.

### 5.1 Rate vs. height regime

| Height range  | Sustained          | Notes                                  |
|---------------|--------------------|----------------------------------------|
| 0 – 200 k     | 50 – 80 blk/s      | Tiny blocks, dominated by network RTT  |
| 200 k – 480 k | 25 – 30 blk/s      | Growing block size, sig-cache cold      |
| 480 k – 810 k | 15 – 25 blk/s      | Segwit witness bytes, larger blocks    |
| 810 k – tip   | 8 – 15 blk/s       | Post-checkpoint, spend journal writes   |

Burst rates (single 10 s window where the reorder buffer was full and
the chain advancer ran flat-out) hit **86 blk/s at ~700 k** and
**874 blk/s at ~600 k**, both with `GOGC=300`.

### 5.2 What was the bottleneck at each level

Determined by `pprof` snapshots at each tuning step.

| Stage                                | Bottleneck                       | Fix              |
|--------------------------------------|----------------------------------|------------------|
| Single peer, defaults                | Single TCP flow at ~5–15 Mbps    | `defaultTargetOutbound=24` + parallel scheduler |
| Multi-peer, defaults                 | 70 % CPU in `runtime.gcDrain`    | `GOGC=300` (4× heap) + buffer pools |
| GC tamed                              | 73 % CPU in `mdbx_put` of spend journal | `skipUndo` pre-checkpoint |
| Spend journal skipped                | UTXO flush serial zstd encode    | `BatchPutter` + parallel encode |
| Flush parallelised                   | UTXO `mdbx_put` page seeks       | Sort batch by key before writing |
| Sort + parallel                      | Network bandwidth                | (genuinely network-limited) |

### 5.3 Memory profile

- UTXO cache: configurable via `--utxocachemaxsize`, set to 4096 MiB in
  the `b.bat` recipe.
- Reorder buffer: up to `reorderBufferCap × max_block_size` ≈ 8 GiB
  worst case.  Typical 1-2 GiB.
- MDBX mmap: declared 2 TiB upper, only physically committed pages
  count toward RSS.  Observed peak ~50 GiB on a 350 GB database during
  high-cache-flush windows.
- Go heap: bounded by `GOMEMLIMIT=24GiB` in the recipe.  The runtime
  paces GC against this limit instead of pure doubling.

Total resident on a host with 64 GiB RAM during IBD: 50-80 GiB working
set was the observed range, comfortable for the machine but tight
enough that the user should not co-run other heavy processes.

---

## 6. Known limitations

### 6.1 Hot stop required

`BTCD_MDBX_FAST_SYNC` mode means **graceful stop is mandatory** for
zero data loss.  The recipe `b.bat` documents this.  A `task-killer`
kill loses ≤ 5 s of commits and a few hundred blocks of re-fetch on
next start.

### 6.2 OOM crash during IBD

The historic peak resident set during one IBD run was 82 GiB, which on
some hosts is the OOM cliff.  When OOM happens, MDBX's reconcile
recovers cleanly (the IBD picked up at the next start and rolled back
only 41 blocks), but it interrupts the run.

If your host has less than 64 GiB RAM, lower `--utxocachemaxsize` to
2048 and `GOMEMLIMIT` to 16 GiB; the run will be slower but stable.

### 6.3 Test coverage gaps

The new IBD pipeline does not yet have unit-test coverage for:

- `drainOrderedBlocks` drain on a gap
- `checkHeadOfLineStall` issuing a speculative re-request
- `reclaimStalledAssignments` disconnecting a slow peer
- `assignBlocksToPeer` chunk assignment with established vs. unproven
  peers
- Reorder buffer hitting `reorderBufferCap` and triggering back-pressure

These should be added as integration tests against a mocked
`peerNotifier`.

### 6.4 Single-peer "fastest-peer lock" experiment is permanent dead end

A scheme that disconnects all peers except the empirically fastest one
was tried (see `git log -p` for the reverted patch).  Empirically it
*hurt* throughput: typical residential peers can't sustain 30 blk/s
solo, and the lock+release+rediscover cycle thrashed every 30 s.  The
parallel multi-peer scheduler with established-peer pinning is
strictly better.

---

## 7. Tuning guide

The `b.bat` recipe ships with the values that worked best on a
32-core / 64-GiB Windows host with residential gigabit.  Adjust
according to your environment.

| Env / flag                  | Default      | When to change                                  |
|-----------------------------|--------------|-------------------------------------------------|
| `GOGC`                      | 500          | Lower (100-200) if peak memory is tight        |
| `GOMEMLIMIT`                | 24 GiB       | Match to your RAM minus 8 GiB                  |
| `BTCD_MDBX_FAST_SYNC`       | 1            | Unset for production-archival deployments      |
| `--utxocachemaxsize`        | 4096         | Match to GOMEMLIMIT / 4 roughly                |
| `--nocfilters`              | enabled      | Drop for archival nodes serving BIP157         |
| `defaultTargetOutbound`     | 24           | Lower to 12-16 if router can't handle 24 TCP   |
| `reorderBufferCap`          | 2048         | Lower to 256-512 if memory-constrained         |
| `assignChunkSize`           | 16           | Lower to 8 if peer pool quality is very mixed  |
| `headOfLineTimeout`         | 5 s          | Lower (2-3 s) if you have a large fast peer pool|

---

## 8. History pruning (`--prunetocheckpoint`)

A coarser-grained prune than Bitcoin Core's size-based `--prune`:
when the chain catches up past the most recent hard-coded checkpoint,
delete every `.fdb` file that sits entirely below it.  Headers stay
in the block index so the chain still verifies; only the raw block
bytes for pre-checkpoint heights disappear.  On mainnet, that's
roughly two thirds of the on-disk footprint (the chain's first
~870 k blocks vs. the ~80 k post-checkpoint blocks).

### Pieces

| Piece | Where | What it does |
|---|---|---|
| `--prunetocheckpoint` flag | `config.go` | Opt-in; mutually exclusive with `--prune`, `--txindex`, `--addrindex` (those need block data). |
| `BlockChainConfig.PruneToCheckpoint` | `blockchain/chain.go` | Carried through to `BlockChain.pruneToCheckpoint`. |
| `BlockFileLocator` interface | `database/interface.go` | Optional driver capability: `BlockFileNum(hash)` and `PruneBlockFilesBefore(keepFromFileNum)`.  mdbxdb implements it; ffldb does not. |
| `mdbxdb.transaction.PruneBlockFilesBefore` | `database/mdbxdb/tx.go` | Marks `.fdb` files for delete and drops matching block-index rows in one transaction.  Cold-segment locations are skipped. |
| `BlockChain.MaybePruneToLatestCheckpoint` | `blockchain/chain.go` | Idempotent driver: looks up the checkpoint's owning file number, calls the locator, logs the result.  Takes `chainLock` for writing for the (sub-second) transaction. |
| Triggers | `netsync/manager.go`, `server.go` | Called at IBD-complete (the moment `ibdMode` flips false) and once at startup so a restart with the flag freshly enabled also prunes. |
| Service-flag fix-up | `server.go` | When pruning is on, drop `SFNodeNetwork` so peers correctly classify us as `SFNodeNetworkLimited` and don't ask for blocks we no longer hold. |

### Why this design

Bitcoin Core prunes by size (delete oldest until below target).  That
involves continuous bookkeeping during normal block append and
careful interaction with the spend-journal flush cadence.  We avoid
all of it by pruning *only* at well-defined moments and *only* at
file granularity:

- Files are append-only and homogeneous in policy: blocks below the
  checkpoint can never be needed for reorg (checkpoints forbid
  rewriting them) and are not needed for indexers either (we refuse
  to combine the flag with indexers up front).
- The spend journal never wrote pre-checkpoint stxos in the first
  place — `connectBlock`'s `skipUndo` path already short-circuits
  the write for those heights.  No extra cleanup is required.
- The on-disk effect is unambiguous: file numbers below `cpFileNum`
  vanish; everything else is untouched.  `reconcile` on the next open
  sees the gap and accepts it (BeenPruned reports true).

### Limits / non-goals

- We do not prune block-index rows for the surviving files; the
  index keeps the (header) data for those heights so chain
  verification continues to work end-to-end.
- We do not migrate or rewrite the file containing the checkpoint.
  Blocks in that file are kept even if they are pre-checkpoint, on
  the assumption that the disk savings are not worth a re-encode.
- RPC `getblock` for a pruned block returns the underlying database
  `ErrBlockNotFound`.  No special "pruned" error type yet.

---

## 9. Future work

In approximate order of expected impact:

1. **Async `.fdb` writer**: move the raw-block disk write into a
   write-back queue so `ProcessBlock` doesn't stall on it.
2. **Slab-allocated UTXO entries**: collapse the per-entry `pkScript
   []byte` indirection into an inline-or-slab representation so GC
   mark walks scan fewer pointers.
3. **Size-target prune (`--prune`) running alongside header-only
   archive**: combine Core-style continuous pruning with the
   `--prunetocheckpoint` one-shot for nodes that want only the very
   recent tail.
4. **Pruned-block RPC error type**: surface a distinct error code so
   wallets know to ask a different node for historical data instead
   of treating the response as "block doesn't exist."
