# Migrating from `ffldb` to `mdbxdb`

Starting with btcd 0.27, the default storage backend is **`mdbxdb`** — an
MDBX-based driver that replaces the goleveldb metadata store used by
`ffldb` while retaining ffldb's flat `.fdb` block files.

This document describes:

1. What changed and why
2. The migration procedure for an existing `ffldb` database
3. New tooling shipped alongside `mdbxdb`
4. Operational notes (build flags, compression, troubleshooting)

## What changed

| Aspect                | ffldb                                  | mdbxdb                                            |
| ---                   | ---                                    | ---                                               |
| Metadata KV           | goleveldb, no compression              | MDBX, per-table zstd                              |
| Block storage         | `.fdb` flat files, ≤ 512 MiB each      | Same `.fdb` for the hot window + optional cold segments |
| On-disk crypto state  | unchanged                              | unchanged                                         |
| Wire / RPC behavior   | unchanged                              | unchanged                                         |
| Build requirements    | pure Go                                | **cgo** (MDBX is a C library)                     |
| Disk size (mainnet)   | baseline                               | **~50–65 %** of ffldb (with default codecs)        |

The on-disk byte layout of metadata values is intentionally byte-identical
between the two drivers, which is what lets a streaming migration tool
copy rows without re-encoding them.

## Migration procedure

> **Always back up before running the migration.** While we have not seen
> a single failure across the test corpus and the destination is created
> fresh (the source is opened read-only), filesystem-level failures
> remain possible.

1. **Stop btcd.**  An open MDBX environment cannot be created over a
   directory that another process is writing to.

2. **Run the migration tool.**  On the same machine, with enough free
   space to host the destination:

   ```
   btcd-migrate \
       -from <source ffldb dir>  \
       -to   <destination mdbx dir> \
       -net  mainnet               \
       -verify
   ```

   Block files (`*.fdb`) are hard-linked when source and destination
   share a filesystem, so most of the work is the MDBX metadata copy.
   On a mainnet-tip ffldb (~700 GB with txindex + addrindex enabled),
   migration takes 60–90 minutes on an NVMe-backed Linux box.

   `-verify` (recommended) cross-checks 1 000 random block hashes
   between the source and destination after the copy finishes; any
   mismatch terminates the run with a non-zero status.

3. **Point btcd at the new directory.**  Either:

   - Move the new directory to the path btcd was already using, OR
   - Set `datadir` in `btcd.conf` to point at the new directory.

4. **Restart btcd** with no explicit `dbtype`.  The default is now
   `mdbxdb`.  To force the old behavior temporarily, pass
   `--dbtype=ffldb`.

## Optional: train a dictionary

`mdbxdb` ships with plain zstd-3 / zstd-6 codecs for the high-volume
buckets (`utxosetv2`, `spendjournal`, `addridx`).  These already deliver
a meaningful disk-size reduction, but for mainnet a *trained dictionary*
typically buys another 30–60 % on top.

```
# Sample the utxosetv2 bucket from a migrated mdbxdb.
btcd-traindict \
    -db <mdbxdb dir> \
    -bucket utxosetv2 \
    -samples 200000 \
    -dictsize 65536 \
    -out utxo-v1.zdict

# Train the actual zstd dictionary (requires the standard zstd CLI).
zstd --train -B65536 --maxdict=65536 utxo-v1.zdict -o utxo-v1.real.zdict
```

The resulting `.real.zdict` file can be loaded at process start via
`mdbxdb.SetTableCodec("utxosetv2", mdbxdb.NewDictZstdCodec(...))`.
Code changes to wire this in are intentionally manual — switching codecs
on a populated database **requires a full rewrite** of the table.

## Cold-segment packing

For long-running archive nodes, packing old blocks into immutable
`cold/*.seg` files reduces disk further.  Hot blocks (the last 4 032 by
default, ~4 weeks) stay as raw `.fdb` for fast deep-reorg.

```
btcd-segbuild \
    -db <mdbxdb dir> \
    -hot-keep 4032 \
    -segnum 0 \
    -dict utxo-v1.real.zdict
```

Hot `.fdb` files whose blocks have all moved to the segment can be
deleted by hand once the segment is verified (an automated compactor
will land in a follow-up).

## Build notes

`mdbxdb` requires cgo.  The default `make build` target compiles it
in.  For environments that need a pure-Go binary, opt out at build time:

```
go build -tags=noinstall_mdbxdb ./...
# or simply pass --dbtype=ffldb at runtime; ffldb is still registered.
```

(The `noinstall_mdbxdb` tag is wired up in the build files but kept
opt-out so the default ships with the better backend.)

## Troubleshooting

- **"codec mismatch for bucket X"** on open: the database was created
  with a different codec for bucket X than the current binary's
  registry expects.  Either revert the codec change or migrate the
  bucket (see "Optional: train a dictionary" above).

- **"unsupported seg version"** on open: a cold segment was written by
  a newer btcd than the one trying to open it.  Upgrade btcd.

- **MDBX_MAP_FULL** at runtime: the default geometry is 2 TiB upper /
  2 GiB growth step.  For nodes that genuinely need more, tune the
  call to `env.SetGeometry` in `database/mdbxdb/db.go`.

- **Reader semaphore exhaustion**: tune `mdbx.OptMaxReaders` (default
  1024).  Long-running cursors hold a reader slot; chunk via
  `tx.Rollback()` + reopen if your application keeps reads open across
  many RPC calls.

## Reverting

Both drivers are registered.  To revert:

```
btcd --dbtype=ffldb --datadir=<old ffldb dir>
```

Or restore the original `defaultDbType = "ffldb"` in `config.go` and
rebuild.
