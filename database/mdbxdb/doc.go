// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

/*
Package mdbxdb implements a driver for the database package that uses an MDBX
environment for metadata storage and flat block files for raw block bytes.

The driver is named "mdbxdb" and is intended to replace the legacy "ffldb"
driver.  Like ffldb it stores raw block bytes in 512 MiB flat ".fdb" files;
unlike ffldb it stores all metadata (block index, utxo set, indexer state,
etc.) inside a single MDBX environment.

Bucket virtualization uses the exact same scheme as ffldb: every metadata key
is prefixed by a 4-byte big-endian bucket ID, and the bucket index lives at
keys prefixed with "bidx".  This makes the on-disk representation of metadata
keys byte-identical between drivers, which is what allows a streaming
ffldb→mdbxdb migration tool (introduced in a later phase) to copy entries
without re-encoding them.

This package requires cgo: MDBX itself is a C library bundled by
github.com/erigontech/mdbx-go.
*/
package mdbxdb
