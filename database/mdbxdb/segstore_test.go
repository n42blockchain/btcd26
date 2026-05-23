// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package mdbxdb_test

import (
	"bytes"
	"crypto/sha256"
	"path/filepath"
	"testing"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/database/mdbxdb"
)

// TestSegBuilderRoundTrip builds a cold segment from a synthetic set of
// blocks, opens it through a fresh segReader path, and verifies every block
// reads back byte-exact.
func TestSegBuilderRoundTrip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()

	// Make 256 random-but-deterministic blocks of varying sizes.
	const n = 256
	blocks := make([][]byte, n)
	hashes := make([]chainhash.Hash, n)
	for i := 0; i < n; i++ {
		sz := 256 + (i*37)%4096
		b := make([]byte, sz)
		// Repetitive content with low entropy so zstd actually does
		// something visible.
		filler := []byte{byte(i), byte(i ^ 0x5a)}
		for off := 0; off < sz; off++ {
			b[off] = filler[off%2]
		}
		blocks[i] = b
		h := sha256.Sum256(b)
		copy(hashes[i][:], h[:])
	}

	// Build a tiny segment with no dictionary — the seg format supports
	// optional dicts but the round-trip test only needs to confirm the
	// pipeline.  Production builds via `zstd --train` and passes the
	// result here.
	var dict []byte
	builder := mdbxdb.NewSegBuilder(0, 0xd9b4bef9, dict, 64*1024)
	for i := range blocks {
		builder.AddBlock(&hashes[i], blocks[i])
	}
	segNum, err := builder.Finalize(dir)
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if segNum != 0 {
		t.Fatalf("segNum=%d want 0", segNum)
	}

	// Open the .seg via the internal segPool by going through a freshly
	// created mdbxdb — easiest way to obtain a segPool tied to the same
	// basePath.
	db, err := database.Create("mdbxdb", filepath.Join(dir, "thedb"), blockDataNet)
	if err != nil {
		t.Fatalf("create db: %v", err)
	}
	defer db.Close()

	// We'll inject the cold seg path into the test db by symlinking — no,
	// simpler: re-finalize into the db's own dir.
	builder2 := mdbxdb.NewSegBuilder(0, 0xd9b4bef9, dict, 64*1024)
	for i := range blocks {
		builder2.AddBlock(&hashes[i], blocks[i])
	}
	if _, err := builder2.Finalize(filepath.Join(dir, "thedb")); err != nil {
		t.Fatalf("finalize2: %v", err)
	}

	// Write cold-block-index rows into MDBX so the database routes
	// FetchBlock to the segment.
	if err := db.Update(func(tx database.Tx) error {
		return mdbxdb.Mdbxdb_InstallColdBlockIndex(tx, 0, hashes, blockLens(blocks))
	}); err != nil {
		t.Fatalf("install cold index: %v", err)
	}

	// Now FetchBlock through the public database.Tx interface; each block
	// must come back byte-exact.
	if err := db.View(func(tx database.Tx) error {
		for i := range hashes {
			got, err := tx.FetchBlock(&hashes[i])
			if err != nil {
				return err
			}
			if !bytes.Equal(got, blocks[i]) {
				t.Fatalf("block %d round-trip mismatch (got len=%d, want len=%d)",
					i, len(got), len(blocks[i]))
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}

	// Also test FetchBlockRegion against a cold block.
	if err := db.View(func(tx database.Tx) error {
		region := &database.BlockRegion{
			Hash:   &hashes[5],
			Offset: 10,
			Len:    32,
		}
		got, err := tx.FetchBlockRegion(region)
		if err != nil {
			return err
		}
		want := blocks[5][10:42]
		if !bytes.Equal(got, want) {
			t.Fatalf("cold region mismatch")
		}
		return nil
	}); err != nil {
		t.Fatalf("view region: %v", err)
	}
}

func blockLens(blocks [][]byte) []uint32 {
	out := make([]uint32, len(blocks))
	for i, b := range blocks {
		out[i] = uint32(len(b))
	}
	return out
}
