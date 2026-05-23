// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package mdbxdb

import (
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/database"
)

// Mdbxdb_WalkBlockIndex iterates every row of the internal block-index
// bucket inside the given transaction and invokes fn for each one.  The
// raw fileNum/offset/length is exposed unchanged from the on-disk
// representation; callers wanting to filter cold-vs-hot rows can check the
// high bit of fileNum (set => cold segment).
//
// Intended for offline maintenance tools (segbuild, migrate, verify); the
// daemon code itself never walks the block index directly.
func Mdbxdb_WalkBlockIndex(tx database.Tx,
	fn func(hash chainhash.Hash, fileNum, offset, length uint32) error) error {

	itx := tx.(*transaction)
	cursor := itx.blockIdxBucket.Cursor()
	for ok := cursor.First(); ok; ok = cursor.Next() {
		var h chainhash.Hash
		copy(h[:], cursor.Key())
		loc := deserializeBlockLoc(cursor.Value())
		if err := fn(h, loc.blockFileNum, loc.fileOffset, loc.blockLen); err != nil {
			return err
		}
	}
	return nil
}

// Mdbxdb_InstallColdBlockIndexProd is the production-callable counterpart
// of the test-only Mdbxdb_InstallColdBlockIndex helper.  It re-writes
// block-index rows for hashes so that subsequent FetchBlock calls route
// through cold segment `segNum`.  The supplied blockLens are the
// decompressed lengths of each block (used by FetchBlockRegion bounds
// checks).
//
// Intended for offline segment-builder tools.
func Mdbxdb_InstallColdBlockIndexProd(tx database.Tx, segNum uint32,
	hashes []chainhash.Hash, blockLens []uint32) error {

	itx := tx.(*transaction)
	for i := range hashes {
		loc := makeColdLocation(segNum, blockLens[i])
		row := serializeBlockLoc(loc)
		if err := itx.blockIdxBucket.Put(hashes[i][:], row); err != nil {
			return err
		}
	}
	return nil
}
