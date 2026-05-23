// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package mdbxdb

import (
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/database"
)

// Mdbxdb_InstallColdBlockIndex is a test-only helper that re-writes the
// block-index rows for the given hashes to point at cold segment `segNum`
// with the supplied decompressed block lengths.  Used by tests that build a
// cold segment by hand and want subsequent FetchBlock calls to route there.
//
// Outside of tests there is no need for this — the offline segbuild tool
// performs the equivalent write transparently.
func Mdbxdb_InstallColdBlockIndex(tx database.Tx, segNum uint32,
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
