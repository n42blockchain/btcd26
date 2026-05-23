// Copyright (c) 2015-2016 The btcsuite developers
// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package mdbxdb

import (
	"fmt"
	"hash/crc32"

	"github.com/btcsuite/btcd/database"
)

// The serialized write cursor location format is:
//
//  [0:4]  Block file (4 bytes)
//  [4:8]  File offset (4 bytes)
//  [8:12] Castagnoli CRC-32 checksum (4 bytes)

func serializeWriteRow(curBlockFileNum, curFileOffset uint32) []byte {
	var serializedRow [12]byte
	byteOrder.PutUint32(serializedRow[0:4], curBlockFileNum)
	byteOrder.PutUint32(serializedRow[4:8], curFileOffset)
	checksum := crc32.Checksum(serializedRow[:8], castagnoli)
	byteOrder.PutUint32(serializedRow[8:12], checksum)
	return serializedRow[:]
}

func deserializeWriteRow(writeRow []byte) (uint32, uint32, error) {
	gotChecksum := crc32.Checksum(writeRow[:8], castagnoli)
	wantChecksum := byteOrder.Uint32(writeRow[8:12])
	if gotChecksum != wantChecksum {
		str := fmt.Sprintf("metadata for write cursor does not match the "+
			"expected checksum - got %d, want %d", gotChecksum, wantChecksum)
		return 0, 0, makeDbErr(database.ErrCorruption, str, nil)
	}
	return byteOrder.Uint32(writeRow[0:4]), byteOrder.Uint32(writeRow[4:8]), nil
}

// reconcileDB reconciles the metadata stored in MDBX with the flat block files
// on disk.  It also initializes the MDBX environment on first create.
func reconcileDB(pdb *db, create bool) (database.DB, error) {
	if create {
		if err := initDB(pdb); err != nil {
			return nil, err
		}
	}

	var curFileNum, curOffset uint32
	err := pdb.View(func(tx database.Tx) error {
		writeRow := tx.Metadata().Get(writeLocKeyName)
		if writeRow == nil {
			return makeDbErr(database.ErrCorruption,
				"write cursor does not exist", nil)
		}
		var err error
		curFileNum, curOffset, err = deserializeWriteRow(writeRow)
		return err
	})
	if err != nil {
		return nil, err
	}

	wc := pdb.store.writeCursor
	if wc.curFileNum > curFileNum || (wc.curFileNum == curFileNum &&
		wc.curOffset > curOffset) {

		log.Info("Detected unclean shutdown - Repairing...")
		log.Debugf("Metadata claims file %d, offset %d. Block data is at "+
			"file %d, offset %d", curFileNum, curOffset,
			wc.curFileNum, wc.curOffset)
		pdb.store.handleRollback(curFileNum, curOffset)
		log.Infof("Database sync complete")
	}

	if wc.curFileNum < curFileNum || (wc.curFileNum == curFileNum &&
		wc.curOffset < curOffset) {

		str := fmt.Sprintf("metadata claims file %d, offset %d, but block "+
			"data is at file %d, offset %d", curFileNum, curOffset,
			wc.curFileNum, wc.curOffset)
		log.Warnf("***Database corruption detected***: %v", str)
		return nil, makeDbErr(database.ErrCorruption, str, nil)
	}

	return pdb, nil
}
