// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// migrate.go exposes a controlled "raw open" path used by the offline
// migration tool so the destination database can be populated outside of
// the normal Create/reconcile flow.  Production code uses database.Open /
// database.Create exclusively — this entry point is for ffldb→mdbxdb
// streaming migration only.

package mdbxdb

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/erigontech/mdbx-go/mdbx"
)

// Mdbxdb_OpenForMigration prepares an empty mdbxdb directory for migration:
// it makes the metadata sub-directory, opens the MDBX environment, opens
// the kv DBI, but does NOT call initDB or reconcileDB.  The caller is
// responsible for writing the initial writeLoc / blockIdx bucket index /
// curBucketID rows in addition to its own data, and for closing the
// database when done.  After the next regular Open, reconcile must pass.
//
// If the target directory already contains a metadata/ subdirectory the
// function returns ErrDbExists.
func Mdbxdb_OpenForMigration(dbPath string, network wire.BitcoinNet) (database.DB, error) {
	metadataDir := filepath.Join(dbPath, metadataDirName)
	if fileExists(metadataDir) {
		return nil, makeDbErr(database.ErrDbExists,
			fmt.Sprintf("migration target %q already initialized", metadataDir), nil)
	}
	if err := os.MkdirAll(metadataDir, 0o700); err != nil {
		return nil, makeDbErr(database.ErrDriverSpecific,
			fmt.Sprintf("mkdir %q: %v", metadataDir, err), err)
	}

	env, err := mdbx.NewEnv(mdbx.Label(dbType))
	if err != nil {
		return nil, convertMdbxErr("mdbx.NewEnv", err)
	}
	if err := env.SetOption(uint(mdbx.OptMaxDB), defaultMaxDBI); err != nil {
		env.Close()
		return nil, convertMdbxErr("env.SetOption max_db", err)
	}
	if err := env.SetOption(uint(mdbx.OptMaxReaders), 1024); err != nil {
		env.Close()
		return nil, convertMdbxErr("env.SetOption max_readers", err)
	}
	if err := env.SetGeometry(-1, -1, int(defaultMapSize), defaultGrowthStep,
		-1, defaultPageSize); err != nil {
		env.Close()
		return nil, convertMdbxErr("env.SetGeometry", err)
	}
	openFlags := uint(mdbx.NoReadahead | mdbx.Durable)
	if err := env.Open(metadataDir, openFlags, 0o664); err != nil {
		env.Close()
		return nil, convertMdbxErr("env.Open", err)
	}

	var dbi mdbx.DBI
	if err := env.Update(func(mtxn *mdbx.Txn) error {
		var err error
		dbi, err = mtxn.OpenDBISimple(kvDBIName, mdbx.Create)
		return err
	}); err != nil {
		env.Close()
		return nil, convertMdbxErr("open kv dbi", err)
	}

	store, err := newBlockStore(dbPath, network)
	if err != nil {
		env.Close()
		return nil, err
	}

	pdb := &db{env: env, kvDBI: dbi, store: store}
	return pdb, nil
}

// Mdbxdb_WriteWriteLoc sets the durable .fdb write cursor inside a
// migration transaction.  This must be called once during migration with
// the (fileNum, offset) values copied from the source database; otherwise
// reconcileDB at the next regular Open will detect a mismatch with the
// .fdb files on disk and truncate them.
func Mdbxdb_WriteWriteLoc(tx database.Tx, fileNum, offset uint32) error {
	itx := tx.(*transaction)
	return itx.metaBucket.Put(writeLocKeyName, serializeWriteRow(fileNum, offset))
}

// Mdbxdb_InitMigrationMeta writes the small set of structural rows that
// initDB would normally create: the blockIdx bucket index entry and the
// current-bucket-id counter.  Idempotent.  Call before walking any
// per-bucket data.
func Mdbxdb_InitMigrationMeta(tx database.Tx) error {
	itx := tx.(*transaction)
	if !itx.rawHas(bucketIndexKey(metadataBucketID, blockIdxBucketName)) {
		if err := itx.rawPut(
			bucketIndexKey(metadataBucketID, blockIdxBucketName),
			blockIdxBucketID[:]); err != nil {
			return err
		}
	}
	if !itx.rawHas(curBucketIDKeyName) {
		if err := itx.rawPut(curBucketIDKeyName, blockIdxBucketID[:]); err != nil {
			return err
		}
	}
	// Also persist codec fingerprints so reopen verification succeeds.
	for bucketName, c := range defaultRegistry.entries {
		fpKey := bucketizedKey(metadataBucketID,
			append(append([]byte{}, codecMetaKeyPrefix...),
				[]byte(bucketName)...))
		if !itx.rawHas(fpKey) {
			if err := itx.rawPut(fpKey, []byte(c.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}
