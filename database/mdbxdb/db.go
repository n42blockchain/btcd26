// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package mdbxdb

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/erigontech/mdbx-go/mdbx"
)

const (
	// metadataDirName is the subdirectory inside the database root that
	// holds the MDBX environment (mdbx.dat + lock file).
	metadataDirName = "metadata"

	// kvDBIName is the name of the single MDBX named database that holds
	// every metadata key.  All bucket virtualization happens via key
	// prefixes the way ffldb does, which keeps the on-disk byte layout
	// of values byte-identical to ffldb.
	kvDBIName = "kv"
)

// Tunable defaults.  These mirror the conservative values that erigon-style
// MDBX users settle on.  They can later be promoted to opts but for P0 the
// defaults are baked in.
const (
	defaultPageSize        = 8 * 1024
	defaultMapSize   int64 = 2 * 1024 * 1024 * 1024 * 1024 // 2 TiB upper limit
	defaultGrowthStep      = 2 * 1024 * 1024 * 1024        // 2 GiB
	defaultMaxDBI    uint64 = 32
)

var (
	bucketIndexPrefix = []byte("bidx")

	curBucketIDKeyName = []byte("bidx-cbid")

	metadataBucketID = [4]byte{}

	blockIdxBucketID = [4]byte{0x00, 0x00, 0x00, 0x01}

	blockIdxBucketName = []byte("ffldb-blockidx")

	writeLocKeyName = []byte("ffldb-writeloc")

	// codecMetaKeyPrefix is the prefix used for per-table codec
	// fingerprint keys, stored inside the root metadata bucket.  The full
	// key for a given table is codecMetaKeyPrefix + bucketName.  The
	// stored value is the Codec.Name() at the time the database was
	// created.  On subsequent opens we re-derive the name from the active
	// registry and refuse to open if they differ — this catches the case
	// where someone swaps in a different codec without explicit migration.
	codecMetaKeyPrefix = []byte("\x00mdbxcodec:")
)

// db is the database.DB implementation for mdbxdb.
type db struct {
	closeLock sync.RWMutex
	writeLock sync.Mutex
	closed    bool

	env     *mdbx.Env
	kvDBI   mdbx.DBI
	dbiOnce sync.Once
	dbiErr  error

	store *blockStore
}

var _ database.DB = (*db)(nil)

// Type returns the database driver type the current database instance was
// created with.
func (db *db) Type() string { return dbType }

// Begin starts a transaction.  See ffldb's documentation for the contract.
func (db *db) Begin(writable bool) (database.Tx, error) {
	return db.begin(writable)
}

// begin opens an MDBX transaction matching writable and returns the wrapper.
//
// Per mdbx-go's contract, write transactions must run on a goroutine that
// has been locked to its OS thread.  We LockOSThread here and unlock it in
// transaction.close().  Callers using Begin(true) MUST Commit or Rollback on
// the same goroutine, just as ffldb required them not to switch goroutines.
func (db *db) begin(writable bool) (*transaction, error) {
	if writable {
		db.writeLock.Lock()
	}
	db.closeLock.RLock()

	if db.closed {
		db.closeLock.RUnlock()
		if writable {
			db.writeLock.Unlock()
		}
		return nil, makeDbErr(database.ErrDbNotOpen, errDbNotOpenStr, nil)
	}

	if writable {
		runtime.LockOSThread()
	}

	var flags uint = mdbx.Readonly
	if writable {
		flags = 0
	}
	mtxn, err := db.env.BeginTxn(nil, flags)
	if err != nil {
		if writable {
			runtime.UnlockOSThread()
		}
		db.closeLock.RUnlock()
		if writable {
			db.writeLock.Unlock()
		}
		return nil, convertMdbxErr("begin transaction", err)
	}

	tx := &transaction{
		db:       db,
		mtxn:     mtxn,
		writable: writable,
	}
	tx.metaBucket = &bucket{tx: tx, id: metadataBucketID,
		path: [][]byte{}}
	tx.blockIdxBucket = &bucket{tx: tx, id: blockIdxBucketID,
		path: [][]byte{blockIdxBucketName}}
	return tx, nil
}

// View invokes the passed function inside a managed read-only transaction.
func (db *db) View(fn func(database.Tx) error) error {
	tx, err := db.begin(false)
	if err != nil {
		return err
	}
	defer rollbackOnPanic(tx)

	tx.managed = true
	err = fn(tx)
	tx.managed = false
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Rollback()
}

// Update invokes the passed function inside a managed read-write transaction.
func (db *db) Update(fn func(database.Tx) error) error {
	tx, err := db.begin(true)
	if err != nil {
		return err
	}
	defer rollbackOnPanic(tx)

	tx.managed = true
	err = fn(tx)
	tx.managed = false
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Close blocks until all transactions finish, then shuts down MDBX and any
// open .fdb file handles.
func (db *db) Close() error {
	db.closeLock.Lock()
	defer db.closeLock.Unlock()

	if db.closed {
		return makeDbErr(database.ErrDbNotOpen, errDbNotOpenStr, nil)
	}
	db.closed = true

	if db.env != nil {
		db.env.Close()
		db.env = nil
	}

	wc := db.store.writeCursor
	if wc.curFile.file != nil {
		_ = wc.curFile.file.Close()
		wc.curFile.file = nil
	}
	for _, blockFile := range db.store.openBlockFiles {
		_ = blockFile.file.Close()
	}
	db.store.openBlockFiles = nil
	db.store.openBlocksLRU.Init()
	db.store.fileNumToLRUElem = nil
	if db.store.segs != nil {
		db.store.segs.closeAll()
	}
	return nil
}

// rollbackOnPanic rolls back the passed transaction if the calling code
// panics, so the database is left in a usable state.
func rollbackOnPanic(tx *transaction) {
	if err := recover(); err != nil {
		tx.managed = false
		_ = tx.Rollback()
		panic(err)
	}
}

// fileExists reports whether the named file or directory exists.
func fileExists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// openDB opens or creates the database at dbPath.  database.ErrDbDoesNotExist
// is returned if the database doesn't exist and the create flag is not set.
func openDB(dbPath string, network wire.BitcoinNet, create bool) (database.DB, error) {
	metadataDir := filepath.Join(dbPath, metadataDirName)
	dbExists := fileExists(metadataDir)
	if !create && !dbExists {
		str := fmt.Sprintf("database %q does not exist", metadataDir)
		return nil, makeDbErr(database.ErrDbDoesNotExist, str, nil)
	}
	if create && dbExists {
		str := fmt.Sprintf("database %q already exists", metadataDir)
		return nil, makeDbErr(database.ErrDbExists, str, nil)
	}

	if err := os.MkdirAll(metadataDir, 0o700); err != nil {
		return nil, makeDbErr(database.ErrDriverSpecific,
			fmt.Sprintf("failed to create mdbx dir %q: %v", metadataDir, err), err)
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

	// Default to fully durable commits.  IBD users who want to trade
	// crash-resilience-of-the-last-few-seconds for throughput can set
	// BTCD_MDBX_FAST_SYNC=1 to use SafeNoSync + WriteMap.  Reconcile on
	// the next open truncates any .fdb data past the last MDBX writeLoc,
	// so the worst case after a hard crash is re-fetching the tail of
	// the chain — no corruption.  Optimal for fresh sync; switch back
	// (unset the env var) once IBD is done.
	openFlags := uint(mdbx.NoReadahead | mdbx.Durable)
	// FAST_SYNC variants:
	//   "1"        → SafeNoSync + NoMetaSync (skip fsync only)
	//   "writemap" → also enable WriteMap (mmap-based writes; can
	//                be slower than syscall writes on Windows with a
	//                large declared map size — only use after benchmarking)
	if v := os.Getenv("BTCD_MDBX_FAST_SYNC"); v != "" && v != "0" {
		openFlags = uint(mdbx.NoReadahead | mdbx.SafeNoSync |
			mdbx.NoMetaSync)
		mode := "SafeNoSync|NoMetaSync"
		if v == "writemap" {
			openFlags |= uint(mdbx.WriteMap)
			mode += "|WriteMap"
		}
		log.Warnf("MDBX opened in FAST_SYNC mode (%s) — a hard "+
			"crash may lose the last few seconds of commits; "+
			"chain reconcile will recover at next start", mode)
	}
	if err := env.Open(metadataDir, openFlags, 0o664); err != nil {
		env.Close()
		return nil, convertMdbxErr("env.Open", err)
	}

	// Open or create the single kv DBI used for everything.
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

	out, err := reconcileDB(pdb, create)
	if err != nil {
		_ = pdb.Close()
		return nil, err
	}
	if err := pdb.verifyCodecs(); err != nil {
		_ = pdb.Close()
		return nil, err
	}
	return out, nil
}

// initDB creates the initial bucket index entries, the writeLoc cursor row,
// and the codec-name fingerprints for every registered table — all inside a
// single MDBX transaction.
func initDB(pdb *db) error {
	return pdb.env.Update(func(mtxn *mdbx.Txn) error {
		// writeLoc starts at file 0 offset 0.
		if err := mtxn.Put(pdb.kvDBI,
			bucketizedKey(metadataBucketID, writeLocKeyName),
			serializeWriteRow(0, 0), 0); err != nil {
			return err
		}
		// Bucket index entry for the internal block-index bucket.
		if err := mtxn.Put(pdb.kvDBI,
			bucketIndexKey(metadataBucketID, blockIdxBucketName),
			blockIdxBucketID[:], 0); err != nil {
			return err
		}
		// Current highest used bucket ID.
		if err := mtxn.Put(pdb.kvDBI, curBucketIDKeyName,
			blockIdxBucketID[:], 0); err != nil {
			return err
		}
		// Persist the codec fingerprint for every table the registry
		// currently knows about.  On reopen we'll cross-check these.
		for bucketName, c := range defaultRegistry.entries {
			fpKey := bucketizedKey(metadataBucketID,
				append(append([]byte{}, codecMetaKeyPrefix...),
					[]byte(bucketName)...))
			if err := mtxn.Put(pdb.kvDBI, fpKey,
				[]byte(c.Name()), 0); err != nil {
				return err
			}
		}
		return nil
	})
}

// verifyCodecs checks that every persisted codec fingerprint matches the one
// currently in the active registry.  Mismatch returns ErrInvalid so the
// caller does not silently corrupt data with the wrong codec.
//
// New tables introduced after the database was created are written into the
// fingerprint table on this open so subsequent opens see them.
func (pdb *db) verifyCodecs() error {
	return pdb.env.Update(func(mtxn *mdbx.Txn) error {
		// Phase 1: read every existing fingerprint and verify it.
		c, err := mtxn.OpenCursor(pdb.kvDBI)
		if err != nil {
			return err
		}
		// Scope: keys under the metadata bucket whose suffix starts
		// with codecMetaKeyPrefix.
		scopePrefix := append(append([]byte{}, metadataBucketID[:]...),
			codecMetaKeyPrefix...)
		found := make(map[string]string)
		k, v, err := c.Get(scopePrefix, nil, mdbx.SetRange)
		for err == nil {
			if !bytesHasPrefix(k, scopePrefix) {
				break
			}
			bucketName := string(k[len(scopePrefix):])
			found[bucketName] = string(v)
			k, v, err = c.Get(nil, nil, mdbx.Next)
		}
		c.Close()

		for bucketName, storedName := range found {
			activeCodec := defaultRegistry.entries[bucketName]
			activeName := "raw"
			if activeCodec != nil {
				activeName = activeCodec.Name()
			}
			if storedName != activeName {
				return makeDbErr(database.ErrInvalid, fmt.Sprintf(
					"codec mismatch for bucket %q: db was created "+
						"with %q, current registry has %q",
					bucketName, storedName, activeName), nil)
			}
		}

		// Phase 2: persist fingerprints for any table the registry has
		// added since this database was created.
		for bucketName, codec := range defaultRegistry.entries {
			if _, ok := found[bucketName]; ok {
				continue
			}
			fpKey := bucketizedKey(metadataBucketID,
				append(append([]byte{}, codecMetaKeyPrefix...),
					[]byte(bucketName)...))
			if err := mtxn.Put(pdb.kvDBI, fpKey,
				[]byte(codec.Name()), 0); err != nil {
				return err
			}
		}
		return nil
	})
}

// bytesHasPrefix is a local helper to avoid pulling in bytes into db.go.
func bytesHasPrefix(s, prefix []byte) bool {
	if len(s) < len(prefix) {
		return false
	}
	for i := range prefix {
		if s[i] != prefix[i] {
			return false
		}
	}
	return true
}

