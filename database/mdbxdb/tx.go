// Copyright (c) 2015-2016 The btcsuite developers
// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package mdbxdb

import (
	"errors"
	"fmt"
	"runtime"
	"sort"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/database"
	"github.com/erigontech/mdbx-go/mdbx"
)

// pendingBlock houses a block whose bytes still need to be appended to a flat
// block file when the transaction is committed.
type pendingBlock struct {
	hash  *chainhash.Hash
	bytes []byte
}

// transaction implements database.Tx on top of an MDBX *Txn.  Writes within
// the txn are visible to subsequent reads because MDBX naturally exposes a
// txn's own pending mutations.
type transaction struct {
	db       *db
	mtxn     *mdbx.Txn
	writable bool
	closed   bool
	managed  bool

	metaBucket     *bucket
	blockIdxBucket *bucket

	// Blocks pending append to disk; flushed on Commit.  Map is keyed by
	// hash for fast HasBlock lookups during the same txn.
	pendingBlocks    map[chainhash.Hash]int
	pendingBlockData []pendingBlock

	// File numbers slated for deletion on commit (prune support).
	pendingDelFileNums []uint32
}

var _ database.Tx = (*transaction)(nil)

// checkClosed returns ErrTxClosed if the transaction is no longer usable.
func (tx *transaction) checkClosed() error {
	if tx.closed {
		return makeDbErr(database.ErrTxClosed, errTxClosedStr, nil)
	}
	return nil
}

// rawGet returns the raw bytes for key from MDBX inside the current txn,
// or nil if the key is absent.  The returned slice is a *copy* that survives
// past cursor advancements within the txn (mdbx-go's Txn.Get returns memory
// scoped to the next Get call).
func (tx *transaction) rawGet(key []byte) []byte {
	v, err := tx.mtxn.Get(tx.db.kvDBI, key)
	if err != nil {
		if errors.Is(err, mdbx.ErrNotFound) || mdbx.IsNotFound(err) {
			return nil
		}
		log.Errorf("mdbx Get failed: %v", err)
		return nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out
}

// rawHas returns whether key exists.  Cheaper than rawGet because we don't
// need to copy the value, but we still pay one MDBX lookup.
func (tx *transaction) rawHas(key []byte) bool {
	_, err := tx.mtxn.Get(tx.db.kvDBI, key)
	if err == nil {
		return true
	}
	if errors.Is(err, mdbx.ErrNotFound) || mdbx.IsNotFound(err) {
		return false
	}
	log.Errorf("mdbx Get failed: %v", err)
	return false
}

// rawPut writes a key/value pair.  Must only be called on a writable txn.
func (tx *transaction) rawPut(key, value []byte) error {
	if err := tx.mtxn.Put(tx.db.kvDBI, key, value, 0); err != nil {
		return convertMdbxErr("mdbx Put", err)
	}
	return nil
}

// rawDelete removes a key.  Missing keys are silently ignored (matching the
// database.Bucket.Delete contract).
func (tx *transaction) rawDelete(key []byte) error {
	if err := tx.mtxn.Del(tx.db.kvDBI, key, nil); err != nil {
		if errors.Is(err, mdbx.ErrNotFound) || mdbx.IsNotFound(err) {
			return nil
		}
		return convertMdbxErr("mdbx Del", err)
	}
	return nil
}

// deletePrefix removes every key/value pair whose key has the given prefix.
// Used by DeleteBucket to wipe bucket contents.
func (tx *transaction) deletePrefix(prefix []byte) error {
	c, err := tx.newRawCursor()
	if err != nil {
		return err
	}
	defer c.close()

	for ok := c.seekPrefix(prefix); ok; ok = c.nextWithPrefix(prefix) {
		if err := c.del(); err != nil {
			return err
		}
	}
	return nil
}

// Metadata returns the root metadata bucket.
func (tx *transaction) Metadata() database.Bucket { return tx.metaBucket }

// hasBlock checks both pending blocks and the block index.
func (tx *transaction) hasBlock(hash *chainhash.Hash) bool {
	if _, ok := tx.pendingBlocks[*hash]; ok {
		return true
	}
	return tx.rawHas(bucketizedKey(blockIdxBucketID, hash[:]))
}

// StoreBlock queues a block for write at commit time.
func (tx *transaction) StoreBlock(block *btcutil.Block) error {
	if err := tx.checkClosed(); err != nil {
		return err
	}
	if !tx.writable {
		return makeDbErr(database.ErrTxNotWritable,
			"store block requires a writable database transaction", nil)
	}

	blockHash := block.Hash()
	if tx.hasBlock(blockHash) {
		return makeDbErr(database.ErrBlockExists,
			fmt.Sprintf("block %s already exists", blockHash), nil)
	}
	blockBytes, err := block.Bytes()
	if err != nil {
		return makeDbErr(database.ErrDriverSpecific,
			fmt.Sprintf("failed to get serialized bytes for block %s",
				blockHash), err)
	}

	if tx.pendingBlocks == nil {
		tx.pendingBlocks = make(map[chainhash.Hash]int)
	}
	tx.pendingBlocks[*blockHash] = len(tx.pendingBlockData)
	tx.pendingBlockData = append(tx.pendingBlockData, pendingBlock{
		hash: blockHash, bytes: blockBytes,
	})
	log.Tracef("Added block %s to pending blocks", blockHash)
	return nil
}

func (tx *transaction) HasBlock(hash *chainhash.Hash) (bool, error) {
	if err := tx.checkClosed(); err != nil {
		return false, err
	}
	return tx.hasBlock(hash), nil
}

func (tx *transaction) HasBlocks(hashes []chainhash.Hash) ([]bool, error) {
	if err := tx.checkClosed(); err != nil {
		return nil, err
	}
	out := make([]bool, len(hashes))
	for i := range hashes {
		out[i] = tx.hasBlock(&hashes[i])
	}
	return out, nil
}

func (tx *transaction) fetchBlockRow(hash *chainhash.Hash) ([]byte, error) {
	row := tx.blockIdxBucket.Get(hash[:])
	if row == nil {
		return nil, makeDbErr(database.ErrBlockNotFound,
			fmt.Sprintf("block %s does not exist", hash), nil)
	}
	return row, nil
}

func (tx *transaction) FetchBlockHeader(hash *chainhash.Hash) ([]byte, error) {
	const headerSize = 80 // wire.MaxBlockHeaderPayload
	return tx.FetchBlockRegion(&database.BlockRegion{
		Hash: hash, Offset: 0, Len: headerSize,
	})
}

func (tx *transaction) FetchBlockHeaders(hashes []chainhash.Hash) ([][]byte, error) {
	const headerSize = 80
	regions := make([]database.BlockRegion, len(hashes))
	for i := range hashes {
		regions[i].Hash = &hashes[i]
		regions[i].Offset = 0
		regions[i].Len = headerSize
	}
	return tx.FetchBlockRegions(regions)
}

func (tx *transaction) FetchBlock(hash *chainhash.Hash) ([]byte, error) {
	if err := tx.checkClosed(); err != nil {
		return nil, err
	}
	if idx, ok := tx.pendingBlocks[*hash]; ok {
		return tx.pendingBlockData[idx].bytes, nil
	}
	row, err := tx.fetchBlockRow(hash)
	if err != nil {
		return nil, err
	}
	loc := deserializeBlockLoc(row)
	return tx.db.store.readBlock(hash, loc)
}

func (tx *transaction) FetchBlocks(hashes []chainhash.Hash) ([][]byte, error) {
	if err := tx.checkClosed(); err != nil {
		return nil, err
	}
	out := make([][]byte, len(hashes))
	for i := range hashes {
		var err error
		out[i], err = tx.FetchBlock(&hashes[i])
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// fetchPendingRegion returns the requested region from a pending block, or
// (nil, nil) if the block isn't pending.  Region bounds are validated.
func (tx *transaction) fetchPendingRegion(region *database.BlockRegion) ([]byte, error) {
	idx, ok := tx.pendingBlocks[*region.Hash]
	if !ok {
		return nil, nil
	}
	blockBytes := tx.pendingBlockData[idx].bytes
	blockLen := uint32(len(blockBytes))
	endOffset := region.Offset + region.Len
	if endOffset < region.Offset || endOffset > blockLen {
		return nil, makeDbErr(database.ErrBlockRegionInvalid,
			fmt.Sprintf("block %s region offset %d, length %d exceeds "+
				"block length of %d", region.Hash, region.Offset,
				region.Len, blockLen), nil)
	}
	return blockBytes[region.Offset:endOffset:endOffset], nil
}

func (tx *transaction) FetchBlockRegion(region *database.BlockRegion) ([]byte, error) {
	if err := tx.checkClosed(); err != nil {
		return nil, err
	}
	if tx.pendingBlocks != nil {
		bs, err := tx.fetchPendingRegion(region)
		if err != nil {
			return nil, err
		}
		if bs != nil {
			return bs, nil
		}
	}
	row, err := tx.fetchBlockRow(region.Hash)
	if err != nil {
		return nil, err
	}
	loc := deserializeBlockLoc(row)
	endOffset := region.Offset + region.Len
	if endOffset < region.Offset || endOffset > loc.blockLen {
		return nil, makeDbErr(database.ErrBlockRegionInvalid,
			fmt.Sprintf("block %s region offset %d, length %d exceeds "+
				"block length of %d", region.Hash, region.Offset,
				region.Len, loc.blockLen), nil)
	}
	if isColdLocation(loc) {
		return tx.db.store.readBlockColdRegion(region.Hash, loc,
			region.Offset, region.Len)
	}
	return tx.db.store.readBlockRegion(loc, region.Offset, region.Len)
}

// bulkFetchData groups together a location and the original index so that
// reads can be sorted by (file, offset) to reduce random IO.
type bulkFetchData struct {
	*blockLocation
	replyIndex int
}

type bulkFetchDataSorter []bulkFetchData

func (s bulkFetchDataSorter) Len() int      { return len(s) }
func (s bulkFetchDataSorter) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s bulkFetchDataSorter) Less(i, j int) bool {
	if s[i].blockFileNum != s[j].blockFileNum {
		return s[i].blockFileNum < s[j].blockFileNum
	}
	return s[i].fileOffset < s[j].fileOffset
}

func (tx *transaction) FetchBlockRegions(regions []database.BlockRegion) ([][]byte, error) {
	if err := tx.checkClosed(); err != nil {
		return nil, err
	}

	out := make([][]byte, len(regions))
	fetchList := make([]bulkFetchData, 0, len(regions))
	for i := range regions {
		region := &regions[i]

		if tx.pendingBlocks != nil {
			bs, err := tx.fetchPendingRegion(region)
			if err != nil {
				return nil, err
			}
			if bs != nil {
				out[i] = bs
				continue
			}
		}
		row, err := tx.fetchBlockRow(region.Hash)
		if err != nil {
			return nil, err
		}
		loc := deserializeBlockLoc(row)
		endOffset := region.Offset + region.Len
		if endOffset < region.Offset || endOffset > loc.blockLen {
			return nil, makeDbErr(database.ErrBlockRegionInvalid,
				fmt.Sprintf("block %s region offset %d, length %d "+
					"exceeds block length of %d", region.Hash,
					region.Offset, region.Len, loc.blockLen), nil)
		}
		fetchList = append(fetchList, bulkFetchData{&loc, i})
	}
	sort.Sort(bulkFetchDataSorter(fetchList))

	for i := range fetchList {
		fd := &fetchList[i]
		region := &regions[fd.replyIndex]
		var bs []byte
		var err error
		if isColdLocation(*fd.blockLocation) {
			bs, err = tx.db.store.readBlockColdRegion(region.Hash,
				*fd.blockLocation, region.Offset, region.Len)
		} else {
			bs, err = tx.db.store.readBlockRegion(*fd.blockLocation,
				region.Offset, region.Len)
		}
		if err != nil {
			return nil, err
		}
		out[fd.replyIndex] = bs
	}
	return out, nil
}

// PruneBlocks deletes block files until the on-disk footprint drops below
// targetSize bytes.  Block-index rows for hashes inside the deleted files
// are dropped from MDBX in the same transaction.
func (tx *transaction) PruneBlocks(targetSize uint64) ([]chainhash.Hash, error) {
	if err := tx.checkClosed(); err != nil {
		return nil, err
	}
	if !tx.writable {
		return nil, makeDbErr(database.ErrTxNotWritable,
			"prune blocks requires a writable database transaction", nil)
	}

	maxSize := uint64(tx.db.store.maxBlockFileSize)
	if targetSize < maxSize {
		return nil, fmt.Errorf("got target size of %d but it must be greater "+
			"than %d, the max size of a single block file",
			targetSize, maxSize)
	}

	first, last, lastFileSize, err := scanBlockFiles(tx.db.store.basePath)
	if err != nil {
		return nil, err
	}
	if first == last {
		return nil, nil
	}

	maxSizeFileCount := uint32(last - first)
	totalSize := uint64(lastFileSize) + (maxSize * uint64(maxSizeFileCount))
	if totalSize <= targetSize {
		return nil, nil
	}

	log.Tracef("Using %d more bytes than the target of %d MiB. Pruning files...",
		totalSize-targetSize, targetSize/(1024*1024))

	deletedFiles := make(map[uint32]struct{})
	for i := uint32(first); i < uint32(last); i++ {
		if tx.pendingDelFileNums == nil {
			tx.pendingDelFileNums = make([]uint32, 0, 1)
		}
		tx.pendingDelFileNums = append(tx.pendingDelFileNums, i)
		deletedFiles[i] = struct{}{}
		totalSize -= maxSize
		if totalSize <= targetSize {
			break
		}
	}

	var deletedBlockHashes []chainhash.Hash
	cursor := tx.blockIdxBucket.Cursor()
	for ok := cursor.First(); ok; ok = cursor.Next() {
		loc := deserializeBlockLoc(cursor.Value())
		if _, found := deletedFiles[loc.blockFileNum]; !found {
			continue
		}
		var h chainhash.Hash
		copy(h[:], cursor.Key())
		deletedBlockHashes = append(deletedBlockHashes, h)
		if err := cursor.Delete(); err != nil {
			return nil, err
		}
	}
	log.Tracef("Finished pruning. Database now at %d bytes", totalSize)
	return deletedBlockHashes, nil
}

// BeenPruned returns whether the block storage has ever been pruned.
func (tx *transaction) BeenPruned() (bool, error) {
	first, last, _, err := scanBlockFiles(tx.db.store.basePath)
	if err != nil {
		return false, err
	}
	return first != 0 && (first != last), nil
}

// close releases all resources held by the txn.  Safe to call multiple times.
func (tx *transaction) close() {
	if tx.closed {
		return
	}
	tx.closed = true
	tx.pendingBlocks = nil
	tx.pendingBlockData = nil
	tx.pendingDelFileNums = nil
	if tx.mtxn != nil {
		tx.mtxn.Abort()
		tx.mtxn = nil
	}
	tx.db.closeLock.RUnlock()
	if tx.writable {
		runtime.UnlockOSThread()
		tx.db.writeLock.Unlock()
	}
}

// writePendingAndCommit flushes pending block-file mutations and commits the
// MDBX transaction.  On any failure the block files are rolled back and the
// MDBX transaction is aborted.
func (tx *transaction) writePendingAndCommit() error {
	// Delete files slated for pruning first (they can't be undone, so do
	// them before anything else).
	for _, fileNum := range tx.pendingDelFileNums {
		tx.db.store.closeFile(fileNum)
		if err := tx.db.store.deleteFileFunc(fileNum); err != nil {
			return err
		}
	}

	// Save the current write cursor for potential rollback.
	wc := tx.db.store.writeCursor
	wc.RLock()
	oldBlkFileNum := wc.curFileNum
	oldBlkOffset := wc.curOffset
	wc.RUnlock()

	rollback := func() {
		tx.db.store.handleRollback(oldBlkFileNum, oldBlkOffset)
	}

	for _, bd := range tx.pendingBlockData {
		log.Tracef("Storing block %s", bd.hash)
		loc, err := tx.db.store.writeBlock(bd.bytes)
		if err != nil {
			rollback()
			return err
		}
		row := serializeBlockLoc(loc)
		if err := tx.blockIdxBucket.Put(bd.hash[:], row); err != nil {
			rollback()
			return err
		}
	}

	// Update writeLoc.  This is the durable witness used by reconcile to
	// detect unclean shutdown.
	writeRow := serializeWriteRow(wc.curFileNum, wc.curOffset)
	if err := tx.metaBucket.Put(writeLocKeyName, writeRow); err != nil {
		rollback()
		return fmt.Errorf("failed to store write cursor: %w", err)
	}

	// Sync block files before committing metadata so that the metadata
	// only points to fully-written block bytes.
	if err := tx.db.store.syncBlocks(); err != nil {
		rollback()
		return err
	}

	if _, err := tx.mtxn.Commit(); err != nil {
		rollback()
		return convertMdbxErr("mdbx Commit", err)
	}
	tx.mtxn = nil
	return nil
}

// Commit commits pending data to MDBX and disk.
func (tx *transaction) Commit() error {
	if tx.managed {
		tx.close()
		panic("managed transaction commit not allowed")
	}
	if err := tx.checkClosed(); err != nil {
		return err
	}
	defer tx.close()

	if !tx.writable {
		return makeDbErr(database.ErrTxNotWritable,
			"Commit requires a writable database transaction", nil)
	}
	return tx.writePendingAndCommit()
}

// Rollback discards all pending changes.
func (tx *transaction) Rollback() error {
	if tx.managed {
		tx.close()
		panic("managed transaction rollback not allowed")
	}
	if err := tx.checkClosed(); err != nil {
		return err
	}
	tx.close()
	return nil
}
