// Copyright (c) 2023 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"container/list"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/txscript/v2"
	"github.com/btcsuite/btcd/wire/v2"
)

// mapSlice is a slice of maps for utxo entries.  The slice of maps are needed to
// guarantee that the map will only take up N amount of bytes.  As of v1.20, the
// go runtime will allocate 2^N + few extra buckets, meaning that for large N, we'll
// allocate a lot of extra memory if the amount of entries goes over the previously
// allocated buckets.  A slice of maps allows us to have a better control of how much
// total memory gets allocated by all the maps.
type mapSlice struct {
	// mtx protects against concurrent access for the map slice.
	mtx sync.Mutex

	// maps are the underlying maps in the slice of maps.
	maps []map[wire.OutPoint]*UtxoEntry

	// maxEntries is the maximum amount of elements that the map is allocated for.
	maxEntries []int

	// maxTotalMemoryUsage is the maximum memory usage in bytes that the state
	// should contain in normal circumstances.
	maxTotalMemoryUsage uint64
}

// length returns the length of all the maps in the map slice added together.
//
// This function is safe for concurrent access.
func (ms *mapSlice) length() int {
	ms.mtx.Lock()
	defer ms.mtx.Unlock()

	var l int
	for _, m := range ms.maps {
		l += len(m)
	}

	return l
}

// size returns the size of all the maps in the map slice added together.
//
// This function is safe for concurrent access.
func (ms *mapSlice) size() int {
	ms.mtx.Lock()
	defer ms.mtx.Unlock()

	var size int
	for _, num := range ms.maxEntries {
		size += calculateRoughMapSize(num, bucketSize)
	}

	return size
}

// get looks for the outpoint in all the maps in the map slice and returns
// the entry.  nil and false is returned if the outpoint is not found.
//
// This function is safe for concurrent access.
func (ms *mapSlice) get(op wire.OutPoint) (*UtxoEntry, bool) {
	ms.mtx.Lock()
	defer ms.mtx.Unlock()

	var entry *UtxoEntry
	var found bool

	for _, m := range ms.maps {
		entry, found = m[op]
		if found {
			return entry, found
		}
	}

	return nil, false
}

// put puts the outpoint and the entry into one of the maps in the map slice.  If the
// existing maps are all full, it will allocate a new map based on how much memory we
// have left over.  Leftover memory is calculated as:
// maxTotalMemoryUsage - (totalEntryMemory + mapSlice.size())
//
// This function is safe for concurrent access.
func (ms *mapSlice) put(op wire.OutPoint, entry *UtxoEntry, totalEntryMemory uint64) {
	ms.mtx.Lock()
	defer ms.mtx.Unlock()

	// Look for the key in the maps.
	for i := range ms.maxEntries {
		m := ms.maps[i]
		_, found := m[op]
		if found {
			// If the key is found, overwrite it.
			m[op] = entry
			return // Return as we were successful in adding the entry.
		}
	}

	for i, maxNum := range ms.maxEntries {
		m := ms.maps[i]
		if len(m) >= maxNum {
			// Don't try to insert if the map already at max since
			// that'll force the map to allocate double the memory it's
			// currently taking up.
			continue
		}

		m[op] = entry
		return // Return as we were successful in adding the entry.
	}

	// We only reach this code if we've failed to insert into the map above as
	// all the current maps were full.  We thus make a new map and insert into
	// it.
	m := ms.makeNewMap(totalEntryMemory)
	m[op] = entry
}

// delete attempts to delete the given outpoint in all of the maps. No-op if the
// outpoint doesn't exist.
//
// This function is safe for concurrent access.
func (ms *mapSlice) delete(op wire.OutPoint) {
	ms.mtx.Lock()
	defer ms.mtx.Unlock()

	for i := 0; i < len(ms.maps); i++ {
		delete(ms.maps[i], op)
	}
}

// makeNewMap makes and appends the new map into the map slice.
//
// This function is NOT safe for concurrent access and must be called with the
// lock held.
func (ms *mapSlice) makeNewMap(totalEntryMemory uint64) map[wire.OutPoint]*UtxoEntry {
	// Get the size of the leftover memory.
	memSize := ms.maxTotalMemoryUsage - totalEntryMemory
	for _, maxNum := range ms.maxEntries {
		memSize -= uint64(calculateRoughMapSize(maxNum, bucketSize))
	}

	// Get a new map that's sized to house inside the leftover memory.
	// -1 on the returned value will make the map allocate half as much total
	// bytes.  This is done to make sure there's still room left for utxo
	// entries to take up.
	numMaxElements := calculateMinEntries(int(memSize), bucketSize+avgEntrySize)
	numMaxElements -= 1
	ms.maxEntries = append(ms.maxEntries, numMaxElements)
	ms.maps = append(ms.maps, make(map[wire.OutPoint]*UtxoEntry, numMaxElements))

	return ms.maps[len(ms.maps)-1]
}

// deleteMaps deletes all maps except for the first one which should be the biggest.
//
// This function is safe for concurrent access.
func (ms *mapSlice) deleteMaps() {
	ms.mtx.Lock()
	defer ms.mtx.Unlock()

	size := ms.maxEntries[0]
	ms.maxEntries = []int{size}
	ms.maps = ms.maps[:1]
}

const (
	// utxoFlushPeriodicInterval is the interval at which a flush is performed
	// when the flush mode FlushPeriodic is used.  This is used when the initial
	// block download is complete and it's useful to flush periodically in case
	// of unforeseen shutdowns.
	utxoFlushPeriodicInterval = time.Minute * 5
)

// FlushMode is used to indicate the different urgency types for a flush.
type FlushMode uint8

const (
	// FlushRequired is the flush mode that means a flush must be performed
	// regardless of the cache state.  For example right before shutting down.
	FlushRequired FlushMode = iota

	// FlushPeriodic is the flush mode that means a flush can be performed
	// when it would be almost needed.  This is used to periodically signal when
	// no I/O heavy operations are expected soon, so there is time to flush.
	FlushPeriodic

	// FlushIfNeeded is the flush mode that means a flush must be performed only
	// if the cache is exceeding a safety threshold very close to its maximum
	// size.  This is used mostly internally in between operations that can
	// increase the cache size.
	FlushIfNeeded
)

// utxoCache is a cached utxo view in the chainstate of a BlockChain.
type utxoCache struct {
	db database.DB

	// maxTotalMemoryUsage is the maximum memory usage in bytes that the state
	// should contain in normal circumstances.
	maxTotalMemoryUsage uint64

	// cachedEntries keeps the internal cache of the utxo state.  The tfModified
	// flag indicates that the state of the entry (potentially) deviates from the
	// state in the database.  Explicit nil values in the map are used to
	// indicate that the database does not contain the entry.
	cachedEntries    mapSlice
	totalEntryMemory uint64 // Total memory usage in bytes.

	// Below fields are used to indicate when the last flush happened.
	lastFlushHash chainhash.Hash
	lastFlushTime time.Time

	// --- asynchronous double-buffered flush (opt-in) ---
	//
	// When asyncFlush is true, a full cache flush does not block block
	// processing.  Instead the active cachedEntries map is frozen and
	// swapped out for a fresh empty one, and a background goroutine
	// writes the frozen snapshot to the database in its own transaction.
	// Block processing continues against the new active map; reads fall
	// through active -> flushing -> db, and any frozen entry that gets
	// touched is copy-on-written into the active map so the background
	// writer always sees the pristine swap-time snapshot.
	//
	// All of these fields are only ever read/written from the chain
	// writer (under chainLock); the background goroutine only reads the
	// frozen snapshot it was handed and signals completion on flushDone.
	asyncFlush bool

	// flushing is the frozen snapshot currently being written by the
	// background goroutine, or nil when no async flush is in progress.
	flushing *mapSlice

	// flushingHash is the consistency hash the in-progress background
	// flush will commit once the frozen snapshot is fully written.
	flushingHash chainhash.Hash

	// flushDone receives the result of the background flush exactly once;
	// nil when no async flush is in progress.
	flushDone chan error

	// flushGate fairly serializes the single MDBX writer between the
	// background chunked flush and block-connect commits.  MDBX's own
	// write lock is an OS mutex whose handoff favors the tight-looping
	// background flush goroutine, starving the block-connect commits for
	// many chunks (measured: block processing crawled at ~0.9 blk/s
	// during a flush because each commit waited through ~10 chunks).
	// A Go sync.Mutex enters starvation-mode handoff after 1 ms, so a
	// commit that waits gets the writer right after the current chunk
	// instead of losing the race repeatedly — moving the contention off
	// the unfair OS mutex and onto this fair one.  Only engaged when the
	// async flush is enabled.
	flushGate sync.Mutex
}

// guardedUpdate runs fn inside a writable database transaction, serialized
// through flushGate so the background chunked flush and block-connect
// commits get fair turns at the single MDBX writer.  When the async flush
// is disabled there is no background writer to contend with, so the gate
// is skipped entirely.
func (s *utxoCache) guardedUpdate(fn func(database.Tx) error) error {
	if s.asyncFlush {
		s.flushGate.Lock()
		defer s.flushGate.Unlock()
	}
	return s.db.Update(fn)
}

// newUtxoCache initiates a new utxo cache instance with its memory usage limited
// to the given maximum.
func newUtxoCache(db database.DB, maxTotalMemoryUsage uint64) *utxoCache {
	// While the entry isn't included in the map size, add the average size to the
	// bucket size so we get some leftover space for entries to take up.
	numMaxElements := calculateMinEntries(int(maxTotalMemoryUsage), bucketSize+avgEntrySize)
	numMaxElements -= 1

	log.Infof("Pre-allocating for %d MiB", maxTotalMemoryUsage/(1024*1024)+1)

	m := make(map[wire.OutPoint]*UtxoEntry, numMaxElements)

	return &utxoCache{
		db:                  db,
		maxTotalMemoryUsage: maxTotalMemoryUsage,
		asyncFlush:          asyncFlushEnabled(),
		cachedEntries: mapSlice{
			maps:                []map[wire.OutPoint]*UtxoEntry{m},
			maxEntries:          []int{numMaxElements},
			maxTotalMemoryUsage: maxTotalMemoryUsage,
		},
	}
}

// newEmptyMapSlice returns a fresh mapSlice sized for the given memory
// budget, matching the initial allocation newUtxoCache performs.  Used to
// swap in a clean active layer after the previous one is frozen for an
// asynchronous flush.
func newEmptyMapSlice(maxTotalMemoryUsage uint64) mapSlice {
	numMaxElements := calculateMinEntries(
		int(maxTotalMemoryUsage), bucketSize+avgEntrySize,
	)
	numMaxElements -= 1
	m := make(map[wire.OutPoint]*UtxoEntry, numMaxElements)
	return mapSlice{
		maps:                []map[wire.OutPoint]*UtxoEntry{m},
		maxEntries:          []int{numMaxElements},
		maxTotalMemoryUsage: maxTotalMemoryUsage,
	}
}

// asyncFlushEnabled reports whether the opt-in asynchronous UTXO cache
// flush is enabled via the BTCD_ASYNC_UTXO_FLUSH environment variable.
// It is off by default: the synchronous path is the long-proven one,
// and the async path trades transient 2x cache memory and added
// complexity for not stalling block processing during a multi-GiB
// flush.  Enable only after soak-testing on a throwaway datadir.
func asyncFlushEnabled() bool {
	v := os.Getenv("BTCD_ASYNC_UTXO_FLUSH")
	return v != "" && v != "0"
}

// totalMemoryUsage returns the total memory usage in bytes of the UTXO cache.
//
// Only the active layer is counted: the frozen layer (when an async flush
// is in progress) is being drained to disk and must NOT keep the active
// layer from flushing, otherwise the threshold check would never fire
// while a flush is pending.  Whole-process memory (active + frozen) is
// the runtime's concern, bounded by GOMEMLIMIT.
func (s *utxoCache) totalMemoryUsage() uint64 {
	// Total memory is the map size + the size that the utxo entries are
	// taking up.
	size := uint64(s.cachedEntries.size())
	size += s.totalEntryMemory

	return size
}

// cacheGet returns the entry for op, consulting the active layer first
// and then, when an async flush is in progress, the frozen layer.
//
// On a frozen-layer hit the entry is copy-on-written into the active
// layer and the copy is returned: callers (notably addTxIn via
// fetchEntries) mutate entries in place (Spend), and the background
// flush writer must continue to see the pristine swap-time snapshot.
// The copy also has its tfFresh flag cleared — the frozen original will
// be (or already is) written to the database by the background flush,
// so spending the copy must leave a spent marker that flushes as a
// database delete rather than being dropped as a never-persisted entry.
//
// MUST be called with the chain lock held (for writes).
func (s *utxoCache) cacheGet(op wire.OutPoint) (*UtxoEntry, bool) {
	if entry, ok := s.cachedEntries.get(op); ok {
		return entry, true
	}

	if s.flushing == nil {
		return nil, false
	}

	entry, ok := s.flushing.get(op)
	if !ok {
		return nil, false
	}

	// Copy-on-write into the active layer.  Clone is nil-safe, so a
	// negative-cache (nil) entry is propagated as a nil into the active
	// layer unchanged.
	clone := entry.Clone()
	if clone != nil {
		clone.packedFlags &^= tfFresh
	}
	s.cachedEntries.put(op, clone, s.totalEntryMemory)
	s.totalEntryMemory += clone.memoryUsage()

	return clone, true
}

// fetchEntries returns the UTXO entries for the given outpoints.  The function always
// returns as many entries as there are outpoints and the returns entries are in the
// same order as the outpoints.  It returns nil if there is no entry for the outpoint
// in the UTXO set.
//
// The returned entries are NOT safe for concurrent access.
func (s *utxoCache) fetchEntries(outpoints []wire.OutPoint) ([]*UtxoEntry, error) {
	entries := make([]*UtxoEntry, len(outpoints))
	var (
		missingOps    []wire.OutPoint
		missingOpsIdx []int
	)
	for i := range outpoints {
		if entry, ok := s.cacheGet(outpoints[i]); ok {
			entries[i] = entry
			continue
		}

		// At this point, we have missing outpoints.  Allocate them now
		// so that we never allocate if the cache never misses.
		if len(missingOps) == 0 {
			missingOps = make([]wire.OutPoint, 0, len(outpoints))
			missingOpsIdx = make([]int, 0, len(outpoints))
		}

		missingOpsIdx = append(missingOpsIdx, i)
		missingOps = append(missingOps, outpoints[i])
	}

	// Return early and don't attempt access the database if we don't have any
	// missing outpoints.
	if len(missingOps) == 0 {
		return entries, nil
	}

	// Fetch the missing outpoints in the cache from the database.
	dbEntries := make([]*UtxoEntry, len(missingOps))
	err := s.db.View(func(dbTx database.Tx) error {
		utxoBucket := dbTx.Metadata().Bucket(utxoSetBucketName)

		for i := range missingOps {
			entry, err := dbFetchUtxoEntry(dbTx, utxoBucket, missingOps[i])
			if err != nil {
				return err
			}

			dbEntries[i] = entry
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Add each of the entries to the UTXO cache and update their memory
	// usage.
	//
	// NOTE: When the fetched entry is nil, it is still added to the cache
	// as a miss; this prevents future lookups to perform the same database
	// fetch.
	for i := range dbEntries {
		s.cachedEntries.put(missingOps[i], dbEntries[i], s.totalEntryMemory)
		s.totalEntryMemory += dbEntries[i].memoryUsage()
	}

	// Fill in the entries with the ones fetched from the database.
	for i := range missingOpsIdx {
		entries[missingOpsIdx[i]] = dbEntries[i]
	}

	return entries, nil
}

// addTxOut adds the specified output to the cache if it is not provably
// unspendable.  When the cache already has an entry for the output, it will be
// overwritten with the given output.  All fields will be updated for existing
// entries since it's possible it has changed during a reorg.
func (s *utxoCache) addTxOut(outpoint wire.OutPoint, txOut *wire.TxOut, isCoinBase bool,
	blockHeight int32) error {

	// Don't add provably unspendable outputs.
	if txscript.IsUnspendable(txOut.PkScript) {
		return nil
	}

	entry := new(UtxoEntry)
	entry.amount = txOut.Value

	// Deep copy the script when the script in the entry differs from the one in
	// the txout.  This is required since the txout script is a subslice of the
	// overall contiguous buffer that the msg tx houses for all scripts within
	// the tx.  It is deep copied here since this entry may be added to the utxo
	// cache, and we don't want the utxo cache holding the entry to prevent all
	// of the other tx scripts from getting garbage collected.
	entry.pkScript = make([]byte, len(txOut.PkScript))
	copy(entry.pkScript, txOut.PkScript)

	entry.blockHeight = blockHeight
	entry.packedFlags = tfFresh | tfModified
	if isCoinBase {
		entry.packedFlags |= tfCoinBase
	}

	s.cachedEntries.put(outpoint, entry, s.totalEntryMemory)
	s.totalEntryMemory += entry.memoryUsage()

	return nil
}

// addTxOuts adds all outputs in the passed transaction which are not provably
// unspendable to the view.  When the view already has entries for any of the
// outputs, they are simply marked unspent.  All fields will be updated for
// existing entries since it's possible it has changed during a reorg.
func (s *utxoCache) addTxOuts(tx *btcutil.Tx, blockHeight int32) error {
	// Loop all of the transaction outputs and add those which are not
	// provably unspendable.
	isCoinBase := IsCoinBase(tx)
	prevOut := wire.OutPoint{Hash: *tx.Hash()}
	for txOutIdx, txOut := range tx.MsgTx().TxOut {
		// Update existing entries.  All fields are updated because it's
		// possible (although extremely unlikely) that the existing
		// entry is being replaced by a different transaction with the
		// same hash.  This is allowed so long as the previous
		// transaction is fully spent.
		prevOut.Index = uint32(txOutIdx)
		err := s.addTxOut(prevOut, txOut, isCoinBase, blockHeight)
		if err != nil {
			return err
		}
	}

	return nil
}

// addTxIn will add the given input to the cache if the previous outpoint the txin
// is pointing to exists in the utxo set.  The utxo that is being spent by the input
// will be marked as spent and if the utxo is fresh (meaning that the database on disk
// never saw it), it will be removed from the cache.
func (s *utxoCache) addTxIn(txIn *wire.TxIn, stxos *[]SpentTxOut) error {
	// Ensure the referenced utxo exists in the view.  This should
	// never happen unless there is a bug is introduced in the code.
	entries, err := s.fetchEntries([]wire.OutPoint{txIn.PreviousOutPoint})
	if err != nil {
		return err
	}
	if len(entries) != 1 || entries[0] == nil {
		return AssertError(fmt.Sprintf("missing input %v",
			txIn.PreviousOutPoint))
	}

	// Only create the stxo details if requested.
	entry := entries[0]
	if stxos != nil {
		// Populate the stxo details using the utxo entry.
		stxo := SpentTxOut{
			Amount:     entry.Amount(),
			PkScript:   entry.PkScript(),
			Height:     entry.BlockHeight(),
			IsCoinBase: entry.IsCoinBase(),
		}

		*stxos = append(*stxos, stxo)
	}

	// Mark the entry as spent.
	entry.Spend()

	// If an entry is fresh it indicates that this entry was spent before it could be
	// flushed to the database. Because of this, we can just delete it from the map of
	// cached entries.
	if entry.isFresh() {
		// If the entry is fresh, we will always have it in the cache.
		s.cachedEntries.delete(txIn.PreviousOutPoint)
		s.totalEntryMemory -= entry.memoryUsage()
	} else {
		// Can leave the entry to be garbage collected as the only purpose
		// of this entry now is so that the entry on disk can be deleted.
		entry = nil
		s.totalEntryMemory -= entry.memoryUsage()
	}

	return nil
}

// addTxIns will add the given inputs of the tx if it's not a coinbase tx and if
// the previous output that the input is pointing to exists in the utxo set.  The
// utxo that is being spent by the input will be marked as spent and if the utxo
// is fresh (meaning that the database on disk never saw it), it will be removed
// from the cache.
func (s *utxoCache) addTxIns(tx *btcutil.Tx, stxos *[]SpentTxOut) error {
	// Coinbase transactions don't have any inputs to spend.
	if IsCoinBase(tx) {
		return nil
	}

	for _, txIn := range tx.MsgTx().TxIn {
		err := s.addTxIn(txIn, stxos)
		if err != nil {
			return err
		}
	}

	return nil
}

// connectTransaction updates the cache by adding all new utxos created by the
// passed transaction and marking and/or removing all utxos that the transactions
// spend as spent.  In addition, when the 'stxos' argument is not nil, it will
// be updated to append an entry for each spent txout.  An error will be returned
// if the cache and the database does not contain the required utxos.
func (s *utxoCache) connectTransaction(
	tx *btcutil.Tx, blockHeight int32, stxos *[]SpentTxOut) error {

	err := s.addTxIns(tx, stxos)
	if err != nil {
		return err
	}

	// Add the transaction's outputs as available utxos.
	return s.addTxOuts(tx, blockHeight)
}

// connectTransactions updates the cache by adding all new utxos created by all
// of the transactions in the passed block, marking and/or removing all utxos
// the transactions spend as spent, and setting the best hash for the view to
// the passed block.  In addition, when the 'stxos' argument is not nil, it will
// be updated to append an entry for each spent txout.
func (s *utxoCache) connectTransactions(block *btcutil.Block, stxos *[]SpentTxOut) error {
	for _, tx := range block.Transactions() {
		err := s.connectTransaction(tx, block.Height(), stxos)
		if err != nil {
			return err
		}
	}

	return nil
}

// writeMapSliceEntries writes every entry in ms to the database via dbTx,
// emptying each map as it goes: spent/nil entries are deleted, modified
// entries are (batch-)put, clean entries are skipped.  Used by the
// synchronous active-layer flush (writeCache); the per-entry delete is
// what empties the retained maps[0] that deleteMaps does not drop.  It
// does NOT write the consistency marker — the caller does.
func writeMapSliceEntries(dbTx database.Tx, ms *mapSlice) error {
	utxoBucket := dbTx.Metadata().Bucket(utxoSetBucketName)

	// If the bucket implementation supports batched puts (mdbxdb does)
	// collect the live entries into a single slice and submit them in
	// one call.  This lets the backend parallelize the per-entry
	// codec encode (zstd-3 over ~30 M UTXOs is the dominant cost of
	// a multi-GiB flush) instead of doing it serially in this loop.
	batcher, _ := utxoBucket.(database.BatchPutter)

	var batch []database.KVPair
	if batcher != nil {
		// Preallocate roughly to expected live-entry count.
		approxLive := 0
		for i := range ms.maps {
			approxLive += len(ms.maps[i])
		}
		batch = make([]database.KVPair, 0, approxLive)
	}

	for i := range ms.maps {
		for outpoint, entry := range ms.maps[i] {
			switch {
			// If the entry is nil or spent, remove the entry from the database
			// and the cache.
			case entry == nil || entry.IsSpent():
				err := dbDeleteUtxoEntry(utxoBucket, outpoint)
				if err != nil {
					return err
				}

			// No need to update the cache if the entry was not modified.
			case !entry.isModified():
			default:
				if batcher != nil {
					// Stage into the batch — actual encode +
					// write happens below via PutBatch.
					serialized, err := serializeUtxoEntry(entry)
					if err != nil {
						return err
					}
					key := outpointKey(outpoint)
					batch = append(batch, database.KVPair{
						Key: *key, Value: serialized,
					})
				} else {
					// Entry is fresh and needs to be put into the database.
					err := dbPutUtxoEntry(utxoBucket, outpoint, entry)
					if err != nil {
						return err
					}
				}
			}

			delete(ms.maps[i], outpoint)
		}
	}
	if batcher != nil && len(batch) > 0 {
		if err := batcher.PutBatch(batch); err != nil {
			return err
		}
	}

	return nil
}

// asyncFlushChunkEntries is the number of frozen-snapshot entries the
// background flush writes per database transaction.  A full flush of a
// multi-GiB cache (tens of millions of entries) into MDBX's B+tree is
// minutes of single-writer work; doing it in one transaction holds the
// sole MDBX writer for that whole duration and stalls every block-connect
// commit behind it (observed: ~23 min dead stop at a 522 GiB database).
// Chunking releases the writer between transactions so block validation +
// commit interleave — the 32-core script verification that dominates
// post-checkpoint IBD overlaps the flush instead of idling.
//
// The chunk size must keep each chunk's write time well under a block's
// validate time (~tens of ms) so the block-connect commit-wait between
// chunks stays small and validation actually overlaps the flush.  A live
// soak at a 644 GiB database measured a 64 Ki chunk at ~1.4 s of B+tree
// write — far too coarse: during-flush block throughput was only ~0.15
// blk/s (commit-wait dominated, validation did not overlap).  4 Ki brings
// the chunk write to ~85 ms there, comparable to the per-block validate
// time, so validation overlaps and during-flush throughput rises toward
// the validation-bound rate.  Smaller still trades interleaving for
// per-transaction overhead.  The value is hardware/db-size dependent, so
// it is overridable via BTCD_ASYNC_FLUSH_CHUNK; it is also a var so tests
// can lower it to exercise the multi-chunk path.
var asyncFlushChunkEntries = asyncFlushChunkDefault()

// asyncFlushChunkDefault returns the per-chunk entry count, honoring the
// BTCD_ASYNC_FLUSH_CHUNK override when set to a positive integer.
func asyncFlushChunkDefault() int {
	const def = 4096
	if v := os.Getenv("BTCD_ASYNC_FLUSH_CHUNK"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// flushFrozenItem pairs an outpoint with its frozen-snapshot entry for
// chunked writing.
type flushFrozenItem struct {
	outpoint wire.OutPoint
	entry    *UtxoEntry
}

// flushFrozen writes the frozen snapshot ms to the database in
// asyncFlushChunkEntries-sized transactions, releasing the MDBX writer
// between each so concurrent block-connect commits interleave, then
// commits the consistency marker in a final transaction.
//
// Crash safety: the marker advances only after every chunk is durably
// committed.  A crash mid-flush leaves the old on-disk marker, and
// InitConsistentState replays from there — re-applying any
// partially-written chunks idempotently (puts overwrite, spends
// re-delete).  Block commits that interleave write the best-state /
// block-index / spend-journal buckets, never the utxoSet bucket the
// flush writes, so there is no key conflict between the two, only
// writer serialization.
//
// ms is read-only here (never mutated): the chain writer may be reading
// it concurrently via cacheGet, and both sides doing only reads keeps
// the access data-race-free.
func (s *utxoCache) flushFrozen(ms *mapSlice, hash chainhash.Hash) error {
	chunk := make([]flushFrozenItem, 0, asyncFlushChunkEntries)

	flushChunk := func() error {
		if len(chunk) == 0 {
			return nil
		}
		// guardedUpdate releases the fairness gate between chunks so a
		// waiting block-connect commit gets the writer next.
		err := s.guardedUpdate(func(dbTx database.Tx) error {
			return writeFrozenChunk(dbTx, chunk)
		})
		chunk = chunk[:0]
		return err
	}

	for i := range ms.maps {
		for outpoint, entry := range ms.maps[i] {
			chunk = append(chunk, flushFrozenItem{outpoint, entry})
			if len(chunk) >= asyncFlushChunkEntries {
				if err := flushChunk(); err != nil {
					return err
				}
			}
		}
	}
	if err := flushChunk(); err != nil {
		return err
	}

	// Marker last — only now is the whole snapshot durable.
	return s.guardedUpdate(func(dbTx database.Tx) error {
		return dbPutUtxoStateConsistency(dbTx, &hash)
	})
}

// writeFrozenChunk writes one chunk of frozen entries within dbTx:
// spent/nil are deleted, modified are batch-put, clean are skipped.
func writeFrozenChunk(dbTx database.Tx, items []flushFrozenItem) error {
	utxoBucket := dbTx.Metadata().Bucket(utxoSetBucketName)
	batcher, _ := utxoBucket.(database.BatchPutter)

	var batch []database.KVPair
	if batcher != nil {
		batch = make([]database.KVPair, 0, len(items))
	}

	for _, it := range items {
		entry := it.entry
		switch {
		case entry == nil || entry.IsSpent():
			if err := dbDeleteUtxoEntry(utxoBucket, it.outpoint); err != nil {
				return err
			}

		case !entry.isModified():
		default:
			if batcher != nil {
				serialized, err := serializeUtxoEntry(entry)
				if err != nil {
					return err
				}
				key := outpointKey(it.outpoint)
				batch = append(batch, database.KVPair{
					Key: *key, Value: serialized,
				})
			} else {
				if err := dbPutUtxoEntry(utxoBucket, it.outpoint, entry); err != nil {
					return err
				}
			}
		}
	}
	if batcher != nil && len(batch) > 0 {
		if err := batcher.PutBatch(batch); err != nil {
			return err
		}
	}

	return nil
}

// writeCache writes all the entries that are cached in memory to the database atomically.
func (s *utxoCache) writeCache(dbTx database.Tx, bestState *BestState) error {
	if err := writeMapSliceEntries(dbTx, &s.cachedEntries); err != nil {
		return err
	}
	s.cachedEntries.deleteMaps()
	s.totalEntryMemory = 0

	// When done, store the best state hash in the database to indicate the state
	// is consistent until that hash.
	err := dbPutUtxoStateConsistency(dbTx, &bestState.Hash)
	if err != nil {
		return err
	}

	// The best state is the new last flush hash.
	s.lastFlushHash = bestState.Hash
	s.lastFlushTime = time.Now()

	return nil
}

// flush flushes the UTXO state to the database if a flush is needed with the given flush mode.
//
// This function MUST be called with the chain state lock held (for writes).
func (s *utxoCache) flush(dbTx database.Tx, mode FlushMode, bestState *BestState) error {
	var threshold uint64
	switch mode {
	case FlushRequired:
		threshold = 0

	case FlushIfNeeded:
		// If we performed a flush in the current best state, we have nothing to do.
		if bestState.Hash == s.lastFlushHash {
			return nil
		}

		threshold = s.maxTotalMemoryUsage

	case FlushPeriodic:
		// If the time since the last flush is over the periodic interval,
		// force a flush.  Otherwise just flush when the cache is full.
		if time.Since(s.lastFlushTime) > utxoFlushPeriodicInterval {
			threshold = 0
		} else {
			threshold = s.maxTotalMemoryUsage
		}
	}

	if s.totalMemoryUsage() >= threshold {
		// Add one to round up the integer division.
		totalMiB := s.totalMemoryUsage() / ((1024 * 1024) + 1)
		log.Infof("Flushing UTXO cache of %d MiB with %d entries to disk. For large sizes, "+
			"this can take up to several minutes...", totalMiB, s.cachedEntries.length())

		return s.writeCache(dbTx, bestState)
	}

	return nil
}

// reapAsyncFlush observes a previously-started background flush.  When
// wait is true it blocks until the background goroutine finishes;
// otherwise it returns immediately if the flush is still in progress.
// On a completed flush it clears the frozen-snapshot bookkeeping and, on
// success, advances the in-memory consistency hash to the snapshot's
// marker (the durable marker was written by the background transaction
// itself).  A background failure is fatal: the frozen deltas were
// swapped out of the active cache and never persisted, so the only safe
// recovery is to surface the error, stop, and let the next startup
// replay from the un-advanced on-disk marker.
//
// MUST be called with the chain lock held (for writes) and MUST NOT be
// called from inside an open database write transaction (the background
// flush holds its own write transaction; MDBX permits a single writer).
func (s *utxoCache) reapAsyncFlush(wait bool) error {
	if s.flushDone == nil {
		return nil
	}

	var err error
	if wait {
		err = <-s.flushDone
	} else {
		select {
		case err = <-s.flushDone:
		default:
			return nil // still in progress
		}
	}

	s.flushDone = nil
	s.flushing = nil
	if err != nil {
		return err
	}

	s.lastFlushHash = s.flushingHash
	s.lastFlushTime = time.Now()
	return nil
}

// drainAsyncFlush blocks until any in-progress background flush completes
// and reaps it.  Used before operations that must observe a quiescent
// cache and own the sole database writer: synchronous (FlushRequired)
// flushes, reorganizations, and shutdown.
//
// MUST be called with the chain lock held and outside any open write txn.
func (s *utxoCache) drainAsyncFlush() error {
	return s.reapAsyncFlush(true)
}

// maybeAsyncFlush performs a non-blocking, double-buffered flush of the
// active cache when it has grown past the memory threshold.  Instead of
// writing the cache inline (which stalls block processing for the
// multi-GiB, multi-minute duration of a full flush), it freezes the
// active map, swaps in a fresh empty one, and hands the frozen snapshot
// to a background goroutine that writes it in its own transaction.
//
// Reads fall through active -> frozen -> db (see cacheGet), and frozen
// entries touched afterwards are copy-on-written into the active map, so
// the background writer always sees the pristine swap-time snapshot.
//
// Crash safety rides on the existing lag+replay model: the durable
// consistency marker only advances when the background transaction
// commits the entire frozen snapshot atomically.  A crash mid-flush
// leaves the older marker on disk and replay rebuilds from there.
//
// MUST be called with the chain lock held and outside any open write txn.
func (s *utxoCache) maybeAsyncFlush(bestState *BestState, mode FlushMode) error {
	// Reap a previously-completed background flush so its frozen map is
	// released and a new one can be started.
	if err := s.reapAsyncFlush(false); err != nil {
		return err
	}

	// Determine the flush threshold from the mode, mirroring the
	// synchronous flush().  FlushRequired never reaches here (callers
	// route it to the synchronous path), but handle it defensively.
	var threshold uint64
	switch mode {
	case FlushRequired:
		threshold = 0

	case FlushIfNeeded:
		if bestState.Hash == s.lastFlushHash {
			return nil
		}
		threshold = s.maxTotalMemoryUsage

	case FlushPeriodic:
		if time.Since(s.lastFlushTime) > utxoFlushPeriodicInterval {
			threshold = 0
		} else {
			threshold = s.maxTotalMemoryUsage
		}
	}

	// Nothing to do unless the active layer has crossed the threshold.
	if s.totalMemoryUsage() < threshold {
		return nil
	}

	// Nothing to flush if the active layer is empty (can happen for a
	// time-triggered periodic flush right after a prior flush).
	if s.cachedEntries.length() == 0 {
		return nil
	}

	// Back-pressure: a background flush is still draining the previous
	// snapshot and we cannot hold two frozen snapshots at once.  Wait for
	// it to finish, then proceed.  This is the safety valve for the case
	// where block processing outruns flush throughput.
	if s.flushDone != nil {
		if err := s.drainAsyncFlush(); err != nil {
			return err
		}
	}

	totalMiB := s.totalMemoryUsage() / ((1024 * 1024) + 1)
	log.Infof("Async-flushing UTXO cache of %d MiB with %d entries in the "+
		"background; block processing continues", totalMiB,
		s.cachedEntries.length())

	// Freeze the active map and swap in a fresh one.  The swap is cheap
	// (slice-header moves).  Construct the frozen mapSlice fresh rather
	// than copying s.cachedEntries by value — mapSlice embeds a Mutex and
	// copying a held-by-value lock is a bug — moving the maps/maxEntries
	// across gives the frozen snapshot its own zero-value mutex.
	frozen := &mapSlice{
		maps:                s.cachedEntries.maps,
		maxEntries:          s.cachedEntries.maxEntries,
		maxTotalMemoryUsage: s.cachedEntries.maxTotalMemoryUsage,
	}
	s.flushing = frozen
	s.flushingHash = bestState.Hash

	s.cachedEntries = newEmptyMapSlice(s.maxTotalMemoryUsage)
	s.totalEntryMemory = 0

	markerHash := bestState.Hash
	done := make(chan error, 1)
	s.flushDone = done
	go func(ms *mapSlice, hash chainhash.Hash) {
		// flushFrozen writes the snapshot in chunked transactions,
		// releasing the MDBX writer between chunks so block-connect
		// commits interleave instead of stalling for the whole flush.
		// ms is read-only here; the chain writer may read it
		// concurrently via cacheGet (both sides read-only = safe).
		done <- s.flushFrozen(ms, hash)
	}(frozen, markerHash)

	return nil
}

// FlushUtxoCache flushes the UTXO state to the database if a flush is needed with the
// given flush mode.
//
// This function is safe for concurrent access.
func (b *BlockChain) FlushUtxoCache(mode FlushMode) error {
	b.chainLock.Lock()
	defer b.chainLock.Unlock()

	// Any in-progress background flush must be drained first: it owns a
	// separate write transaction (MDBX is single-writer) and its frozen
	// snapshot must land before we either start a synchronous flush or
	// hand off a new asynchronous one.  Draining happens outside the
	// db.Update below for the same single-writer reason.
	if b.utxoCache.asyncFlush {
		if err := b.utxoCache.drainAsyncFlush(); err != nil {
			return err
		}

		// For non-required modes, prefer the non-blocking background
		// flush so callers (e.g. periodic flushers) don't stall.
		// FlushRequired (shutdown) falls through to the synchronous
		// path so the cache is guaranteed durable on return.
		if mode != FlushRequired {
			return b.utxoCache.maybeAsyncFlush(b.BestSnapshot(), mode)
		}
	}

	return b.db.Update(func(dbTx database.Tx) error {
		return b.utxoCache.flush(dbTx, mode, b.BestSnapshot())
	})
}

// InitConsistentState checks the consistency status of the utxo state and
// replays blocks if it lags behind the best state of the blockchain.
//
// It needs to be ensured that the chainView passed to this method does not
// get changed during the execution of this method.
func (b *BlockChain) InitConsistentState(tip *blockNode, interrupt <-chan struct{}) error {
	s := b.utxoCache

	// Load the consistency status from the database.
	var statusBytes []byte
	s.db.View(func(dbTx database.Tx) error {
		statusBytes = dbFetchUtxoStateConsistency(dbTx)
		return nil
	})

	// If no status was found, the database is old and didn't have a cached utxo
	// state yet. In that case, we set the status to the best state and write
	// this to the database.
	if statusBytes == nil {
		err := s.db.Update(func(dbTx database.Tx) error {
			return dbPutUtxoStateConsistency(dbTx, &tip.hash)
		})

		// Set the last flush hash as it's the default value of 0s.
		s.lastFlushHash = tip.hash
		s.lastFlushTime = time.Now()

		return err
	}

	statusHash, err := chainhash.NewHash(statusBytes)
	if err != nil {
		return err
	}

	// If state is consistent, we are done.
	if statusHash.IsEqual(&tip.hash) {
		log.Debugf("UTXO state consistent at (%d:%v)", tip.height, tip.hash)

		// The last flush hash is set to the default value of all 0s. Set
		// it to the tip since we checked it's consistent.
		s.lastFlushHash = tip.hash

		// Set the last flush time as now since we know the state is consistent
		// at this time.
		s.lastFlushTime = time.Now()

		return nil
	}

	lastFlushNode := b.index.LookupNode(statusHash)
	log.Infof("Reconstructing UTXO state after an unclean shutdown. The UTXO state is "+
		"consistent at block %s (%d) but the chainstate is at block %s (%d),  This may "+
		"take a long time...", statusHash.String(), lastFlushNode.height,
		tip.hash.String(), tip.height)

	// Even though this should always be true, make sure the fetched hash is in
	// the best chain.
	fork := b.bestChain.FindFork(lastFlushNode)
	if fork == nil {
		return AssertError(fmt.Sprintf("last utxo consistency status contains "+
			"hash that is not in best chain: %v", statusHash))
	}

	// We never disconnect blocks as they cannot be inconsistent during a reorganization.
	// This is because The cache is flushed before the reorganization begins and the utxo
	// set at each block disconnect is written atomically to the database.
	node := lastFlushNode

	// We replay the blocks from the last consistent state up to the best
	// state. Iterate forward from the consistent node to the tip of the best
	// chain.
	attachNodes := list.New()
	for n := tip; n.height >= 0; n = n.parent {
		if n == fork {
			break
		}
		attachNodes.PushFront(n)
	}

	// Convert the linked list to a slice so a worker can index ahead
	// of the apply cursor.  attachNodes is bounded by the gap between
	// last-consistent UTXO and the chain tip; in the worst case (full
	// 950 k mainnet replay) that's ~32 MiB of pointers, fine for RAM.
	nodeList := make([]*blockNode, 0, attachNodes.Len())
	for e := attachNodes.Front(); e != nil; e = e.Next() {
		nodeList = append(nodeList, e.Value.(*blockNode))
	}

	// Prefetch worker: pull blocks from disk ahead of the main apply
	// loop and stream them via a bounded channel.  At steady state the
	// apply loop is bottlenecked on disk reads (page-cache cold), and
	// connectTransactions itself is in-memory and dramatically faster.
	// Overlapping the two cuts wall time by ~30-50 % in practice; the
	// queue size caps memory at queueDepth × avg block bytes (~1 MiB
	// for mainnet recent blocks) ≈ 32 MiB.
	type fetched struct {
		node  *blockNode
		block *btcutil.Block
		err   error
	}
	const queueDepth = 32
	feed := make(chan fetched, queueDepth)
	stop := make(chan struct{})

	go func() {
		defer close(feed)
		for _, n := range nodeList {
			var blk *btcutil.Block
			err := s.db.View(func(dbTx database.Tx) error {
				var fetchErr error
				blk, fetchErr = dbFetchBlockByNode(dbTx, n)
				return fetchErr
			})
			select {
			case feed <- fetched{node: n, block: blk, err: err}:
			case <-stop:
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for fb := range feed {
		if fb.err != nil {
			close(stop)
			// Drain any in-flight sends so the worker can exit.
			for range feed {
			}
			return fb.err
		}
		node = fb.node

		err = b.utxoCache.connectTransactions(fb.block, nil)
		if err != nil {
			close(stop)
			for range feed {
			}
			return err
		}

		// Flush the utxo cache if needed.  This updates the
		// consistent state to this block.
		err = s.db.Update(func(dbTx database.Tx) error {
			return s.flush(dbTx, FlushIfNeeded, &BestState{
				Hash: node.hash, Height: node.height,
			})
		})
		if err != nil {
			close(stop)
			for range feed {
			}
			return err
		}

		if interruptRequested(interrupt) {
			log.Warn("UTXO state reconstruction interrupted")
			close(stop)
			for range feed {
			}
			return errInterruptRequested
		}
	}
	log.Debug("UTXO state reconstruction done")

	// Set the last flush hash as it's the default value of 0s.
	s.lastFlushHash = tip.hash
	s.lastFlushTime = time.Now()

	return nil
}

// flushNeededAfterPrune returns true if the utxo cache needs to be flushed after a prune
// of the block storage.  In the case of an unexpected shutdown, the utxo cache needs
// to be reconstructed from where the utxo cache was last flushed.  In order for the
// utxo cache to be reconstructed, we always need to have the blocks since the utxo cache
// flush last happened.
//
// Example: if the last flush hash was at height 100 and one of the deleted blocks was at
// height 98, this function will return true.
func (b *BlockChain) flushNeededAfterPrune(deletedBlockHashes []chainhash.Hash) (bool, error) {
	node := b.index.LookupNode(&b.utxoCache.lastFlushHash)
	if node == nil {
		// If we couldn't find the node where we last flushed at, have the utxo cache
		// flush to be safe and that will set the last flush hash again.
		//
		// This realistically should never happen as nodes are never deleted from
		// the block index.  This happening likely means that there's a hardware
		// error which is something we can't recover from.  The best that we can
		// do here is to just force a flush and hope that the newly set
		// lastFlushHash doesn't error.
		return true, nil
	}

	lastFlushHeight := node.Height()

	// Loop through all the block hashes and find out what the highest block height
	// among the deleted hashes is.
	highestDeletedHeight := int32(-1)
	for _, deletedBlockHash := range deletedBlockHashes {
		node := b.index.LookupNode(&deletedBlockHash)
		if node == nil {
			// If we couldn't find this node, just skip it and try the next
			// deleted hash.  This might be a corruption in the database
			// but there's nothing we can do here to address it except for
			// moving onto the next block.
			continue
		}
		if node.height > highestDeletedHeight {
			highestDeletedHeight = node.height
		}
	}

	return highestDeletedHeight >= lastFlushHeight, nil
}
