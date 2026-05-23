// Copyright (c) 2015-2016 The btcsuite developers
// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package mdbxdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"runtime"
	"sort"
	"sync"

	"github.com/btcsuite/btcd/database"
)

// bucket implements database.Bucket on top of the bucket-ID prefix scheme
// shared with ffldb.
type bucket struct {
	tx *transaction
	id [4]byte

	// path is the logical hierarchical name path of this bucket relative to
	// the root metadata bucket, not including the root.  It is used to
	// resolve per-table codecs.  The root metadata bucket has path == nil.
	path [][]byte
}

var _ database.Bucket = (*bucket)(nil)

// bucketIndexKey returns the actual key used to record a child bucket entry
// in the bucket index.  Layout: <bucketIndexPrefix><parentID><bucketName>.
func bucketIndexKey(parentID [4]byte, key []byte) []byte {
	out := make([]byte, len(bucketIndexPrefix)+4+len(key))
	copy(out, bucketIndexPrefix)
	copy(out[len(bucketIndexPrefix):], parentID[:])
	copy(out[len(bucketIndexPrefix)+4:], key)
	return out
}

// bucketizedKey returns the storage key for a user key within bucket id.
// Layout: <bucketID><key>.
func bucketizedKey(bucketID [4]byte, key []byte) []byte {
	out := make([]byte, 4+len(key))
	copy(out, bucketID[:])
	copy(out[4:], key)
	return out
}

// keyBufPool pools scratch buffers for bucketizedKey on the hot Put
// path.  Same rationale as encodeBufPool — without pooling, every Put
// allocates a small (~36 byte) key buffer which adds up to millions
// of allocations per flush.
var keyBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 64)
		return &b
	},
}

// bucketizedKeyInto writes the bucketID-prefixed key into dst[:0] and
// returns the resulting slice.  Caller bracketed with the pool;
// retains the slice only until the next mdbx Put completes (mdbx
// copies the bytes into its tree pages, so reuse after that is safe).
func bucketizedKeyInto(dst []byte, bucketID [4]byte, key []byte) []byte {
	need := 4 + len(key)
	if cap(dst) < need {
		dst = make([]byte, need)
	} else {
		dst = dst[:need]
	}
	copy(dst, bucketID[:])
	copy(dst[4:], key)
	return dst
}

// childPath returns the path of a sub-bucket whose name is `name`.
func (b *bucket) childPath(name []byte) [][]byte {
	out := make([][]byte, 0, len(b.path)+1)
	out = append(out, b.path...)
	cp := make([]byte, len(name))
	copy(cp, name)
	return append(out, cp)
}

// Bucket retrieves a nested bucket with the given key.
func (b *bucket) Bucket(key []byte) database.Bucket {
	if err := b.tx.checkClosed(); err != nil {
		return nil
	}
	childID := b.tx.rawGet(bucketIndexKey(b.id, key))
	if childID == nil {
		return nil
	}
	child := &bucket{tx: b.tx, path: b.childPath(key)}
	copy(child.id[:], childID)
	return child
}

// CreateBucket creates and returns a new nested bucket.
func (b *bucket) CreateBucket(key []byte) (database.Bucket, error) {
	if err := b.tx.checkClosed(); err != nil {
		return nil, err
	}
	if !b.tx.writable {
		return nil, makeDbErr(database.ErrTxNotWritable,
			"create bucket requires a writable database transaction", nil)
	}
	if len(key) == 0 {
		return nil, makeDbErr(database.ErrBucketNameRequired,
			"create bucket requires a key", nil)
	}

	bidxKey := bucketIndexKey(b.id, key)
	if b.tx.rawGet(bidxKey) != nil {
		return nil, makeDbErr(database.ErrBucketExists,
			"bucket already exists", nil)
	}

	// The internal block-index bucket has a fixed reserved ID.
	var childID [4]byte
	if b.id == metadataBucketID && bytes.Equal(key, blockIdxBucketName) {
		childID = blockIdxBucketID
	} else {
		nid, err := b.tx.nextBucketID()
		if err != nil {
			return nil, err
		}
		childID = nid
	}
	if err := b.tx.rawPut(bidxKey, childID[:]); err != nil {
		return nil, fmt.Errorf("failed to create bucket %q: %w", key, err)
	}
	return &bucket{tx: b.tx, id: childID, path: b.childPath(key)}, nil
}

// CreateBucketIfNotExists returns the existing bucket if any, else creates it.
func (b *bucket) CreateBucketIfNotExists(key []byte) (database.Bucket, error) {
	if err := b.tx.checkClosed(); err != nil {
		return nil, err
	}
	if !b.tx.writable {
		return nil, makeDbErr(database.ErrTxNotWritable,
			"create bucket requires a writable database transaction", nil)
	}
	if existing := b.Bucket(key); existing != nil {
		return existing, nil
	}
	return b.CreateBucket(key)
}

// DeleteBucket removes a nested bucket and all of its contents (recursively).
func (b *bucket) DeleteBucket(key []byte) error {
	if err := b.tx.checkClosed(); err != nil {
		return err
	}
	if !b.tx.writable {
		return makeDbErr(database.ErrTxNotWritable,
			"delete bucket requires a writable database transaction", nil)
	}

	bidxKey := bucketIndexKey(b.id, key)
	rawID := b.tx.rawGet(bidxKey)
	if rawID == nil {
		return makeDbErr(database.ErrBucketNotFound,
			fmt.Sprintf("bucket %q does not exist", key), nil)
	}

	// BFS: delete all keys in the target bucket subtree, plus its bucket
	// index entries.
	stack := [][]byte{rawID}
	for len(stack) > 0 {
		curID := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		// Delete all data keys whose bucket ID equals curID.
		if err := b.tx.deletePrefix(curID); err != nil {
			return err
		}

		// Iterate the bucket index entries for child buckets.
		prefix := make([]byte, len(bucketIndexPrefix)+4)
		copy(prefix, bucketIndexPrefix)
		copy(prefix[len(bucketIndexPrefix):], curID)

		c, err := b.tx.newRawCursor()
		if err != nil {
			return err
		}
		for ok := c.seekPrefix(prefix); ok; ok = c.nextWithPrefix(prefix) {
			childID := append([]byte(nil), c.value()...)
			stack = append(stack, childID)
			if err := c.del(); err != nil {
				c.close()
				return err
			}
		}
		c.close()
	}

	if err := b.tx.rawDelete(bidxKey); err != nil {
		return err
	}
	return nil
}

// Cursor returns a cursor over both the keys and nested buckets of b.
func (b *bucket) Cursor() database.Cursor {
	if err := b.tx.checkClosed(); err != nil {
		return &cursor{bucket: b}
	}
	c := newCursor(b, ctFull)
	runtime.SetFinalizer(c, cursorFinalizer)
	return c
}

// ForEach invokes fn for every key/value pair in the bucket (no recursion).
func (b *bucket) ForEach(fn func(k, v []byte) error) error {
	if err := b.tx.checkClosed(); err != nil {
		return err
	}
	c := newCursor(b, ctKeys)
	defer cursorFinalizer(c)
	for ok := c.First(); ok; ok = c.Next() {
		if err := fn(c.Key(), c.Value()); err != nil {
			return err
		}
	}
	return nil
}

// ForEachBucket invokes fn for every nested bucket name.
func (b *bucket) ForEachBucket(fn func(k []byte) error) error {
	if err := b.tx.checkClosed(); err != nil {
		return err
	}
	c := newCursor(b, ctBuckets)
	defer cursorFinalizer(c)
	for ok := c.First(); ok; ok = c.Next() {
		if err := fn(c.Key()); err != nil {
			return err
		}
	}
	return nil
}

// Writable returns whether the underlying transaction is writable.
func (b *bucket) Writable() bool { return b.tx.writable }

// Put stores a key/value pair, applying the configured codec on value.
// Uses pooled scratch buffers for both the encoded value and the
// bucketized key so the steady-state hot path avoids per-call
// allocation.
//
// Pool lifetime invariant: mdbx_put() is documented to copy both key
// and value bytes into the btree page synchronously before returning
// to the caller (the MDBX_RESERVE flag is the documented exception
// and we never use it).  Therefore both pool buffers are safe to
// recycle on the next Put.  See mdbx-go/mdbx/txn.go Put().  If a
// future change introduces MDBX_RESERVE in this path the pool
// recycling must be re-evaluated; the C-side pointer would remain
// valid only until the transaction commits.
func (b *bucket) Put(key, value []byte) error {
	if err := b.tx.checkClosed(); err != nil {
		return err
	}
	if !b.tx.writable {
		return makeDbErr(database.ErrTxNotWritable,
			"setting a key requires a writable database transaction", nil)
	}
	if len(key) == 0 {
		return makeDbErr(database.ErrKeyRequired, "put requires a key", nil)
	}
	codec := codecFor(b.path)

	encBufPtr := encodeBufGet()
	defer encodeBufPut(encBufPtr)
	encoded, err := encodeInto(codec, *encBufPtr, value)
	if err != nil {
		return makeDbErr(database.ErrDriverSpecific, "codec encode", err)
	}
	*encBufPtr = encoded

	keyBufPtr := keyBufPool.Get().(*[]byte)
	defer keyBufPool.Put(keyBufPtr)
	*keyBufPtr = bucketizedKeyInto(*keyBufPtr, b.id, key)

	return b.tx.rawPut(*keyBufPtr, encoded)
}

// putBatchChunkSize is the number of pairs encoded per chunk inside
// PutBatch.  Chunking bounds the peak memory for the encoded byte
// slices to (chunkSize * avg_encoded_size) while still amortizing the
// goroutine launch overhead.
const putBatchChunkSize = 4096

// PutBatch stores many key/value pairs.  Two optimizations vs. a
// naive Put-in-a-loop:
//
//   - The input is sorted by key in-place before writing so that the
//     MDBX btree sees ascending-key inserts.  MDBX (like any B+
//     tree) is dramatically faster on sequential keys: page splits
//     happen only at the rightmost leaf and no random-access page
//     reads are needed.  In practice the UTXO flush goes from
//     dominated by btree page seeks to mostly compression cost.
//
//   - Values are zstd-encoded in parallel across all available CPUs
//     before the actual writes, which themselves must remain serial
//     because MDBX permits only one writer per transaction.  What
//     was a single-threaded multi-minute step becomes (NumCPU)x
//     faster on the encode phase.
//
// The input slice is sorted in place; callers that need the original
// order should pass a copy.
func (b *bucket) PutBatch(pairs []database.KVPair) error {
	if err := b.tx.checkClosed(); err != nil {
		return err
	}
	if !b.tx.writable {
		return makeDbErr(database.ErrTxNotWritable,
			"setting a key requires a writable database transaction", nil)
	}
	if len(pairs) == 0 {
		return nil
	}

	if len(pairs) > 1 {
		sort.Slice(pairs, func(i, j int) bool {
			return bytes.Compare(pairs[i].Key, pairs[j].Key) < 0
		})
	}

	codec := codecFor(b.path)

	encoded := make([][]byte, putBatchChunkSize)
	for start := 0; start < len(pairs); start += putBatchChunkSize {
		end := start + putBatchChunkSize
		if end > len(pairs) {
			end = len(pairs)
		}
		chunk := pairs[start:end]
		encChunk := encoded[:len(chunk)]

		if err := b.encodeChunkParallel(codec, chunk, encChunk); err != nil {
			return err
		}

		// Serial write phase — MDBX tx is single-writer.
		for i, p := range chunk {
			if len(p.Key) == 0 {
				return makeDbErr(database.ErrKeyRequired,
					"put requires a key", nil)
			}
			if err := b.tx.rawPut(
				bucketizedKey(b.id, p.Key), encChunk[i],
			); err != nil {
				return err
			}
		}
	}
	return nil
}

// encodeChunkParallel fans codec.Encode across runtime.NumCPU workers,
// writing results into out[i] for each pairs[i].  Returns the first
// encode error if any worker fails.
//
// Thread-safety: out[] is shared across workers but each worker is
// assigned a disjoint [from, to) sub-range and only writes to its
// own indices.  No reads happen until wg.Wait() returns.  The
// backing array is never re-sliced or resized during the fan-out, so
// concurrent writes to disjoint indices are safe per the Go memory
// model.  Do NOT add logging or other intermediate reads of out[]
// inside the worker goroutines.
func (b *bucket) encodeChunkParallel(codec Codec,
	pairs []database.KVPair, out [][]byte) error {

	workers := runtime.NumCPU()
	if workers > len(pairs) {
		workers = len(pairs)
	}
	if workers <= 1 {
		// Trivially small — skip goroutine overhead.
		for i, p := range pairs {
			enc, err := codec.Encode(p.Value)
			if err != nil {
				return makeDbErr(database.ErrDriverSpecific,
					"codec encode", err)
			}
			out[i] = enc
		}
		return nil
	}

	var (
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
	)
	per := (len(pairs) + workers - 1) / workers
	for w := 0; w < workers; w++ {
		from := w * per
		if from >= len(pairs) {
			break
		}
		to := from + per
		if to > len(pairs) {
			to = len(pairs)
		}
		wg.Add(1)
		go func(from, to int) {
			defer wg.Done()
			for i := from; i < to; i++ {
				enc, err := codec.Encode(pairs[i].Value)
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					return
				}
				out[i] = enc
			}
		}(from, to)
	}
	wg.Wait()
	if firstErr != nil {
		return makeDbErr(database.ErrDriverSpecific,
			"codec encode", firstErr)
	}
	return nil
}

// Get returns the value for the given key, or nil if absent.  Returned bytes
// are valid only until the transaction ends.
func (b *bucket) Get(key []byte) []byte {
	if err := b.tx.checkClosed(); err != nil {
		return nil
	}
	if len(key) == 0 {
		return nil
	}
	raw := b.tx.rawGet(bucketizedKey(b.id, key))
	if raw == nil {
		return nil
	}
	decoded, err := codecFor(b.path).Decode(raw)
	if err != nil {
		// Decode failures are rare and indicate corruption; surface nil
		// to mirror "missing" semantics callers can detect via lookup.
		log.Errorf("decode failure in bucket %q: %v", b.path, err)
		return nil
	}
	return decoded
}

// Delete removes a key from the bucket.  Missing keys are a no-op.
func (b *bucket) Delete(key []byte) error {
	if err := b.tx.checkClosed(); err != nil {
		return err
	}
	if !b.tx.writable {
		return makeDbErr(database.ErrTxNotWritable,
			"deleting a value requires a writable database transaction", nil)
	}
	if len(key) == 0 {
		return nil
	}
	return b.tx.rawDelete(bucketizedKey(b.id, key))
}

// nextBucketID atomically increments the highest used bucket ID counter.
func (tx *transaction) nextBucketID() ([4]byte, error) {
	cur := tx.rawGet(curBucketIDKeyName)
	if cur == nil {
		return [4]byte{}, makeDbErr(database.ErrCorruption,
			"current bucket id counter missing", nil)
	}
	n := binary.BigEndian.Uint32(cur) + 1
	var next [4]byte
	binary.BigEndian.PutUint32(next[:], n)
	if err := tx.rawPut(curBucketIDKeyName, next[:]); err != nil {
		return [4]byte{}, err
	}
	return next, nil
}
