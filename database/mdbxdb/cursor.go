// Copyright (c) 2015-2016 The btcsuite developers
// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package mdbxdb

import (
	"bytes"
	"errors"

	"github.com/btcsuite/btcd/database"
	"github.com/erigontech/mdbx-go/mdbx"
)

// cursorType selects which subset of bucket entries a cursor walks.
type cursorType int

const (
	// ctKeys iterates only the data keys (bucketID-prefixed).
	ctKeys cursorType = iota
	// ctBuckets iterates only the bucket index entries (bidx-prefixed).
	ctBuckets
	// ctFull iterates both, merged in ascending key order.
	ctFull
)

// cursor is the database.Cursor implementation backed by an MDBX cursor.
//
// A cursor walks a single physical MDBX DBI but restricts itself to one or
// two key prefixes depending on cursorType.  When ctFull is requested we
// maintain two underlying MDBX cursors (one over each prefix) and merge them
// by ascending key.
type cursor struct {
	bucket *bucket
	ctyp   cursorType

	// Underlying physical cursors.  keyCur tracks data keys (bucketID
	// prefix); bktCur tracks the bucket index (bidx + bucketID prefix).
	// Either may be nil if the cursorType doesn't need it.
	keyCur *mdbx.Cursor
	bktCur *mdbx.Cursor

	// Last positions returned by each sub-cursor; nil means exhausted.
	keyKey, keyVal []byte
	bktKey, bktVal []byte

	// curWhich tells which sub-cursor currently provides the visible
	// key/value.  Used by Delete to know which physical cursor to call
	// Del() on.  Values: "key", "bkt", or "" (exhausted).
	curWhich string

	// skipNextAdvance is set by Delete to indicate that MDBX has already
	// repositioned the underlying cursor onto the next item, so the next
	// call to advance() must not move the sub-cursor again.
	skipNextAdvance bool
}

var _ database.Cursor = (*cursor)(nil)

// newCursor allocates a cursor for the given bucket and type.
func newCursor(b *bucket, ctyp cursorType) *cursor {
	c := &cursor{bucket: b, ctyp: ctyp}

	switch ctyp {
	case ctKeys:
		c.keyCur, _ = b.tx.mtxn.OpenCursor(b.tx.db.kvDBI)
	case ctBuckets:
		c.bktCur, _ = b.tx.mtxn.OpenCursor(b.tx.db.kvDBI)
	case ctFull:
		c.keyCur, _ = b.tx.mtxn.OpenCursor(b.tx.db.kvDBI)
		c.bktCur, _ = b.tx.mtxn.OpenCursor(b.tx.db.kvDBI)
	}
	return c
}

// cursorFinalizer releases the underlying MDBX cursors.  Safe to call twice.
func cursorFinalizer(c *cursor) {
	if c.keyCur != nil {
		c.keyCur.Close()
		c.keyCur = nil
	}
	if c.bktCur != nil {
		c.bktCur.Close()
		c.bktCur = nil
	}
}

// dataPrefix returns the byte prefix that scopes a cursor to a bucket's data
// keys: just the bucket ID.
func (c *cursor) dataPrefix() []byte { return c.bucket.id[:] }

// bktPrefix returns the byte prefix that scopes a cursor to a bucket's child
// bucket index entries: bidx + parent bucket ID.
func (c *cursor) bktPrefix() []byte {
	out := make([]byte, len(bucketIndexPrefix)+4)
	copy(out, bucketIndexPrefix)
	copy(out[len(bucketIndexPrefix):], c.bucket.id[:])
	return out
}

// Bucket returns the bucket the cursor was created for.
func (c *cursor) Bucket() database.Bucket {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}
	return c.bucket
}

// Delete removes the current item.  Bucket index entries are NOT allowed to
// be deleted via cursor — matching the database.Cursor contract.
func (c *cursor) Delete() error {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return err
	}
	if !c.bucket.tx.writable {
		return makeDbErr(database.ErrTxNotWritable,
			"delete requires a writable database transaction", nil)
	}
	switch c.curWhich {
	case "":
		return makeDbErr(database.ErrIncompatibleValue,
			"cursor is exhausted", nil)
	case "bkt":
		return makeDbErr(database.ErrIncompatibleValue,
			"buckets may not be deleted from a cursor", nil)
	case "key":
		if err := c.keyCur.Del(0); err != nil {
			return convertMdbxErr("cursor Del", err)
		}
		// MDBX repositions the cursor onto the next item after a
		// successful Del.  Refresh our cached (k, v) from there and
		// arrange that the next user-visible Next/Prev call does NOT
		// step again — otherwise we'd skip the item that landed in the
		// just-deleted slot.
		c.keyKey, c.keyVal = c.readKeyAt(mdbx.GetCurrent, c.keyCur)
		c.applyDataPrefix()
		c.recomputeCurrent(true)
		c.skipNextAdvance = true
		return nil
	}
	return nil
}

// readKeyAt is a convenience that fetches the (k,v) at the given mdbx op
// from a particular cursor handle.  Returns (nil, nil) if absent / error.
func (c *cursor) readKeyAt(op uint, mc *mdbx.Cursor) ([]byte, []byte) {
	k, v, err := mc.Get(nil, nil, op)
	if err != nil {
		if errors.Is(err, mdbx.ErrNotFound) || mdbx.IsNotFound(err) {
			return nil, nil
		}
		// Treat any other error as exhausted; surface separately if needed.
		return nil, nil
	}
	return copyBytes(k), copyBytes(v)
}

func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// applyDataPrefix clears keyKey/keyVal if the current key is outside the
// bucket's data-key prefix.
func (c *cursor) applyDataPrefix() {
	if c.keyKey == nil {
		return
	}
	if !bytes.HasPrefix(c.keyKey, c.dataPrefix()) ||
		bytes.HasPrefix(c.keyKey, bucketIndexPrefix) {
		c.keyKey, c.keyVal = nil, nil
	}
}

// applyBktPrefix clears bktKey/bktVal if the current key is outside the
// bucket index prefix.
func (c *cursor) applyBktPrefix() {
	if c.bktKey == nil {
		return
	}
	if !bytes.HasPrefix(c.bktKey, c.bktPrefix()) {
		c.bktKey, c.bktVal = nil, nil
	}
}

// recomputeCurrent decides which sub-cursor wins right now and sets
// curWhich.  forwards selects ascending vs. descending order.  Returns true
// if any sub-cursor is valid.
func (c *cursor) recomputeCurrent(forwards bool) bool {
	switch c.ctyp {
	case ctKeys:
		if c.keyKey == nil {
			c.curWhich = ""
			return false
		}
		c.curWhich = "key"
		return true
	case ctBuckets:
		if c.bktKey == nil {
			c.curWhich = ""
			return false
		}
		c.curWhich = "bkt"
		return true
	case ctFull:
		switch {
		case c.keyKey == nil && c.bktKey == nil:
			c.curWhich = ""
			return false
		case c.keyKey == nil:
			c.curWhich = "bkt"
			return true
		case c.bktKey == nil:
			c.curWhich = "key"
			return true
		default:
			cmp := bytes.Compare(c.keyKey, c.bktKey)
			if forwards {
				if cmp <= 0 {
					c.curWhich = "key"
				} else {
					c.curWhich = "bkt"
				}
			} else {
				if cmp >= 0 {
					c.curWhich = "key"
				} else {
					c.curWhich = "bkt"
				}
			}
			return true
		}
	}
	return false
}

// First positions both sub-cursors at the beginning of their prefix ranges.
func (c *cursor) First() bool {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	if c.keyCur != nil {
		k, v, err := c.keyCur.Get(c.dataPrefix(), nil, mdbx.SetRange)
		if err == nil {
			c.keyKey, c.keyVal = copyBytes(k), copyBytes(v)
		} else {
			c.keyKey, c.keyVal = nil, nil
		}
		c.applyDataPrefix()
	}
	if c.bktCur != nil {
		k, v, err := c.bktCur.Get(c.bktPrefix(), nil, mdbx.SetRange)
		if err == nil {
			c.bktKey, c.bktVal = copyBytes(k), copyBytes(v)
		} else {
			c.bktKey, c.bktVal = nil, nil
		}
		c.applyBktPrefix()
	}
	return c.recomputeCurrent(true)
}

// Last positions both sub-cursors at the last entry of their prefix ranges.
//
// MDBX has no direct "Last in prefix" op; we compute the upper bound by
// taking the next-byte-after-prefix and seeking to just before it.
func (c *cursor) Last() bool {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}

	if c.keyCur != nil {
		c.keyKey, c.keyVal = lastInPrefix(c.keyCur, c.dataPrefix())
		c.applyDataPrefix()
	}
	if c.bktCur != nil {
		c.bktKey, c.bktVal = lastInPrefix(c.bktCur, c.bktPrefix())
		c.applyBktPrefix()
	}
	return c.recomputeCurrent(false)
}

// lastInPrefix returns the last (k,v) at-or-before the supremum of prefix.
func lastInPrefix(mc *mdbx.Cursor, prefix []byte) ([]byte, []byte) {
	sup := nextPrefix(prefix)
	if sup == nil {
		// Prefix is the maximal possible (all 0xff).  Last() does it.
		k, v, err := mc.Get(nil, nil, mdbx.Last)
		if err != nil {
			return nil, nil
		}
		if !bytes.HasPrefix(k, prefix) {
			return nil, nil
		}
		return copyBytes(k), copyBytes(v)
	}
	// Position at first key >= sup, then step back once.
	_, _, err := mc.Get(sup, nil, mdbx.SetRange)
	if err == nil {
		k, v, err := mc.Get(nil, nil, mdbx.Prev)
		if err == nil && bytes.HasPrefix(k, prefix) {
			return copyBytes(k), copyBytes(v)
		}
		return nil, nil
	}
	// sup > all keys: last in DB is candidate.
	k, v, err := mc.Get(nil, nil, mdbx.Last)
	if err != nil {
		return nil, nil
	}
	if !bytes.HasPrefix(k, prefix) {
		return nil, nil
	}
	return copyBytes(k), copyBytes(v)
}

// nextPrefix returns the lexicographically smallest byte sequence that is
// strictly greater than any sequence starting with prefix.  Returns nil if
// prefix consists entirely of 0xff (no such sequence exists in finite
// bytes).
func nextPrefix(prefix []byte) []byte {
	out := make([]byte, len(prefix))
	copy(out, prefix)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] < 0xff {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

// advance moves the current sub-cursor by one in the requested direction and
// reseeds the (key,val) for that sub-cursor.
func (c *cursor) advance(forwards bool) bool {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}
	if c.skipNextAdvance {
		// Delete already left us pointing at the next item; just
		// re-pick the winner without stepping the sub-cursor again.
		c.skipNextAdvance = false
		return c.recomputeCurrent(forwards)
	}
	var op uint = mdbx.Next
	if !forwards {
		op = mdbx.Prev
	}
	switch c.curWhich {
	case "key":
		k, v, err := c.keyCur.Get(nil, nil, op)
		if err == nil {
			c.keyKey, c.keyVal = copyBytes(k), copyBytes(v)
		} else {
			c.keyKey, c.keyVal = nil, nil
		}
		c.applyDataPrefix()
	case "bkt":
		k, v, err := c.bktCur.Get(nil, nil, op)
		if err == nil {
			c.bktKey, c.bktVal = copyBytes(k), copyBytes(v)
		} else {
			c.bktKey, c.bktVal = nil, nil
		}
		c.applyBktPrefix()
	default:
		return false
	}
	return c.recomputeCurrent(forwards)
}

// Next advances forward by one.
func (c *cursor) Next() bool { return c.advance(true) }

// Prev moves backward by one.
func (c *cursor) Prev() bool { return c.advance(false) }

// Seek positions the cursor at the first user-key >= seek (within this
// bucket's data prefix).  The bucket index sub-cursor is positioned at the
// equivalent point so that ctFull merging works after a Seek.
func (c *cursor) Seek(seek []byte) bool {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return false
	}
	seekKey := bucketizedKey(c.bucket.id, seek)

	if c.keyCur != nil {
		k, v, err := c.keyCur.Get(seekKey, nil, mdbx.SetRange)
		if err == nil {
			c.keyKey, c.keyVal = copyBytes(k), copyBytes(v)
		} else {
			c.keyKey, c.keyVal = nil, nil
		}
		c.applyDataPrefix()
	}
	if c.bktCur != nil {
		// Compose an equivalent seek key in the bucket-index namespace:
		// bidx + parentID + (user-name).  For pure data Seeks, also
		// position the bucket cursor so we keep ordering consistent.
		seekBkt := bucketIndexKey(c.bucket.id, seek)
		k, v, err := c.bktCur.Get(seekBkt, nil, mdbx.SetRange)
		if err == nil {
			c.bktKey, c.bktVal = copyBytes(k), copyBytes(v)
		} else {
			c.bktKey, c.bktVal = nil, nil
		}
		c.applyBktPrefix()
	}
	return c.recomputeCurrent(true)
}

// Key returns the user-visible key portion of the current item.  For data
// rows that's the bytes after the bucket ID; for bucket index rows that's
// the bytes after bidx+parentID.
func (c *cursor) Key() []byte {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}
	switch c.curWhich {
	case "key":
		// Strip the 4-byte bucket ID prefix.
		return append([]byte(nil), c.keyKey[4:]...)
	case "bkt":
		// Strip "bidx" + 4-byte parent ID.
		return append([]byte(nil), c.bktKey[len(bucketIndexPrefix)+4:]...)
	}
	return nil
}

// Value returns the current value.  For bucket index rows we return nil to
// match the database.Cursor contract.
func (c *cursor) Value() []byte {
	if err := c.bucket.tx.checkClosed(); err != nil {
		return nil
	}
	switch c.curWhich {
	case "key":
		decoded, err := codecFor(c.bucket.path).Decode(c.keyVal)
		if err != nil {
			log.Errorf("cursor decode failure in bucket %q: %v",
				c.bucket.path, err)
			return nil
		}
		return decoded
	case "bkt":
		return nil
	}
	return nil
}

// --- internal "raw" cursor used by DeleteBucket ----------------------------

// rawCursor is a lower-level cursor that just walks raw MDBX keys with no
// bucket virtualization.  Used by tx.deletePrefix and DeleteBucket.
type rawCursor struct {
	c   *mdbx.Cursor
	k   []byte
	v   []byte
	end bool
}

func (tx *transaction) newRawCursor() (*rawCursor, error) {
	c, err := tx.mtxn.OpenCursor(tx.db.kvDBI)
	if err != nil {
		return nil, convertMdbxErr("open raw cursor", err)
	}
	return &rawCursor{c: c}, nil
}

func (rc *rawCursor) seekPrefix(prefix []byte) bool {
	k, v, err := rc.c.Get(prefix, nil, mdbx.SetRange)
	if err != nil || !bytes.HasPrefix(k, prefix) {
		rc.end = true
		return false
	}
	rc.k, rc.v = copyBytes(k), copyBytes(v)
	return true
}

func (rc *rawCursor) nextWithPrefix(prefix []byte) bool {
	k, v, err := rc.c.Get(nil, nil, mdbx.Next)
	if err != nil || !bytes.HasPrefix(k, prefix) {
		rc.end = true
		return false
	}
	rc.k, rc.v = copyBytes(k), copyBytes(v)
	return true
}

func (rc *rawCursor) key() []byte   { return rc.k }
func (rc *rawCursor) value() []byte { return rc.v }
func (rc *rawCursor) del() error {
	if err := rc.c.Del(0); err != nil {
		return convertMdbxErr("raw cursor Del", err)
	}
	return nil
}
func (rc *rawCursor) close() {
	if rc.c != nil {
		rc.c.Close()
		rc.c = nil
	}
}
