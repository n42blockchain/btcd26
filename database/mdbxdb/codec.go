// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package mdbxdb

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// encodeBufPool pools scratch buffers used to receive codec-encoded
// output during hot-path bucket Puts.  Without pooling, every Put
// allocates a fresh buffer, translating to millions of make() calls
// per UTXO flush and dominating GC scan time.  Callers must bracket
// usage with encodeBufGet / encodeBufPut and not retain the slice.
var encodeBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 256)
		return &b
	},
}

// encodeBufGet returns a pointer to a scratch buffer from the pool.
// The returned slice has length zero; its capacity may grow during
// use via EncodeAppend.  Call encodeBufPut to release back.
func encodeBufGet() *[]byte {
	bp := encodeBufPool.Get().(*[]byte)
	*bp = (*bp)[:0]
	return bp
}

// encodeBufPut releases a buffer back to the pool.  The caller MUST
// have updated *bp to reference the final (possibly grown) slice
// before calling this, so the pool retains the larger backing array
// for the next user.
func encodeBufPut(bp *[]byte) {
	encodeBufPool.Put(bp)
}

// Codec encodes a value for storage and decodes a stored value for use.  It
// is keyed by the *logical* top-level bucket name (e.g. "utxosetv2") and
// applied transparently at Put/Get/Cursor.Value time.
//
// Codec implementations MUST round-trip bytes exactly, including empty input.
type Codec interface {
	// Name returns a stable identifier persisted to the meta table so a
	// future process can verify it understands the encoding.
	Name() string

	// Encode returns the on-disk representation of value.  The returned
	// slice must NOT alias the input.
	Encode(value []byte) ([]byte, error)

	// Decode returns the logical representation of stored.  The returned
	// slice must NOT alias the input.
	Decode(stored []byte) ([]byte, error)
}

// identityCodec is a no-op codec used by default and by every table that is
// either incompressible (block index, txindex, cfindex) or whose values are
// too small for compression to pay off (1-byte flags, short ints).
type identityCodec struct{}

func (identityCodec) Name() string { return "raw" }

func (identityCodec) Encode(v []byte) ([]byte, error) {
	if len(v) == 0 {
		return v, nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

func (identityCodec) Decode(v []byte) ([]byte, error) {
	if len(v) == 0 {
		return v, nil
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, nil
}

// rawCodec is the default codec for any unregistered bucket.
var rawCodec Codec = identityCodec{}

// zstdCodec is a zstd codec with a fixed encoder level.  No dictionary; this
// is the fallback when no trained dictionary is available for a table.
type zstdCodec struct {
	level int
	name  string

	encOnce sync.Once
	enc     *zstd.Encoder

	decOnce sync.Once
	dec     *zstd.Decoder
}

// newZstdCodec returns a zstd codec at the requested level.  The name is
// persisted to the meta table; bumping it forces a future process to either
// understand the new variant or refuse to open the database.
func newZstdCodec(name string, level zstd.EncoderLevel) *zstdCodec {
	return &zstdCodec{name: name, level: int(level)}
}

func (c *zstdCodec) Name() string { return c.name }

func (c *zstdCodec) encoder() *zstd.Encoder {
	c.encOnce.Do(func() {
		enc, err := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(c.level)),
			zstd.WithEncoderConcurrency(1))
		if err != nil {
			// Fall back to default level on any setup error.
			enc, _ = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		}
		c.enc = enc
	})
	return c.enc
}

func (c *zstdCodec) decoder() *zstd.Decoder {
	c.decOnce.Do(func() {
		dec, err := zstd.NewReader(nil,
			zstd.WithDecoderConcurrency(1))
		if err != nil {
			dec, _ = zstd.NewReader(nil)
		}
		c.dec = dec
	})
	return c.dec
}

func (c *zstdCodec) Encode(v []byte) ([]byte, error) {
	// Preserve empty distinction: an empty value round-trips to an empty
	// value on disk.  This matters for the interface tests that use
	// (key, nil) pairs and rely on bytes.Equal-style comparisons.
	if len(v) == 0 {
		return v, nil
	}
	return c.encoder().EncodeAll(v, make([]byte, 0, len(v)/2)), nil
}

// EncodeAppend appends the zstd-compressed form of v to dst and returns
// the resulting slice.  This is the allocation-free path: callers
// (typically bucket.Put) hand in a pooled buffer and put it back after
// the encoded bytes have been copied into the database, avoiding one
// per-Put make().  Concrete codec types implement this so the bucket
// layer can use a single hot-path without runtime type checks.
func (c *zstdCodec) EncodeAppend(dst, v []byte) ([]byte, error) {
	if len(v) == 0 {
		return dst, nil
	}
	return c.encoder().EncodeAll(v, dst), nil
}

func (c *dictZstdCodec) EncodeAppend(dst, v []byte) ([]byte, error) {
	if len(v) == 0 {
		return dst, nil
	}
	return c.encoder().EncodeAll(v, dst), nil
}

func (identityCodec) EncodeAppend(dst, v []byte) ([]byte, error) {
	if len(v) == 0 {
		return dst, nil
	}
	return append(dst, v...), nil
}

// appendEncoder is the optional fast-path interface a Codec may
// implement to avoid the per-Put allocation that Encode requires.
type appendEncoder interface {
	EncodeAppend(dst, v []byte) ([]byte, error)
}

// encodeInto runs Codec.EncodeAppend if the codec supports it, falling
// back to Encode + append.  Returns the encoded slice (potentially
// backed by dst).
func encodeInto(codec Codec, dst, v []byte) ([]byte, error) {
	if ae, ok := codec.(appendEncoder); ok {
		return ae.EncodeAppend(dst, v)
	}
	out, err := codec.Encode(v)
	if err != nil {
		return nil, err
	}
	return append(dst, out...), nil
}

func (c *zstdCodec) Decode(stored []byte) ([]byte, error) {
	if len(stored) == 0 {
		return stored, nil
	}
	out, err := c.decoder().DecodeAll(stored, make([]byte, 0, len(stored)*3))
	if err != nil {
		return nil, fmt.Errorf("zstd decode: %w", err)
	}
	return out, nil
}

// dictZstdCodec layers a pre-trained dictionary onto zstd for tables whose
// content has strong recurring patterns (e.g. UTXO P2PKH/P2WPKH/P2TR script
// templates).  The dictionary bytes are bound at construction time and the
// same bytes are required at decode time, so dictionary upgrades must use a
// new codec name to stay forward-compatible.
type dictZstdCodec struct {
	name  string
	level int
	dict  []byte

	encOnce sync.Once
	enc     *zstd.Encoder

	decOnce sync.Once
	dec     *zstd.Decoder
}

func newDictZstdCodec(name string, level zstd.EncoderLevel, dict []byte) *dictZstdCodec {
	cp := make([]byte, len(dict))
	copy(cp, dict)
	return &dictZstdCodec{name: name, level: int(level), dict: cp}
}

func (c *dictZstdCodec) Name() string { return c.name }

func (c *dictZstdCodec) encoder() *zstd.Encoder {
	c.encOnce.Do(func() {
		enc, err := zstd.NewWriter(nil,
			zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(c.level)),
			zstd.WithEncoderDict(c.dict),
			zstd.WithEncoderConcurrency(1))
		if err != nil {
			enc, _ = zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		}
		c.enc = enc
	})
	return c.enc
}

func (c *dictZstdCodec) decoder() *zstd.Decoder {
	c.decOnce.Do(func() {
		dec, err := zstd.NewReader(nil,
			zstd.WithDecoderDicts(c.dict),
			zstd.WithDecoderConcurrency(1))
		if err != nil {
			dec, _ = zstd.NewReader(nil)
		}
		c.dec = dec
	})
	return c.dec
}

func (c *dictZstdCodec) Encode(v []byte) ([]byte, error) {
	if len(v) == 0 {
		return v, nil
	}
	return c.encoder().EncodeAll(v, make([]byte, 0, len(v)/2)), nil
}

func (c *dictZstdCodec) Decode(stored []byte) ([]byte, error) {
	if len(stored) == 0 {
		return stored, nil
	}
	out, err := c.decoder().DecodeAll(stored, make([]byte, 0, len(stored)*3))
	if err != nil {
		return nil, fmt.Errorf("zstd-dict decode: %w", err)
	}
	return out, nil
}

// codecRegistry maps a top-level bucket name to its Codec.  Lookups for
// unknown buckets or for sub-buckets (path length > 1) always return the
// identity codec, so application code can introduce new nested structures
// without worrying about compression aliasing.
type codecRegistry struct {
	entries map[string]Codec
}

// newCodecRegistry returns the default registry for P1.  Defaults match the
// plan: zstd-3 on the bulk tables, raw on everything else.  P2 can override
// individual entries via SetCodec when a trained dictionary is available.
func newCodecRegistry() *codecRegistry {
	r := &codecRegistry{entries: make(map[string]Codec)}
	// Bulk tables that benefit from compression.  Level 3 keeps IBD write
	// path fast; tighter levels can be set for cold archival jobs.
	r.entries["utxosetv2"] = newZstdCodec("zstd3-utxosetv2", zstd.SpeedDefault)
	r.entries["spendjournal"] = newZstdCodec("zstd3-spendjournal", zstd.SpeedDefault)
	// addridx values are large ordered txid lists with very high
	// inter-value redundancy — a tighter level pays off.
	r.entries["addridx"] = newZstdCodec("zstd6-addridx", zstd.SpeedBetterCompression)
	return r
}

// SetCodec replaces (or installs) the codec for a top-level bucket.  Intended
// to be used at db open time, before any reads or writes happen.
func (r *codecRegistry) SetCodec(bucketName string, c Codec) {
	r.entries[bucketName] = c
}

// CodecFor returns the codec for the given logical bucket path.  Only the
// first element is considered, and only when the path has length exactly 1.
// Nested buckets always use the raw codec.
func (r *codecRegistry) CodecFor(path [][]byte) Codec {
	if len(path) != 1 {
		return rawCodec
	}
	c, ok := r.entries[string(path[0])]
	if !ok {
		return rawCodec
	}
	return c
}

// defaultRegistry is the process-wide registry used by every database opened
// after init.  Tests can replace individual entries via SetCodec.
var defaultRegistry = newCodecRegistry()

// codecFor is the package-level entry point used by bucket.Put/Get and
// cursor.Value.  Going through the registry indirectly keeps the bucket
// implementation free of any compression-specific knowledge.
func codecFor(path [][]byte) Codec {
	return defaultRegistry.CodecFor(path)
}

// SetTableCodec installs a custom Codec for the named top-level bucket.  Must
// be called *before* any transaction touches the bucket; otherwise stored
// rows will be uninterpretable to the new codec.  Returns the previous codec
// if any.
func SetTableCodec(bucketName string, c Codec) Codec {
	prev := defaultRegistry.entries[bucketName]
	defaultRegistry.entries[bucketName] = c
	return prev
}

// --- helpers used by codecsTrainable for in-test dictionary use ----------

// readAllNonEmpty is a small helper used by tests that need to walk a stream
// of small candidate samples (e.g. UTXOs) and concatenate them.
func readAllNonEmpty(samples [][]byte) []byte {
	var buf bytes.Buffer
	for _, s := range samples {
		if len(s) == 0 {
			continue
		}
		buf.Write(s)
	}
	return buf.Bytes()
}
