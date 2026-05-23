// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package mdbxdb

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/klauspost/compress/zstd"
)

// TestIdentityCodecRoundTrip verifies that the raw codec preserves every
// kind of input — nil, empty, random.
func TestIdentityCodecRoundTrip(t *testing.T) {
	t.Parallel()
	cases := [][]byte{
		nil,
		{},
		{0x00},
		bytes.Repeat([]byte{0xab}, 1024),
		randomBytes(1 << 14),
	}
	c := identityCodec{}
	for i, in := range cases {
		enc, err := c.Encode(in)
		if err != nil {
			t.Fatalf("case %d: encode: %v", i, err)
		}
		dec, err := c.Decode(enc)
		if err != nil {
			t.Fatalf("case %d: decode: %v", i, err)
		}
		if !bytes.Equal(dec, in) && !(len(dec) == 0 && len(in) == 0) {
			t.Fatalf("case %d: round-trip mismatch", i)
		}
	}
}

// TestZstdCodecRoundTrip verifies zstd encode/decode round-trips for empty,
// small and large payloads with both natural-language-like and
// high-entropy data.
func TestZstdCodecRoundTrip(t *testing.T) {
	t.Parallel()
	c := newZstdCodec("zstd3-test", zstd.SpeedDefault)
	cases := [][]byte{
		nil,
		{},
		{0x42},
		[]byte("a quick brown fox jumps over the lazy dog"),
		bytes.Repeat([]byte("repeated pattern "), 256),
		randomBytes(1 << 16),
	}
	for i, in := range cases {
		enc, err := c.Encode(in)
		if err != nil {
			t.Fatalf("case %d encode: %v", i, err)
		}
		dec, err := c.Decode(enc)
		if err != nil {
			t.Fatalf("case %d decode: %v", i, err)
		}
		if !bytes.Equal(dec, in) && !(len(dec) == 0 && len(in) == 0) {
			t.Fatalf("case %d: round-trip mismatch (in len=%d, dec len=%d)",
				i, len(in), len(dec))
		}
	}
}

// TestDictZstdCodecRoundTrip verifies that a dictionary-bound codec
// round-trips data losslessly given that the same dictionary is used at
// decode time.
func TestDictZstdCodecRoundTrip(t *testing.T) {
	t.Parallel()
	dict := bytes.Repeat([]byte("OP_DUP OP_HASH160 OP_EQUALVERIFY OP_CHECKSIG"), 128)
	c := newDictZstdCodec("zstd-dict-utxotest", zstd.SpeedDefault, dict)
	cases := [][]byte{
		nil,
		{},
		bytes.Repeat([]byte("OP_DUP OP_HASH160 "), 32),
		randomBytes(4096),
	}
	for i, in := range cases {
		enc, err := c.Encode(in)
		if err != nil {
			t.Fatalf("case %d encode: %v", i, err)
		}
		dec, err := c.Decode(enc)
		if err != nil {
			t.Fatalf("case %d decode: %v", i, err)
		}
		if !bytes.Equal(dec, in) && !(len(dec) == 0 && len(in) == 0) {
			t.Fatalf("case %d: round-trip mismatch", i)
		}
	}
}

// TestRegistryScoping ensures that the codec registry only matches at the
// top-level bucket and never bleeds into nested buckets.
func TestRegistryScoping(t *testing.T) {
	t.Parallel()
	r := newCodecRegistry()
	tag := newZstdCodec("tagged-utxoset", zstd.SpeedDefault)
	r.SetCodec("utxosetv2", tag)

	cases := []struct {
		path     [][]byte
		wantName string
	}{
		{[][]byte{[]byte("utxosetv2")}, "tagged-utxoset"},
		{[][]byte{[]byte("spendjournal")}, "zstd3-spendjournal"},
		{[][]byte{[]byte("addridx")}, "zstd6-addridx"},
		{[][]byte{[]byte("blockheaderidx")}, "raw"},
		{[][]byte{[]byte("utxosetv2"), []byte("nested")}, "raw"},
		{[][]byte{[]byte("anything-else")}, "raw"},
		{nil, "raw"},
	}
	for _, tc := range cases {
		got := r.CodecFor(tc.path).Name()
		if got != tc.wantName {
			t.Errorf("path=%q got codec=%q want=%q", tc.path, got, tc.wantName)
		}
	}
}

// TestCodecCompressionRatio is a sanity check: zstd-3 on a 64 KiB blob of
// repeated material must compress strictly below 10% of input size.
// Failing this indicates the encoder isn't actually engaging.
func TestCodecCompressionRatio(t *testing.T) {
	t.Parallel()
	template := append([]byte{0x76, 0xa9, 0x14}, make([]byte, 20)...)
	template = append(template, 0x88, 0xac)
	input := bytes.Repeat(template, 4096) // 100 KiB

	c := newZstdCodec("zstd3-test", zstd.SpeedDefault)
	enc, err := c.Encode(input)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if len(enc) >= len(input)/10 {
		t.Fatalf("expected compression to ≤ 10%% of input, got %d/%d",
			len(enc), len(input))
	}
}

// randomBytes returns n random bytes from a fixed-seed RNG so tests are
// reproducible.
func randomBytes(n int) []byte {
	r := rand.New(rand.NewSource(42))
	b := make([]byte, n)
	_, _ = r.Read(b)
	return b
}
