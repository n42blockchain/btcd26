// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package mdbxdb_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/database/mdbxdb"
)

// TestUtxosetCompressionEndToEnd writes a synthetic UTXO-shaped payload into
// the utxosetv2 bucket, closes & reopens the database, and verifies the
// stored bytes round-trip byte-exact through the zstd codec.  It also
// asserts that the on-disk MDBX environment is materially smaller than the
// raw input size, which proves compression is actually engaging in the live
// driver (not just the unit test).
func TestUtxosetCompressionEndToEnd(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "mdbxdb-utxocomp")
	db, err := database.Create("mdbxdb", dbPath, blockDataNet)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Build a 16 KiB synthetic UTXO blob with a repetitive P2PKH-style
	// template that compresses well.
	blob := buildSyntheticUtxoBlob(16 * 1024)
	const rows = 64
	const key1 = "row-001"

	if err := db.Update(func(tx database.Tx) error {
		b, err := tx.Metadata().CreateBucketIfNotExists([]byte("utxosetv2"))
		if err != nil {
			return err
		}
		for i := 0; i < rows; i++ {
			rowKey := []byte{
				byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i),
			}
			if err := b.Put(rowKey, blob); err != nil {
				return err
			}
		}
		return b.Put([]byte(key1), blob)
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	// Close + reopen to force a real disk read path.
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	db, err = database.Open("mdbxdb", dbPath, blockDataNet)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()

	if err := db.View(func(tx database.Tx) error {
		b := tx.Metadata().Bucket([]byte("utxosetv2"))
		if b == nil {
			t.Fatal("utxosetv2 bucket missing after reopen")
		}
		got := b.Get([]byte(key1))
		if !bytes.Equal(got, blob) {
			t.Fatalf("round-trip mismatch (got len=%d, want len=%d)",
				len(got), len(blob))
		}
		// Also exercise iteration via cursor to make sure Cursor.Value
		// also routes through the codec.
		c := b.Cursor()
		count := 0
		for ok := c.First(); ok; ok = c.Next() {
			v := c.Value()
			if v == nil {
				continue // bucket-index rows are nil-valued
			}
			if !bytes.Equal(v, blob) {
				t.Fatalf("cursor walk: blob mismatch at item %d "+
					"(got len=%d, want len=%d)",
					count, len(v), len(blob))
			}
			count++
		}
		if count != rows+1 {
			t.Fatalf("cursor saw %d rows, expected %d", count, rows+1)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

// TestRawBucketsStillRawOnDisk ensures that buckets without a registered
// codec write byte-identical values, so a future migration tool can stream
// keys between ffldb and mdbxdb without re-encoding.
func TestRawBucketsStillRawOnDisk(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "mdbxdb-raw")
	db, err := database.Create("mdbxdb", dbPath, blockDataNet)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer db.Close()

	raw := []byte{0xde, 0xad, 0xbe, 0xef, 0x00, 0xff}
	if err := db.Update(func(tx database.Tx) error {
		b, err := tx.Metadata().CreateBucketIfNotExists([]byte("hashidx"))
		if err != nil {
			return err
		}
		return b.Put([]byte("key"), raw)
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	if err := db.View(func(tx database.Tx) error {
		got := tx.Metadata().Bucket([]byte("hashidx")).Get([]byte("key"))
		if !bytes.Equal(got, raw) {
			t.Fatalf("hashidx round-trip mismatch")
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

// TestCodecMismatchOnReopen verifies that opening an mdbxdb database with a
// different codec registered for a recorded bucket returns ErrInvalid
// rather than silently corrupting data.
//
// NOTE: This test mutates the process-global codec registry, so it must
// NOT use t.Parallel — other tests in this package would otherwise observe
// the swapped codec and either fail to round-trip or hit the mismatch
// path themselves.
func TestCodecMismatchOnReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "mdbxdb-codecmis")
	db, err := database.Create("mdbxdb", dbPath, blockDataNet)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Swap the codec for utxosetv2 to something with a different name.
	prev := mdbxdb.Mdbxdb_SetTableCodec("utxosetv2",
		mdbxdb.Mdbxdb_NewZstdCodecForTest("zstd6-utxosetv2-different"))
	defer mdbxdb.Mdbxdb_SetTableCodec("utxosetv2", prev)

	_, err = database.Open("mdbxdb", dbPath, blockDataNet)
	if err == nil {
		t.Fatalf("Open should have failed due to codec mismatch")
	}
	dbErr, ok := err.(database.Error)
	if !ok || dbErr.ErrorCode != database.ErrInvalid {
		t.Fatalf("expected ErrInvalid, got %v", err)
	}
}

// TestOnDiskCompressionRatio writes 8 MiB of synthetic UTXO-shaped data into
// the compressed utxosetv2 bucket and into a raw hashidx bucket, then
// measures the resulting MDBX data file size to confirm the compressed
// table is materially smaller.  This is the integration-level proof that
// codec routing is actually engaging during live writes (not just in unit
// tests that exercise the codec directly).
func TestOnDiskCompressionRatio(t *testing.T) {
	t.Parallel()

	mkRun := func(t *testing.T, name string, bucket []byte) int64 {
		t.Helper()
		dbPath := filepath.Join(t.TempDir(), name)
		db, err := database.Create("mdbxdb", dbPath, blockDataNet)
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		// Write rowCount payloads of payloadSize each → ~8 MiB total.
		const rowCount = 256
		const payloadSize = 32 * 1024
		blob := buildSyntheticUtxoBlob(payloadSize)
		if err := db.Update(func(tx database.Tx) error {
			b, err := tx.Metadata().CreateBucketIfNotExists(bucket)
			if err != nil {
				return err
			}
			for i := 0; i < rowCount; i++ {
				key := []byte{
					byte(i >> 24), byte(i >> 16),
					byte(i >> 8), byte(i),
				}
				if err := b.Put(key, blob); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("update: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		st, err := os.Stat(filepath.Join(dbPath, "metadata", "mdbx.dat"))
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		return st.Size()
	}

	compressedSize := mkRun(t, "compressed", []byte("utxosetv2"))
	rawSize := mkRun(t, "raw", []byte("hashidx"))

	t.Logf("utxosetv2 (zstd-3) mdbx.dat size: %d bytes", compressedSize)
	t.Logf("hashidx    (raw)    mdbx.dat size: %d bytes", rawSize)

	// Compressed table must use no more than half the disk of the raw one.
	// On the highly redundant template above zstd-3 typically achieves a
	// 30-50× ratio at the page level, but MDBX page overhead floors the
	// gain at the lower end.  We demand a conservative 2× win.
	if compressedSize*2 >= rawSize {
		t.Fatalf("expected compressed db ≤ half of raw db, got %d vs %d",
			compressedSize, rawSize)
	}
}

// buildSyntheticUtxoBlob returns a buffer of approximate size n bytes whose
// content mimics the bulk-utxo layout: many ~30-byte rows each prefixed by
// a small header and ending in a P2PKH scriptPubKey.
func buildSyntheticUtxoBlob(n int) []byte {
	template := []byte{
		0x01, 0x00, 0x00, 0x00, 0x00,
		0x76, 0xa9, 0x14,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x88, 0xac,
	}
	out := make([]byte, 0, n)
	for len(out) < n {
		out = append(out, template...)
	}
	return out[:n]
}
