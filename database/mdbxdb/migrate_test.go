// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package mdbxdb_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/database"
	_ "github.com/btcsuite/btcd/database/ffldb"
	"github.com/btcsuite/btcd/database/mdbxdb"
)

// TestFfldbToMdbxdbMigration creates an ffldb database with a handful of
// blocks and nested metadata, runs the in-package migration helpers
// (mirroring what cmd/btcd-migrate does), reopens the destination through
// the regular driver, and verifies that every key and every block
// round-trips byte-exact.
func TestFfldbToMdbxdbMigration(t *testing.T) {
	srcDir := filepath.Join(t.TempDir(), "src-ffldb")
	dstDir := filepath.Join(t.TempDir(), "dst-mdbxdb")

	// --- 1. Populate the source ffldb. -----------------------------------
	srcDB, err := database.Create("ffldb", srcDir, blockDataNet)
	if err != nil {
		t.Fatalf("create ffldb: %v", err)
	}

	blocks, err := loadBlocks(t, blockDataFile, blockDataNet)
	if err != nil {
		t.Fatalf("load test blocks: %v", err)
	}

	// Store a few blocks + some nested metadata.
	const writtenBlocks = 16
	if err := srcDB.Update(func(tx database.Tx) error {
		for i := 0; i < writtenBlocks; i++ {
			if err := tx.StoreBlock(blocks[i]); err != nil {
				return err
			}
		}
		root := tx.Metadata()
		b1, err := root.CreateBucket([]byte("bucket1"))
		if err != nil {
			return err
		}
		if err := b1.Put([]byte("k1"), []byte("v1")); err != nil {
			return err
		}
		if err := b1.Put([]byte("k2"), []byte("v2-longer-value-with-content")); err != nil {
			return err
		}
		b2, err := b1.CreateBucket([]byte("nested"))
		if err != nil {
			return err
		}
		if err := b2.Put([]byte("nkey"), []byte("nval")); err != nil {
			return err
		}
		return root.Put([]byte("rootkey"), []byte("rootval"))
	}); err != nil {
		t.Fatalf("populate ffldb: %v", err)
	}
	if err := srcDB.Close(); err != nil {
		t.Fatalf("close ffldb: %v", err)
	}

	// --- 2. Migrate. -----------------------------------------------------
	if err := os.MkdirAll(dstDir, 0o700); err != nil {
		t.Fatalf("mkdir dst: %v", err)
	}
	if err := copyFdbFiles(srcDir, dstDir); err != nil {
		t.Fatalf("copy .fdb files: %v", err)
	}

	srcDB, err = database.Open("ffldb", srcDir, blockDataNet)
	if err != nil {
		t.Fatalf("reopen ffldb: %v", err)
	}
	dst, err := mdbxdb.Mdbxdb_OpenForMigration(dstDir, blockDataNet)
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}

	if err := dst.Update(func(dstTx database.Tx) error {
		if err := mdbxdb.Mdbxdb_InitMigrationMeta(dstTx); err != nil {
			return err
		}
		return srcDB.View(func(srcTx database.Tx) error {
			writeRow := srcTx.Metadata().Get([]byte("ffldb-writeloc"))
			if writeRow == nil {
				t.Fatal("src missing writeLoc")
			}
			fileNum := uint32(writeRow[0]) | uint32(writeRow[1])<<8 |
				uint32(writeRow[2])<<16 | uint32(writeRow[3])<<24
			offset := uint32(writeRow[4]) | uint32(writeRow[5])<<8 |
				uint32(writeRow[6])<<16 | uint32(writeRow[7])<<24
			if err := mdbxdb.Mdbxdb_WriteWriteLoc(dstTx, fileNum, offset); err != nil {
				return err
			}
			return copyBucketRecursive(srcTx.Metadata(), dstTx.Metadata(), true)
		})
	}); err != nil {
		t.Fatalf("migrate update: %v", err)
	}
	if err := dst.Close(); err != nil {
		t.Fatalf("close dst: %v", err)
	}
	srcDB.Close()

	// --- 3. Reopen dst with the regular driver and verify. ---------------
	migDB, err := database.Open("mdbxdb", dstDir, blockDataNet)
	if err != nil {
		t.Fatalf("regular Open of migrated dst: %v", err)
	}
	defer migDB.Close()

	if err := migDB.View(func(tx database.Tx) error {
		// Spot-check the nested keys.
		root := tx.Metadata()
		if got := root.Get([]byte("rootkey")); !bytes.Equal(got, []byte("rootval")) {
			t.Fatalf("rootkey mismatch: %q", got)
		}
		b1 := root.Bucket([]byte("bucket1"))
		if b1 == nil {
			t.Fatal("bucket1 missing")
		}
		if got := b1.Get([]byte("k1")); !bytes.Equal(got, []byte("v1")) {
			t.Fatalf("k1 mismatch: %q", got)
		}
		if got := b1.Get([]byte("k2")); !bytes.Equal(got, []byte("v2-longer-value-with-content")) {
			t.Fatalf("k2 mismatch: %q", got)
		}
		b2 := b1.Bucket([]byte("nested"))
		if b2 == nil {
			t.Fatal("nested missing")
		}
		if got := b2.Get([]byte("nkey")); !bytes.Equal(got, []byte("nval")) {
			t.Fatalf("nkey mismatch: %q", got)
		}

		// Every block must read back byte-exact.
		for i := 0; i < writtenBlocks; i++ {
			h := blocks[i].Hash()
			gotBytes, err := tx.FetchBlock(h)
			if err != nil {
				return err
			}
			wantBytes, _ := blocks[i].Bytes()
			if !bytes.Equal(gotBytes, wantBytes) {
				t.Fatalf("block %d (%s) mismatch", i, h)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

// copyFdbFiles is a test-local re-implementation of the cmd helper.
func copyFdbFiles(srcDir, dstDir string) error {
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) < 5 || name[len(name)-4:] != ".fdb" {
			continue
		}
		in, err := os.Open(filepath.Join(srcDir, name))
		if err != nil {
			return err
		}
		out, err := os.Create(filepath.Join(dstDir, name))
		if err != nil {
			in.Close()
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			in.Close()
			out.Close()
			return err
		}
		in.Close()
		if err := out.Close(); err != nil {
			return err
		}
	}
	return nil
}

// copyBucketRecursive mirrors the cmd helper.  The skipWriteLoc parameter
// is true for the root bucket (whose writeLoc is written explicitly) and
// false otherwise.
func copyBucketRecursive(srcBucket, dstBucket database.Bucket, skipWriteLoc bool) error {
	if err := srcBucket.ForEach(func(k, v []byte) error {
		if skipWriteLoc && string(k) == "ffldb-writeloc" {
			return nil
		}
		kc := append([]byte(nil), k...)
		vc := append([]byte(nil), v...)
		return dstBucket.Put(kc, vc)
	}); err != nil {
		return err
	}
	return srcBucket.ForEachBucket(func(name []byte) error {
		nc := append([]byte(nil), name...)
		childSrc := srcBucket.Bucket(nc)
		if childSrc == nil {
			return nil
		}
		childDst, err := dstBucket.CreateBucketIfNotExists(nc)
		if err != nil {
			return err
		}
		return copyBucketRecursive(childSrc, childDst, false)
	})
}

// Unused import compatibility: pull btcutil/chainhash so the file builds
// even before tests reference them.
var _ = btcutil.NewBlock
var _ chainhash.Hash
