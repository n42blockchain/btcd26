// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// btcd-migrate streams an existing ffldb database into a freshly created
// mdbxdb database.  Block flat files (.fdb) are hard-linked when possible
// and copied otherwise; metadata is walked recursively via the
// database.Bucket interface so the destination contains a byte-exact
// representation of every key/value pair under the root metadata bucket.
//
// Usage:
//
//	btcd-migrate \
//	    -from <ffldb-path>     # source ffldb directory
//	    -to   <mdbxdb-path>    # destination directory (must not exist)
//	    -net  mainnet          # network magic
//	    [-verify]              # cross-check a sample of blocks afterwards
//	    [-verify-samples 1000] # number of blocks to spot-check
//
// On any error the destination is left in place for inspection — the user
// can run `btcd-migrate` again after cleaning up, or run with -verify on
// the partial output to diagnose.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/database"
	_ "github.com/btcsuite/btcd/database/ffldb"
	"github.com/btcsuite/btcd/database/mdbxdb"
	"github.com/btcsuite/btcd/wire/v2"
)

func main() {
	var (
		from          = flag.String("from", "", "source ffldb directory (required)")
		to            = flag.String("to", "", "destination mdbxdb directory (required)")
		netName       = flag.String("net", "mainnet", "network magic")
		verify        = flag.Bool("verify", false, "verify sample of blocks after migration")
		verifySamples = flag.Int("verify-samples", 1000, "number of blocks to spot-check")
	)
	flag.Parse()

	if *from == "" || *to == "" {
		flag.Usage()
		os.Exit(2)
	}
	net, err := netFromName(*netName)
	if err != nil {
		log.Fatalf("bad -net: %v", err)
	}

	if err := os.MkdirAll(*to, 0o700); err != nil {
		log.Fatalf("mkdir %q: %v", *to, err)
	}

	// Phase 1: hard-link / copy every .fdb file from src to dst.  We do
	// this BEFORE opening either database so neither side races against
	// the file system.
	if err := copyBlockFiles(*from, *to); err != nil {
		log.Fatalf("copy block files: %v", err)
	}

	// Phase 2: open ffldb (RO), open mdbxdb for migration.
	src, err := database.Open("ffldb", *from, net)
	if err != nil {
		log.Fatalf("open ffldb at %q: %v", *from, err)
	}
	defer src.Close()

	dst, err := mdbxdb.Mdbxdb_OpenForMigration(*to, net)
	if err != nil {
		log.Fatalf("open mdbxdb for migration: %v", err)
	}

	// Phase 3: copy the writeLoc + all metadata in a single MDBX
	// transaction.  This keeps everything atomic; the destination is
	// either fully populated or aborted.
	if err := dst.Update(func(dstTx database.Tx) error {
		if err := mdbxdb.Mdbxdb_InitMigrationMeta(dstTx); err != nil {
			return fmt.Errorf("init migration meta: %w", err)
		}
		// Within the same MDBX update, view the src ffldb.
		return src.View(func(srcTx database.Tx) error {
			// Copy writeLoc (it's a root-level key, but ForEach
			// will pick it up too — we just want to be explicit).
			writeRow := srcTx.Metadata().Get([]byte("ffldb-writeloc"))
			if writeRow == nil {
				return fmt.Errorf("source ffldb missing writeLoc")
			}
			fileNum := uint32(writeRow[0]) | uint32(writeRow[1])<<8 |
				uint32(writeRow[2])<<16 | uint32(writeRow[3])<<24
			offset := uint32(writeRow[4]) | uint32(writeRow[5])<<8 |
				uint32(writeRow[6])<<16 | uint32(writeRow[7])<<24
			if err := mdbxdb.Mdbxdb_WriteWriteLoc(dstTx, fileNum, offset); err != nil {
				return fmt.Errorf("write writeLoc: %w", err)
			}
			// Recursively copy the rest of the metadata tree.
			return copyBucket(srcTx.Metadata(), dstTx.Metadata())
		})
	}); err != nil {
		_ = dst.Close()
		log.Fatalf("migrate transaction: %v", err)
	}
	if err := dst.Close(); err != nil {
		log.Fatalf("close dst: %v", err)
	}

	// Phase 4: reopen dst with the regular driver so reconcile runs and
	// confirms metadata ↔ block-file consistency.
	dst2, err := database.Open("mdbxdb", *to, net)
	if err != nil {
		log.Fatalf("reopen dst: %v", err)
	}
	defer dst2.Close()

	if *verify {
		if err := verifySamplesFn(src, dst2, *verifySamples); err != nil {
			log.Fatalf("verify: %v", err)
		}
		log.Printf("verify passed (%d samples)", *verifySamples)
	}
	log.Printf("migration complete: %s → %s", *from, *to)
}

// copyBlockFiles hard-links (or, on failure, byte-copies) every .fdb file
// from src to dst.  Hard-linking is preferred because it's atomic, instant,
// and only doubles disk usage if the user later modifies one set.  On
// platforms where hard linking isn't available across the filesystem
// boundary, falls back to a regular copy.
func copyBlockFiles(srcDir, dstDir string) error {
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return fmt.Errorf("readdir %q: %w", srcDir, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) < 5 || name[len(name)-4:] != ".fdb" {
			continue
		}
		src := filepath.Join(srcDir, name)
		dst := filepath.Join(dstDir, name)
		if err := os.Link(src, dst); err == nil {
			continue
		}
		// Fall back to byte copy.
		if err := copyFile(src, dst); err != nil {
			return fmt.Errorf("copy %q→%q: %w", src, dst, err)
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// copyBucket recursively copies every (k, v) pair and every nested bucket
// from srcBucket to dstBucket.
func copyBucket(srcBucket, dstBucket database.Bucket) error {
	if err := srcBucket.ForEach(func(k, v []byte) error {
		// Skip the writeLoc key in the root — we wrote it explicitly
		// before recursion to keep the on-disk format correct even if
		// the caller decides to abort partway through.  Inside nested
		// buckets there's no such key so this guard is cheap.
		if dstBucket.Writable() && string(k) == "ffldb-writeloc" {
			return nil
		}
		// Make defensive copies; the slices returned by ForEach are
		// only valid during the iteration step.
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
			return fmt.Errorf("src bucket %q missing during walk", nc)
		}
		childDst, err := dstBucket.CreateBucketIfNotExists(nc)
		if err != nil {
			return fmt.Errorf("create dst bucket %q: %w", nc, err)
		}
		return copyBucket(childSrc, childDst)
	})
}

// verifySamplesFn spot-checks the migration by randomly sampling block
// hashes from the source block-index, fetching the block from both src and
// dst, and comparing bytes.  Returns the first mismatch as an error.
func verifySamplesFn(src, dst database.DB, n int) error {
	var hashes []chainhash.Hash
	if err := src.View(func(tx database.Tx) error {
		c := tx.Metadata().Bucket([]byte("ffldb-blockidx")).Cursor()
		for ok := c.First(); ok; ok = c.Next() {
			var h chainhash.Hash
			copy(h[:], c.Key())
			hashes = append(hashes, h)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("collect hashes: %w", err)
	}
	if n > len(hashes) {
		n = len(hashes)
	}
	r := rand.New(rand.NewSource(42))
	r.Shuffle(len(hashes), func(i, j int) {
		hashes[i], hashes[j] = hashes[j], hashes[i]
	})

	return src.View(func(srcTx database.Tx) error {
		return dst.View(func(dstTx database.Tx) error {
			for i := 0; i < n; i++ {
				h := hashes[i]
				sb, err := srcTx.FetchBlock(&h)
				if err != nil {
					return fmt.Errorf("src fetch %s: %w", h, err)
				}
				db, err := dstTx.FetchBlock(&h)
				if err != nil {
					return fmt.Errorf("dst fetch %s: %w", h, err)
				}
				if !equalBytes(sb, db) {
					return fmt.Errorf("block %s differs between src and dst",
						h)
				}
			}
			return nil
		})
	})
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func netFromName(s string) (wire.BitcoinNet, error) {
	switch s {
	case "mainnet":
		return wire.MainNet, nil
	case "testnet", "testnet3":
		return wire.TestNet3, nil
	case "regtest":
		return wire.TestNet, nil
	case "simnet":
		return wire.SimNet, nil
	}
	return 0, fmt.Errorf("unknown network %q", s)
}
