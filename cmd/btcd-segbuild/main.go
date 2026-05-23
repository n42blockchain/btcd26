// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// btcd-segbuild rolls the oldest hot .fdb block files of an mdbxdb database
// into a single immutable cold segment, then rewrites the block-index rows
// for blocks that moved so that subsequent FetchBlock calls route through
// the segment.  The hot files become safe to delete (manually for now —
// a future revision will gate this behind a successful integrity check).
//
// Usage:
//
//	btcd-segbuild -db <path> -hot-keep 4032 [-dict <file>] [-segnum N]
//
// The -hot-keep flag selects how many of the most recent block heights stay
// as hot .fdb files; older blocks are packed.  Blocks within the same
// (file, offset) ordering are packed into ~2 MiB frames inside the cold
// segment.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sort"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/database"
	_ "github.com/btcsuite/btcd/database/mdbxdb"
	"github.com/btcsuite/btcd/database/mdbxdb"
	"github.com/btcsuite/btcd/wire/v2"
)

func main() {
	var (
		dbPath  = flag.String("db", "", "mdbxdb database path (required)")
		hotKeep = flag.Int("hot-keep", 4032, "number of most recent blocks to keep hot")
		dictF   = flag.String("dict", "", "zstd-trained dictionary file (optional)")
		segNum  = flag.Uint("segnum", 0, "cold segment number to write (must be unused)")
		netName = flag.String("net", "mainnet", "network magic")
	)
	flag.Parse()

	if *dbPath == "" {
		flag.Usage()
		os.Exit(2)
	}

	net, err := netFromName(*netName)
	if err != nil {
		log.Fatalf("bad -net: %v", err)
	}

	var dict []byte
	if *dictF != "" {
		dict, err = os.ReadFile(*dictF)
		if err != nil {
			log.Fatalf("read dict: %v", err)
		}
	}

	db, err := database.Open("mdbxdb", *dbPath, net)
	if err != nil {
		log.Fatalf("open mdbxdb at %q: %v", *dbPath, err)
	}
	defer db.Close()

	type rec struct {
		hash    chainhash.Hash
		fileNum uint32
		offset  uint32
		length  uint32
	}
	var all []rec

	// Walk all block-index rows.  Only "hot" rows (high bit clear) are
	// candidates for packing.
	if err := db.View(func(tx database.Tx) error {
		return mdbxdb.Mdbxdb_WalkBlockIndex(tx, func(hash chainhash.Hash, fileNum, offset, length uint32) error {
			if fileNum&0x80000000 != 0 {
				return nil // already cold
			}
			all = append(all, rec{hash, fileNum, offset, length})
			return nil
		})
	}); err != nil {
		log.Fatalf("walk block index: %v", err)
	}
	log.Printf("found %d hot block-index rows", len(all))

	// Sort by (fileNum, offset) so the on-disk order is preserved in the
	// segment.  This makes frame layout naturally height-ordered as a
	// proxy.
	sort.Slice(all, func(i, j int) bool {
		if all[i].fileNum != all[j].fileNum {
			return all[i].fileNum < all[j].fileNum
		}
		return all[i].offset < all[j].offset
	})

	// Keep the most-recent `hotKeep` blocks hot.  Everything older moves
	// into the new segment.
	if len(all) <= *hotKeep {
		log.Printf("only %d hot blocks; need >%d to make packing worthwhile",
			len(all), *hotKeep)
		return
	}
	moving := all[:len(all)-*hotKeep]
	log.Printf("packing %d blocks into segment %d (%d remain hot)",
		len(moving), *segNum, *hotKeep)

	builder := mdbxdb.NewSegBuilder(uint32(*segNum), uint32(net), dict, 2*1024*1024)

	// Read each block via the database and feed it to the builder.
	if err := db.View(func(tx database.Tx) error {
		for _, r := range moving {
			h := r.hash
			bs, err := tx.FetchBlock(&h)
			if err != nil {
				return fmt.Errorf("fetch %s: %w", h, err)
			}
			builder.AddBlock(&h, bs)
		}
		return nil
	}); err != nil {
		log.Fatalf("collect blocks: %v", err)
	}

	if _, err := builder.Finalize(*dbPath); err != nil {
		log.Fatalf("finalize seg: %v", err)
	}
	log.Printf("seg %d written", *segNum)

	// Re-write the block-index rows for the moved blocks to point at the
	// new cold segment.
	hashes := make([]chainhash.Hash, len(moving))
	lens := make([]uint32, len(moving))
	for i, r := range moving {
		hashes[i] = r.hash
		lens[i] = r.length
	}
	if err := db.Update(func(tx database.Tx) error {
		return mdbxdb.Mdbxdb_InstallColdBlockIndexProd(tx, uint32(*segNum), hashes, lens)
	}); err != nil {
		log.Fatalf("update block index: %v", err)
	}
	log.Printf("updated %d block-index rows to point at cold seg %d",
		len(moving), *segNum)
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
