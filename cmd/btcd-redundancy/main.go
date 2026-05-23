// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// btcd-redundancy reads a sample of blocks from an existing
// mdbxdb/ffldb and measures cross-block redundancy of structured
// fields — the kind a structural re-encoder could exploit but
// generic zstd cannot.
//
// For each tracked category (prev-tx-id, output script, witness
// item, etc.) it reports:
//
//   total bytes        : sum of all occurrences in the corpus
//   unique values      : distinct byte strings observed
//   dedup bytes        : bytes that would remain if every distinct
//                        value were stored once and references
//                        replaced with a 4-byte global ID
//   compression ratio  : total / dedup
//   frequency histogram: how many values appear 1x, 2x, 3-9x, 10+x
//
// This is the upper bound on what an offline structural compressor
// could achieve (Erigon-style for Ethereum, but applied to Bitcoin's
// actual repeatable structures rather than the canonical block byte
// stream).
//
// Usage:
//
//	btcd-redundancy \
//	    -db <path> -dbtype mdbxdb -samples 5000 -net mainnet
package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/database"
	_ "github.com/btcsuite/btcd/database/ffldb"
	_ "github.com/btcsuite/btcd/database/mdbxdb"
	"github.com/btcsuite/btcd/wire/v2"
)

const idBytes = 4 // assumed global-ID width when computing dedup savings.

// counter tracks frequency + size of unique byte-string values in
// one category.  Uses string([]byte) as map key — Go converts the
// bytes once and reuses the string identity for hashing.
type counter struct {
	name  string
	freq  map[string]int
	bytes int64 // total bytes across all occurrences
}

func newCounter(name string) *counter {
	return &counter{name: name, freq: make(map[string]int, 1<<16)}
}

func (c *counter) add(b []byte) {
	if len(b) == 0 {
		return
	}
	c.bytes += int64(len(b))
	c.freq[string(b)]++
}

// report prints the redundancy stats for this category.
func (c *counter) report() {
	uniqueCount := len(c.freq)
	if uniqueCount == 0 {
		fmt.Printf("%-20s  (no occurrences)\n", c.name)
		return
	}

	// Distribution buckets.
	var (
		seen1   int            // values that occur exactly once
		seen2   int            // exactly 2
		seen3to9 int           // 3-9
		seen10p  int           // 10+
		totalRefs int64
		uniqueBytes int64      // sum of unique value lengths
		dedupRefBytes int64    // 4-byte IDs that would replace duplicates
		topByCount []dictEntry // for showing the most-repeated values
	)
	for v, n := range c.freq {
		totalRefs += int64(n)
		uniqueBytes += int64(len(v))
		dedupRefBytes += int64(n) * idBytes
		switch {
		case n == 1:
			seen1++
		case n == 2:
			seen2++
		case n < 10:
			seen3to9++
		default:
			seen10p++
		}
		topByCount = append(topByCount, dictEntry{value: v, count: n})
	}
	sort.Slice(topByCount, func(i, j int) bool {
		return topByCount[i].count > topByCount[j].count
	})

	// Dedup model: each unique value stored once (uniqueBytes), each
	// reference becomes idBytes long.
	dedup := uniqueBytes + dedupRefBytes
	ratio := float64(c.bytes) / float64(dedup)
	saved := c.bytes - dedup

	fmt.Printf("%-20s  total=%9s  uniq=%8d  dedup=%9s  saves=%9s  ratio=%.2fx\n",
		c.name,
		humanBytes(c.bytes),
		uniqueCount,
		humanBytes(dedup),
		humanBytes(saved),
		ratio,
	)
	fmt.Printf("  %-18s  refs=%d  1×=%d (%.1f%%)  2×=%d  3-9×=%d  10+×=%d\n",
		"frequency",
		totalRefs,
		seen1, float64(seen1)*100/float64(uniqueCount),
		seen2, seen3to9, seen10p,
	)
	// Show top-3 hottest values (truncated for readability).
	for i := 0; i < 3 && i < len(topByCount); i++ {
		e := topByCount[i]
		preview := fmt.Sprintf("%x", []byte(e.value))
		if len(preview) > 60 {
			preview = preview[:60] + "..."
		}
		fmt.Printf("  top%d  count=%-8d  size=%-4d  %s\n",
			i+1, e.count, len(e.value), preview)
	}
	fmt.Println()
}

type dictEntry struct {
	value string
	count int
}

func humanBytes(n int64) string {
	const k = 1024
	switch {
	case n >= 1024*1024*1024:
		return fmt.Sprintf("%.2f GiB", float64(n)/1024/1024/1024)
	case n >= 1024*1024:
		return fmt.Sprintf("%.2f MiB", float64(n)/1024/1024)
	case n >= 1024:
		return fmt.Sprintf("%.2f KiB", float64(n)/1024)
	}
	return fmt.Sprintf("%d B", n)
}

func main() {
	var (
		dbPath  = flag.String("db", "", "source database path (required)")
		dbType  = flag.String("dbtype", "mdbxdb", "database driver: mdbxdb or ffldb")
		samples = flag.Int("samples", 5000, "number of blocks to sample")
		netName = flag.String("net", "mainnet", "network: mainnet, testnet, regtest, simnet")
		seed    = flag.Int64("seed", 42, "random seed")
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

	db, err := database.Open(*dbType, *dbPath, net)
	if err != nil {
		log.Fatalf("open %s at %q: %v", *dbType, *dbPath, err)
	}
	defer db.Close()

	hashes, err := loadHashes(db)
	if err != nil {
		log.Fatalf("loadHashes: %v", err)
	}
	if len(hashes) == 0 {
		log.Fatalf("no blocks in database")
	}
	r := rand.New(rand.NewSource(*seed))
	r.Shuffle(len(hashes), func(i, j int) {
		hashes[i], hashes[j] = hashes[j], hashes[i]
	})
	if *samples > len(hashes) {
		*samples = len(hashes)
	}
	hashes = hashes[:*samples]

	// Categories tracked across the corpus.
	var (
		cPrevTxid    = newCounter("prevTxid (32B)")     // input outpoint txid
		cPrevOutpoint = newCounter("prevOutpoint (36B)") // txid + vout, full outpoint
		cPkScript    = newCounter("pkScript (output)")  // tx output scripts
		cSigScript   = newCounter("scriptSig (input)")  // legacy unlock scripts
		cWitnessItem = newCounter("witness item")       // each segwit witness element
		cFullBlock   = newCounter("full block")         // for sanity check
	)

	var (
		totalTxn   int64
		totalIn    int64
		totalOut   int64
		totalSegwit int64
		totalBytes int64
	)

	log.Printf("scanning %d blocks...", len(hashes))
	err = db.View(func(tx database.Tx) error {
		for i, h := range hashes {
			rawBlock, ferr := tx.FetchBlock(&h)
			if ferr != nil {
				return ferr
			}
			totalBytes += int64(len(rawBlock))
			cFullBlock.add(rawBlock)

			blk, perr := btcutil.NewBlockFromBytes(rawBlock)
			if perr != nil {
				return fmt.Errorf("parse block %s: %w", h, perr)
			}
			for _, btx := range blk.Transactions() {
				msgTx := btx.MsgTx()
				totalTxn++
				if msgTx.HasWitness() {
					totalSegwit++
				}
				for _, in := range msgTx.TxIn {
					totalIn++
					// Skip coinbase prev-outpoint (all zeros).
					if !isCoinbaseOutpoint(&in.PreviousOutPoint) {
						cPrevTxid.add(in.PreviousOutPoint.Hash[:])
						buf := make([]byte, 36)
						copy(buf, in.PreviousOutPoint.Hash[:])
						buf[32] = byte(in.PreviousOutPoint.Index)
						buf[33] = byte(in.PreviousOutPoint.Index >> 8)
						buf[34] = byte(in.PreviousOutPoint.Index >> 16)
						buf[35] = byte(in.PreviousOutPoint.Index >> 24)
						cPrevOutpoint.add(buf)
					}
					if len(in.SignatureScript) > 0 {
						cSigScript.add(in.SignatureScript)
					}
					for _, w := range in.Witness {
						if len(w) > 0 {
							cWitnessItem.add(w)
						}
					}
				}
				for _, out := range msgTx.TxOut {
					totalOut++
					cPkScript.add(out.PkScript)
				}
			}

			if i > 0 && i%500 == 0 {
				log.Printf("  ...%d/%d blocks processed", i, len(hashes))
			}
		}
		return nil
	})
	if err != nil {
		log.Fatalf("scan: %v", err)
	}

	fmt.Println()
	fmt.Printf("=========== corpus summary ===========\n")
	fmt.Printf("blocks            : %d\n", len(hashes))
	fmt.Printf("total bytes       : %s\n", humanBytes(totalBytes))
	fmt.Printf("transactions      : %d (segwit: %d)\n", totalTxn, totalSegwit)
	fmt.Printf("inputs            : %d\n", totalIn)
	fmt.Printf("outputs           : %d\n", totalOut)
	fmt.Println()
	fmt.Printf("=========== per-category dedup =========\n")
	cFullBlock.report()
	cPrevTxid.report()
	cPrevOutpoint.report()
	cPkScript.report()
	cSigScript.report()
	cWitnessItem.report()
}

func isCoinbaseOutpoint(o *wire.OutPoint) bool {
	for _, b := range o.Hash {
		if b != 0 {
			return false
		}
	}
	return o.Index == 0xffffffff
}

func loadHashes(db database.DB) ([]chainhash.Hash, error) {
	var hashes []chainhash.Hash
	err := db.View(func(tx database.Tx) error {
		b := tx.Metadata().Bucket([]byte("ffldb-blockidx"))
		if b == nil {
			return fmt.Errorf("bucket ffldb-blockidx not found")
		}
		c := b.Cursor()
		for ok := c.First(); ok; ok = c.Next() {
			var h chainhash.Hash
			copy(h[:], c.Key())
			hashes = append(hashes, h)
		}
		return nil
	})
	return hashes, err
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
