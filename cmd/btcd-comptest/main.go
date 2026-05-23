// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// btcd-comptest empirically measures three compression strategies on
// real Bitcoin blocks pulled from an existing mdbxdb/ffldb database.
// It compares the strategies on a held-out test corpus and reports
// size, ratio, encode time, decode time, and roundtrip correctness.
//
// L1: plain zstd-3            (current btcd default for index buckets)
// L2: zstd-3 + trained dict   (target for hot .fdb in-line compression)
// L3: zstd-22 + trained dict  (proxy for Erigon-style seg/Huffman extreme)
//
// Usage:
//
//	btcd-comptest \
//	    -db <path-to-mdbxdb> \
//	    -dbtype mdbxdb \
//	    -samples 5000 \
//	    -dictpct 30 \
//	    -net mainnet
//
// The first dictpct% of sampled blocks are used to train the
// dictionary; the rest are the test corpus.  Encode/decode times
// are wall-clock medians across the corpus.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/database"
	_ "github.com/btcsuite/btcd/database/ffldb"
	_ "github.com/btcsuite/btcd/database/mdbxdb"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/klauspost/compress/zstd"
)

func main() {
	var (
		dbPath  = flag.String("db", "", "source database path (required)")
		dbType  = flag.String("dbtype", "mdbxdb", "database driver: mdbxdb or ffldb")
		samples = flag.Int("samples", 5000, "total blocks to sample")
		dictPct = flag.Int("dictpct", 30, "percentage of samples used for dictionary training")
		netName = flag.String("net", "mainnet", "network: mainnet, testnet, regtest, simnet")
		seed         = flag.Int64("seed", 42, "random seed for sample selection")
		flagDictBytes = flag.Int("dictsize", 64*1024, "raw dictionary size in bytes (content prefix for L2/L3)")
	)
	flag.Parse()
	_ = context.Background()

	if *dbPath == "" {
		flag.Usage()
		os.Exit(2)
	}
	net, err := netFromName(*netName)
	if err != nil {
		log.Fatalf("bad -net: %v", err)
	}
	if *dictPct < 1 || *dictPct > 90 {
		log.Fatalf("dictpct must be in [1, 90]")
	}

	// Open DB read-only.
	db, err := database.Open(*dbType, *dbPath, net)
	if err != nil {
		log.Fatalf("open %s at %q: %v", *dbType, *dbPath, err)
	}
	defer db.Close()

	// Pull <samples> random block hashes from the block-index bucket,
	// then fetch each block's raw bytes.
	blocks, err := loadSampleBlocks(db, *samples, *seed)
	if err != nil {
		log.Fatalf("loadSampleBlocks: %v", err)
	}
	if len(blocks) == 0 {
		log.Fatalf("no blocks in the database")
	}
	log.Printf("loaded %d sample blocks", len(blocks))

	// Split into dictionary corpus and test corpus.
	dictN := len(blocks) * *dictPct / 100
	if dictN < 1 {
		dictN = 1
	}
	dictCorpus := blocks[:dictN]
	testCorpus := blocks[dictN:]
	log.Printf("dict corpus: %d blocks, test corpus: %d blocks",
		len(dictCorpus), len(testCorpus))

	// Compute raw size and per-block size distribution for context.
	var rawBytes int64
	sizes := make([]int, len(testCorpus))
	for i, b := range testCorpus {
		rawBytes += int64(len(b))
		sizes[i] = len(b)
	}
	sort.Ints(sizes)
	log.Printf("test corpus raw size: %.2f MiB (avg %d B, median %d B, max %d B)",
		float64(rawBytes)/1024/1024,
		int(rawBytes)/len(testCorpus),
		sizes[len(sizes)/2],
		sizes[len(sizes)-1],
	)

	// Build a raw content-prefix "dictionary" by concatenating the
	// first 64 KiB of dict corpus.  klauspost's WithEncoderDictRaw
	// accepts arbitrary bytes (no magic header) and treats them as
	// the encoder's initial history window — the compressor's
	// pattern matcher gets warm-started on whatever recurring
	// strings live in those bytes (P2PKH/P2WPKH/P2TR script
	// templates, common addresses, etc.).
	dictSize := *flagDictBytes
	dictBytes := make([]byte, 0, dictSize)
	for _, b := range dictCorpus {
		need := dictSize - len(dictBytes)
		if need <= 0 {
			break
		}
		if len(b) > need {
			dictBytes = append(dictBytes, b[:need]...)
		} else {
			dictBytes = append(dictBytes, b...)
		}
	}
	log.Printf("raw dict: %d bytes (concatenated content prefix)", len(dictBytes))

	// Run three benchmarks.
	fmt.Println()
	fmt.Printf("%-40s  %12s  %8s  %12s  %12s  %s\n",
		"strategy", "compressed", "ratio", "encode/blk", "decode/blk", "ok")
	fmt.Println(repeat("-", 100))

	runBench("L1: zstd-3 (no dict)", testCorpus, rawBytes,
		newPlainEncoder(zstd.SpeedDefault),
		newPlainDecoder())

	runBench("L2: zstd-3 + trained dict", testCorpus, rawBytes,
		newDictEncoder(zstd.SpeedDefault, dictBytes),
		newDictDecoder(dictBytes))

	runBench("L3: zstd-22 + trained dict (Erigon proxy)", testCorpus, rawBytes,
		newDictEncoder(zstd.SpeedBestCompression, dictBytes),
		newDictDecoder(dictBytes))
}

// loadSampleBlocks selects up to n block hashes uniformly at random
// from the block-index bucket and fetches each block's raw bytes.
func loadSampleBlocks(db database.DB, n int, seed int64) ([][]byte, error) {
	// Phase 1: collect hashes from whichever block-index bucket the
	// driver uses.  ffldb/mdbxdb both use "ffldb-blockidx".
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
	if err != nil {
		return nil, err
	}
	if len(hashes) == 0 {
		return nil, nil
	}

	// Phase 2: shuffle and take the first n.
	r := rand.New(rand.NewSource(seed))
	r.Shuffle(len(hashes), func(i, j int) {
		hashes[i], hashes[j] = hashes[j], hashes[i]
	})
	if n > len(hashes) {
		n = len(hashes)
	}
	hashes = hashes[:n]

	// Phase 3: fetch each block.
	blocks := make([][]byte, 0, n)
	err = db.View(func(tx database.Tx) error {
		for i := range hashes {
			raw, err := tx.FetchBlock(&hashes[i])
			if err != nil {
				return err
			}
			// Copy because the slice is only valid during the tx.
			cp := make([]byte, len(raw))
			copy(cp, raw)
			blocks = append(blocks, cp)
		}
		return nil
	})
	return blocks, err
}

// encoder / decoder abstractions so we can swap codecs cleanly.

type encoder interface {
	encode(src []byte) []byte
}
type decoder interface {
	decode(src []byte) ([]byte, error)
}

type zstdEnc struct{ w *zstd.Encoder }

func (e *zstdEnc) encode(src []byte) []byte {
	return e.w.EncodeAll(src, make([]byte, 0, len(src)/2))
}

type zstdDec struct{ r *zstd.Decoder }

func (d *zstdDec) decode(src []byte) ([]byte, error) {
	return d.r.DecodeAll(src, nil)
}

func newPlainEncoder(lvl zstd.EncoderLevel) encoder {
	w, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(lvl),
		zstd.WithEncoderConcurrency(1))
	if err != nil {
		log.Fatalf("newPlainEncoder lvl=%v: %v", lvl, err)
	}
	return &zstdEnc{w}
}

func newPlainDecoder() decoder {
	r, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		log.Fatalf("newPlainDecoder: %v", err)
	}
	return &zstdDec{r}
}

// rawDictID is the arbitrary 32-bit identifier we tag our raw
// content-prefix dictionary with; encoder and decoder must agree.
const rawDictID = 0x57414e47

func newDictEncoder(lvl zstd.EncoderLevel, dict []byte) encoder {
	w, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(lvl),
		zstd.WithEncoderDictRaw(rawDictID, dict),
		zstd.WithEncoderConcurrency(1))
	if err != nil {
		log.Fatalf("newDictEncoder lvl=%v dictLen=%d: %v",
			lvl, len(dict), err)
	}
	return &zstdEnc{w}
}

func newDictDecoder(dict []byte) decoder {
	r, err := zstd.NewReader(nil,
		zstd.WithDecoderDictRaw(rawDictID, dict),
		zstd.WithDecoderConcurrency(1))
	if err != nil {
		log.Fatalf("newDictDecoder dictLen=%d: %v",
			len(dict), err)
	}
	return &zstdDec{r}
}

// runBench encodes/decodes each block in corpus with enc/dec, verifies
// roundtrip, and reports aggregate stats.
func runBench(label string, corpus [][]byte, rawBytes int64,
	enc encoder, dec decoder) {

	var (
		compressed int64
		encDur     time.Duration
		decDur     time.Duration
		ok         = true
	)

	encoded := make([][]byte, len(corpus))

	// Encode all.
	for i, blk := range corpus {
		t0 := time.Now()
		encoded[i] = enc.encode(blk)
		encDur += time.Since(t0)
		compressed += int64(len(encoded[i]))
	}

	// Decode all and verify roundtrip on a random sample to keep cost
	// reasonable (full sample at zstd-22 takes ages).
	for i, blk := range corpus {
		t0 := time.Now()
		got, err := dec.decode(encoded[i])
		decDur += time.Since(t0)
		if err != nil || !bytesEqual(got, blk) {
			ok = false
			log.Printf("%s: roundtrip FAILED on sample %d (%v)", label, i, err)
			break
		}
	}

	ratio := float64(rawBytes) / float64(compressed)
	encPer := encDur / time.Duration(len(corpus))
	decPer := decDur / time.Duration(len(corpus))

	fmt.Printf("%-40s  %8.2f MB  %7.2fx  %12s  %12s  %v\n",
		label,
		float64(compressed)/1024/1024,
		ratio,
		encPer.Truncate(time.Microsecond),
		decPer.Truncate(time.Microsecond),
		ok,
	)
}

func bytesEqual(a, b []byte) bool {
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

func repeat(s string, n int) string {
	out := make([]byte, 0, n*len(s))
	for i := 0; i < n; i++ {
		out = append(out, s...)
	}
	return string(out)
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
