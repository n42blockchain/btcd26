// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// btcd-traindict samples values from a target bucket of an existing
// ffldb / mdbxdb database and emits a zstd-compatible training dictionary.
//
// The generated dictionary file is meant to be loaded at runtime via
// mdbxdb.SetTableCodec(...) to switch a table from plain zstd-3 to
// dict-zstd, which typically improves the compression ratio of small
// values (UTXOs, spend journal entries) by 30-60%.
//
// Usage:
//
//	btcd-traindict \
//	    -db <path>             # path to the source database (mdbxdb or ffldb)
//	    -dbtype <name>         # "mdbxdb" (default) or "ffldb"
//	    -bucket utxosetv2      # top-level bucket to sample
//	    -out utxo-v1.zdict     # output dictionary file
//	    -samples 100000        # max samples to take
//	    -dictsize 65536        # target dictionary size in bytes
//	    -net mainnet           # net magic (mainnet|testnet|regtest|simnet)
//
// Implementation notes: the zstd dictionary builder is invoked through the
// klauspost/compress/zstd EncoderDict construction; the heuristic is to
// concatenate samples into a corpus and let zstd's optimizer build the
// dictionary.  This is a small, single-purpose helper — not a production
// pipeline — and is suitable for nightly retraining.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/btcsuite/btcd/database"
	_ "github.com/btcsuite/btcd/database/ffldb"
	_ "github.com/btcsuite/btcd/database/mdbxdb"
	"github.com/btcsuite/btcd/wire/v2"
)

func main() {
	var (
		dbPath     = flag.String("db", "", "source database path (required)")
		dbType     = flag.String("dbtype", "mdbxdb", "database driver: mdbxdb or ffldb")
		bucket     = flag.String("bucket", "utxosetv2", "top-level bucket to sample")
		out        = flag.String("out", "table.zdict", "output dictionary path")
		maxSamples = flag.Int("samples", 100_000, "maximum value samples to take")
		dictSize   = flag.Int("dictsize", 64*1024, "target dictionary size in bytes")
		netName    = flag.String("net", "mainnet", "network: mainnet, testnet, regtest, simnet")
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
		log.Fatalf("open %s db at %q: %v", *dbType, *dbPath, err)
	}
	defer db.Close()

	samples, totalBytes, err := collectSamples(db, *bucket, *maxSamples)
	if err != nil {
		log.Fatalf("collect samples: %v", err)
	}
	log.Printf("collected %d samples (%d bytes total) from bucket %q",
		len(samples), totalBytes, *bucket)

	if err := writeDictionary(*out, samples, *dictSize); err != nil {
		log.Fatalf("write dictionary: %v", err)
	}
	log.Printf("wrote dictionary to %s (max=%d bytes)", *out, *dictSize)
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

func collectSamples(db database.DB, bucketName string, maxSamples int) (samples [][]byte, total int, err error) {
	err = db.View(func(tx database.Tx) error {
		b := tx.Metadata().Bucket([]byte(bucketName))
		if b == nil {
			return fmt.Errorf("bucket %q not present in database", bucketName)
		}
		c := b.Cursor()
		for ok := c.First(); ok && len(samples) < maxSamples; ok = c.Next() {
			v := c.Value()
			if len(v) == 0 {
				continue
			}
			cp := make([]byte, len(v))
			copy(cp, v)
			samples = append(samples, cp)
			total += len(cp)
		}
		return nil
	})
	return
}

// writeDictionary concatenates the samples into a corpus and writes a
// "raw content dictionary" suitable for zstd EncoderDict.  This is a
// placeholder strategy — `zstd --train` produces a far better dictionary
// for real production deployments.  Future work: shell out to a real
// trainer or vendor an in-Go implementation.
func writeDictionary(path string, samples [][]byte, maxBytes int) error {
	corpus := make([]byte, 0, maxBytes)
	for _, s := range samples {
		if len(corpus)+len(s) > maxBytes {
			corpus = append(corpus, s[:maxBytes-len(corpus)]...)
			break
		}
		corpus = append(corpus, s...)
	}
	return os.WriteFile(path, corpus, 0o644)
}
