// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"testing"

	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/wire/v2"
)

// TestParallelUtxoFetch verifies that fetching a large batch of cache-missed
// outpoints from the database (which fans the reads out across several
// concurrent read transactions) returns exactly the same entries as the data
// that was written, including correctly reporting absent outpoints as nil.
// Run under -race to confirm the parallel read path is data-race free.
func TestParallelUtxoFetch(t *testing.T) {
	chain, _, tearDown := utxoCacheTestChain("TestParallelUtxoFetch")
	defer tearDown()
	cache := chain.utxoCache

	// Add well over parallelUtxoFetchMinBatch outputs so the fetch fans out
	// across multiple workers, with distinct values/heights so a mismatch is
	// detectable.
	const numOutputs = 300
	for i := 0; i < numOutputs; i++ {
		txOut := wire.TxOut{
			Value:    int64(10000 + i),
			PkScript: getValidP2PKHScript(),
		}
		cache.addTxOut(outpointFromInt(i), &txOut, i%2 == 0, int32(i))
	}

	// Flush everything to the database so a fresh cache must read it back.
	err := chain.db.Update(func(dbTx database.Tx) error {
		return cache.flush(dbTx, FlushRequired, chain.stateSnapshot)
	})
	if err != nil {
		t.Fatalf("failed to flush utxo cache: %v", err)
	}

	// A brand-new cache over the same database holds no entries, so every
	// lookup misses and is served from disk via the parallel read path.
	fresh := newUtxoCache(chain.db, 10*1024*1024)

	// Build the request: all stored outpoints interleaved with some that
	// were never stored (indices >= numOutputs) which must come back nil.
	var (
		outpoints []wire.OutPoint
		wantStored []bool
	)
	for i := 0; i < numOutputs; i++ {
		outpoints = append(outpoints, outpointFromInt(i))
		wantStored = append(wantStored, true)
		if i%50 == 0 {
			outpoints = append(outpoints, outpointFromInt(100000+i))
			wantStored = append(wantStored, false)
		}
	}

	entries, err := fresh.fetchEntries(outpoints)
	if err != nil {
		t.Fatalf("fetchEntries failed: %v", err)
	}
	if len(entries) != len(outpoints) {
		t.Fatalf("got %d entries, want %d", len(entries), len(outpoints))
	}

	for i, entry := range entries {
		if !wantStored[i] {
			if entry != nil {
				t.Fatalf("outpoint %v: expected absent (nil), got %+v",
					outpoints[i], entry)
			}
			continue
		}

		if entry == nil {
			t.Fatalf("outpoint %v: expected an entry, got nil",
				outpoints[i])
		}

		idx := int(outpoints[i].Index)
		if entry.Amount() != int64(10000+idx) {
			t.Fatalf("outpoint %v: amount = %d, want %d",
				outpoints[i], entry.Amount(), 10000+idx)
		}
		if entry.BlockHeight() != int32(idx) {
			t.Fatalf("outpoint %v: height = %d, want %d",
				outpoints[i], entry.BlockHeight(), idx)
		}
		if entry.IsCoinBase() != (idx%2 == 0) {
			t.Fatalf("outpoint %v: coinbase = %v, want %v",
				outpoints[i], entry.IsCoinBase(), idx%2 == 0)
		}
	}
}
