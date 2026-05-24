// Copyright (c) 2013-2017 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"errors"
	"fmt"
	"testing"

	"github.com/btcsuite/btcd/txscript/v2"
)

// TestCheckBlockScripts ensures that validating the all of the scripts in a
// known-good block doesn't return an error.
func TestCheckBlockScripts(t *testing.T) {
	testBlockNum := 277647
	blockDataFile := fmt.Sprintf("%d.dat.bz2", testBlockNum)
	blocks, err := loadBlocks(blockDataFile)
	if err != nil {
		t.Errorf("Error loading file: %v\n", err)
		return
	}
	if len(blocks) > 1 {
		t.Errorf("The test block file must only have one block in it")
		return
	}
	if len(blocks) == 0 {
		t.Errorf("The test block file may not be empty")
		return
	}

	storeDataFile := fmt.Sprintf("%d.utxostore.bz2", testBlockNum)
	view, err := loadUtxoView(storeDataFile)
	if err != nil {
		t.Errorf("Error loading txstore: %v\n", err)
		return
	}

	scriptFlags := txscript.ScriptBip16
	err = checkBlockScripts(blocks[0], view, scriptFlags, nil, nil)
	if err != nil {
		t.Errorf("Transaction script validation failed: %v\n", err)
		return
	}
}

// TestCheckBlockScriptsErrorFastFail exercises the cancel path in the
// shared scriptValidatorPool by mutating one input's prevout to be
// unknown.  The resulting ErrMissingTxOut must be returned even when
// the rest of the block's inputs are valid, and the pool must not
// leak workers or block on the doomed inputs.
func TestCheckBlockScriptsErrorFastFail(t *testing.T) {
	testBlockNum := 277647
	blockDataFile := fmt.Sprintf("%d.dat.bz2", testBlockNum)
	blocks, err := loadBlocks(blockDataFile)
	if err != nil {
		t.Fatalf("Error loading file: %v", err)
	}
	if len(blocks) != 1 {
		t.Fatalf("expected one block in fixture, got %d", len(blocks))
	}

	storeDataFile := fmt.Sprintf("%d.utxostore.bz2", testBlockNum)
	view, err := loadUtxoView(storeDataFile)
	if err != nil {
		t.Fatalf("Error loading txstore: %v", err)
	}

	// Mutate a single non-coinbase input's prevout index to one that
	// will fail LookupEntry, forcing runScriptVerify to return
	// ErrMissingTxOut for that one input.  All others should still
	// succeed; we want the first error to bubble up.
	block := blocks[0]
	txs := block.Transactions()
	if len(txs) < 2 {
		t.Skip("block fixture has no non-coinbase tx; skipping")
	}
	mutated := false
	for _, tx := range txs[1:] {
		for _, txIn := range tx.MsgTx().TxIn {
			txIn.PreviousOutPoint.Index = 0xfffffffe
			mutated = true
			break
		}
		if mutated {
			break
		}
	}
	if !mutated {
		t.Skip("no non-coinbase input to mutate; skipping")
	}

	err = checkBlockScripts(block, view, txscript.ScriptBip16, nil, nil)
	if err == nil {
		t.Fatal("expected ErrMissingTxOut, got nil")
	}
	var rerr RuleError
	if !errors.As(err, &rerr) {
		t.Fatalf("expected RuleError, got %T: %v", err, err)
	}
	if rerr.ErrorCode != ErrMissingTxOut {
		t.Fatalf("expected ErrMissingTxOut, got %v", rerr.ErrorCode)
	}
}

// BenchmarkCheckBlockScripts measures throughput of full-block script
// validation through the shared scriptValidatorPool.
func BenchmarkCheckBlockScripts(b *testing.B) {
	testBlockNum := 277647
	blockDataFile := fmt.Sprintf("%d.dat.bz2", testBlockNum)
	blocks, err := loadBlocks(blockDataFile)
	if err != nil {
		b.Fatalf("Error loading file: %v", err)
	}
	storeDataFile := fmt.Sprintf("%d.utxostore.bz2", testBlockNum)
	view, err := loadUtxoView(storeDataFile)
	if err != nil {
		b.Fatalf("Error loading utxos: %v", err)
	}

	// Warm the pool so the first iteration doesn't pay for goroutine
	// spawn.
	if err := checkBlockScripts(blocks[0], view, txscript.ScriptBip16,
		nil, nil); err != nil {
		b.Fatalf("warmup checkBlockScripts: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := checkBlockScripts(blocks[0], view,
			txscript.ScriptBip16, nil, nil); err != nil {
			b.Fatalf("checkBlockScripts: %v", err)
		}
	}
}
