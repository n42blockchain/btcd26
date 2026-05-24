// Copyright (c) 2013-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcutil/v2"
	"github.com/btcsuite/btcd/txscript/v2"
	"github.com/btcsuite/btcd/wire/v2"
)

// txValidateItem holds a transaction along with which input to validate.
type txValidateItem struct {
	txInIndex int
	txIn      *wire.TxIn
	tx        *btcutil.Tx
	sigHashes *txscript.TxSigHashes
}

// scriptValidateJob is the per-input work unit dispatched to a worker in
// scriptValidatorPool.  Each job carries the per-call view/flags/cache
// pointers so workers can run without referencing a per-block validator
// struct.
type scriptValidateJob struct {
	item     *txValidateItem
	utxoView *UtxoViewpoint
	flags    txscript.ScriptFlags
	sigCache *txscript.SigCache
	resultCh chan<- error
	cancel   <-chan struct{}
}

// scriptValidatorPool is a long-lived fan-out worker pool for script
// verification.  It replaces the previous per-block goroutine spawn
// pattern (NumCPU*3 goroutines created and torn down for every block
// validated), which under IBD adds tens of millions of goroutine
// lifecycles and synchronous channel handoffs to the critical path.
//
// One pool is shared across the process.  Multiple concurrent Validate
// calls (e.g. mempool ingress racing block validation) are isolated
// from each other via their own per-call resultCh / cancel channels;
// the shared workCh just FIFO-dispatches their jobs.
type scriptValidatorPool struct {
	workCh chan scriptValidateJob
}

var (
	scriptValPool     *scriptValidatorPool
	scriptValPoolOnce sync.Once
)

// getScriptValidatorPool returns the package-level pool, starting its
// workers on first call.
func getScriptValidatorPool() *scriptValidatorPool {
	scriptValPoolOnce.Do(func() {
		workers := runtime.NumCPU()
		if workers < 1 {
			workers = 1
		}

		// Buffer scaled to worker count: large enough that submit
		// rarely blocks (so the per-Validate select can stay in
		// "submit fast, collect occasionally" mode), small enough
		// that we don't pre-allocate a giant pipeline.
		scriptValPool = &scriptValidatorPool{
			workCh: make(chan scriptValidateJob, workers*4),
		}
		for i := 0; i < workers; i++ {
			go scriptValPool.worker()
		}
	})
	return scriptValPool
}

// worker is the long-lived job loop for one pool slot.  Pulls jobs off
// the shared work channel, runs script validation, and posts the
// result back to the job's per-call result channel.  Honors per-call
// cancellation as a fast-path skip so a failed Validate doesn't keep
// the pool busy on doomed work.
func (p *scriptValidatorPool) worker() {
	for job := range p.workCh {
		// Skip work if the per-call Validate has already hit an
		// error and closed its cancel channel.  We still post a
		// result so Validate's drain loop terminates.
		select {
		case <-job.cancel:
			job.resultCh <- nil
			continue
		default:
		}

		job.resultCh <- runScriptVerify(job)
	}
}

// runScriptVerify executes the script-pair validation for a single
// input.  Pulled out of the per-block validator goroutine into a free
// function so workers can call it without any per-block receiver.
func runScriptVerify(job scriptValidateJob) error {
	item := job.item
	txIn := item.txIn

	// Ensure the referenced input utxo is available.
	utxo := job.utxoView.LookupEntry(txIn.PreviousOutPoint)
	if utxo == nil {
		str := fmt.Sprintf("unable to find unspent output %v "+
			"referenced from transaction %s:%d",
			txIn.PreviousOutPoint, item.tx.Hash(),
			item.txInIndex)
		return ruleError(ErrMissingTxOut, str)
	}

	sigScript := txIn.SignatureScript
	witness := txIn.Witness
	pkScript := utxo.PkScript()
	inputAmount := utxo.Amount()
	vm, err := txscript.NewEngine(
		pkScript, item.tx.MsgTx(), item.txInIndex,
		job.flags, job.sigCache, item.sigHashes,
		inputAmount, job.utxoView,
	)
	if err != nil {
		str := fmt.Sprintf("failed to parse input %s:%d which "+
			"references output %v - %v (input witness %x, input "+
			"script bytes %x, prev output script bytes %x)",
			item.tx.Hash(), item.txInIndex,
			txIn.PreviousOutPoint, err, witness,
			sigScript, pkScript)
		return ruleError(ErrScriptMalformed, str)
	}

	if err := vm.Execute(); err != nil {
		str := fmt.Sprintf("failed to validate input %s:%d which "+
			"references output %v - %v (input witness %x, input "+
			"script bytes %x, prev output script bytes %x)",
			item.tx.Hash(), item.txInIndex,
			txIn.PreviousOutPoint, err, witness,
			sigScript, pkScript)
		return ruleError(ErrScriptValidation, str)
	}

	return nil
}

// txValidator carries the per-Validate-call state -- the UTXO view,
// script flags, and caches that each input's verification needs.  It
// no longer owns any goroutines; Validate dispatches into the shared
// scriptValidatorPool.
type txValidator struct {
	utxoView  *UtxoViewpoint
	flags     txscript.ScriptFlags
	sigCache  *txscript.SigCache
	hashCache *txscript.HashCache
}

// Validate validates the scripts for all of the passed transaction
// inputs through the shared scriptValidatorPool.
func (v *txValidator) Validate(items []*txValidateItem) error {
	if len(items) == 0 {
		return nil
	}

	pool := getScriptValidatorPool()

	// resultCh is buffered so workers never block on send: that lets
	// us drain all submitted jobs before returning even on early
	// error, without orphaning workers on a dangling unbuffered
	// channel.  Capped at a moderate size so very large batches don't
	// pre-allocate a huge channel pipeline; the per-iteration drain
	// in the select loop keeps the buffer from filling.
	resultBuf := len(items)
	if resultBuf > 64 {
		resultBuf = 64
	}
	resultCh := make(chan error, resultBuf)
	cancel := make(chan struct{})

	var firstErr error
	cancelled := false
	currentItem := 0
	processedItems := 0
	for processedItems < len(items) {
		// Drive submit + collect in a single select so a slow
		// collect doesn't stall submit and a slow worker doesn't
		// stall collect.  Setting workCh to nil after the last
		// submit disables the submit branch for the remaining
		// iterations.
		var workCh chan<- scriptValidateJob
		var job scriptValidateJob
		if currentItem < len(items) {
			workCh = pool.workCh
			job = scriptValidateJob{
				item:     items[currentItem],
				utxoView: v.utxoView,
				flags:    v.flags,
				sigCache: v.sigCache,
				resultCh: resultCh,
				cancel:   cancel,
			}
		}

		select {
		case workCh <- job:
			currentItem++

		case err := <-resultCh:
			processedItems++
			if err != nil && firstErr == nil {
				firstErr = err
				if !cancelled {
					close(cancel)
					cancelled = true
				}
			}
		}
	}
	return firstErr
}

// newTxValidator returns a new instance of txValidator to be used for
// validating transaction scripts asynchronously.
func newTxValidator(utxoView *UtxoViewpoint, flags txscript.ScriptFlags,
	sigCache *txscript.SigCache, hashCache *txscript.HashCache) *txValidator {
	return &txValidator{
		utxoView:  utxoView,
		sigCache:  sigCache,
		hashCache: hashCache,
		flags:     flags,
	}
}

// ValidateTransactionScripts validates the scripts for the passed transaction
// using multiple goroutines.
func ValidateTransactionScripts(tx *btcutil.Tx, utxoView *UtxoViewpoint,
	flags txscript.ScriptFlags, sigCache *txscript.SigCache,
	hashCache *txscript.HashCache) error {

	// First determine if segwit is active according to the scriptFlags. If
	// it isn't then we don't need to interact with the HashCache.
	segwitActive := flags&txscript.ScriptVerifyWitness == txscript.ScriptVerifyWitness

	// If the hashcache doesn't yet has the sighash midstate for this
	// transaction, then we'll compute them now so we can re-use them
	// amongst all worker validation goroutines.
	if segwitActive && tx.MsgTx().HasWitness() &&
		!hashCache.ContainsHashes(tx.Hash()) {
		hashCache.AddSigHashes(tx.MsgTx(), utxoView)
	}

	var cachedHashes *txscript.TxSigHashes
	if segwitActive && tx.MsgTx().HasWitness() {
		// The same pointer to the transaction's sighash midstate will
		// be re-used amongst all validation goroutines. By
		// pre-computing the sighash here instead of during validation,
		// we ensure the sighashes
		// are only computed once.
		cachedHashes, _ = hashCache.GetSigHashes(tx.Hash())
	}

	// Collect all of the transaction inputs and required information for
	// validation.
	txIns := tx.MsgTx().TxIn
	txValItems := make([]*txValidateItem, 0, len(txIns))
	for txInIdx, txIn := range txIns {
		// Skip coinbases.
		if txIn.PreviousOutPoint.Index == math.MaxUint32 {
			continue
		}

		txVI := &txValidateItem{
			txInIndex: txInIdx,
			txIn:      txIn,
			tx:        tx,
			sigHashes: cachedHashes,
		}
		txValItems = append(txValItems, txVI)
	}

	// Validate all of the inputs.
	validator := newTxValidator(utxoView, flags, sigCache, hashCache)
	return validator.Validate(txValItems)
}

// checkBlockScripts executes and validates the scripts for all transactions in
// the passed block using multiple goroutines.
func checkBlockScripts(block *btcutil.Block, utxoView *UtxoViewpoint,
	scriptFlags txscript.ScriptFlags, sigCache *txscript.SigCache,
	hashCache *txscript.HashCache) error {

	// First determine if segwit is active according to the scriptFlags. If
	// it isn't then we don't need to interact with the HashCache.
	segwitActive := scriptFlags&txscript.ScriptVerifyWitness == txscript.ScriptVerifyWitness

	// Collect all of the transaction inputs and required information for
	// validation for all transactions in the block into a single slice.
	numInputs := 0
	for _, tx := range block.Transactions() {
		numInputs += len(tx.MsgTx().TxIn)
	}
	txValItems := make([]*txValidateItem, 0, numInputs)
	for _, tx := range block.Transactions() {
		hash := tx.Hash()

		// If the HashCache is present, and it doesn't yet contain the
		// partial sighashes for this transaction, then we add the
		// sighashes for the transaction. This allows us to take
		// advantage of the potential speed savings due to the new
		// digest algorithm (BIP0143).
		if segwitActive && tx.HasWitness() && hashCache != nil &&
			!hashCache.ContainsHashes(hash) {

			hashCache.AddSigHashes(tx.MsgTx(), utxoView)
		}

		var cachedHashes *txscript.TxSigHashes
		if segwitActive && tx.HasWitness() {
			if hashCache != nil {
				cachedHashes, _ = hashCache.GetSigHashes(hash)
			} else {
				cachedHashes = txscript.NewTxSigHashes(
					tx.MsgTx(), utxoView,
				)
			}
		}

		for txInIdx, txIn := range tx.MsgTx().TxIn {
			// Skip coinbases.
			if txIn.PreviousOutPoint.Index == math.MaxUint32 {
				continue
			}

			txVI := &txValidateItem{
				txInIndex: txInIdx,
				txIn:      txIn,
				tx:        tx,
				sigHashes: cachedHashes,
			}
			txValItems = append(txValItems, txVI)
		}
	}

	// Validate all of the inputs.
	validator := newTxValidator(utxoView, scriptFlags, sigCache, hashCache)
	start := time.Now()
	if err := validator.Validate(txValItems); err != nil {
		return err
	}
	elapsed := time.Since(start)

	log.Tracef("block %v took %v to verify", block.Hash(), elapsed)

	// If the HashCache is present, once we have validated the block, we no
	// longer need the cached hashes for these transactions, so we purge
	// them from the cache.
	if segwitActive && hashCache != nil {
		for _, tx := range block.Transactions() {
			if tx.MsgTx().HasWitness() {
				hashCache.PurgeSigHashes(tx.Hash())
			}
		}
	}

	return nil
}
