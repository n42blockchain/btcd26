// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"testing"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/chainhash/v2"
)

// TestIsAssumeValidAncestor exercises the ancestry check that gates skipping
// script verification.  Only the assume-valid block and its ancestors on the
// same chain may be skipped; descendants, side-chain blocks, an unset hash,
// and a not-yet-known assume-valid block must all keep full validation.
func TestIsAssumeValidAncestor(t *testing.T) {
	chain := newFakeChain(&chaincfg.MainNetParams)
	genesis := chain.bestChain.Tip()

	// Build a main chain of 20 nodes on top of genesis and register them in
	// the block index.  nodes[i] sits at height i+1.
	nodes := chainedNodes(genesis, 20)
	for _, n := range nodes {
		chain.index.AddNode(n)
	}

	// Build a small side chain forking off the height-6 main-chain node so we
	// can confirm a same-height side block is not mistaken for an ancestor.
	sideNodes := chainedNodes(nodes[5], 4)
	for _, n := range sideNodes {
		chain.index.AddNode(n)
	}

	// Use a mid-chain main node as the assume-valid block.
	avNode := nodes[10]
	chain.assumeValid = avNode.hash

	assert := func(name string, node *blockNode, want bool) {
		t.Helper()
		if got := chain.isAssumeValidAncestor(node); got != want {
			t.Fatalf("%s: isAssumeValidAncestor(h%d) = %v, want %v",
				name, node.height, got, want)
		}
	}

	// The assume-valid block itself and every ancestor are covered.
	assert("assumevalid block", avNode, true)
	assert("direct parent", nodes[9], true)
	assert("deep ancestor", nodes[0], true)
	assert("genesis", genesis, true)

	// Descendants of the assume-valid block keep full validation.
	assert("child", nodes[11], false)
	assert("tip", nodes[19], false)

	// A side-chain block at or below the assume-valid height but not on its
	// chain must not be skipped.  sideNodes fork off height-6 (nodes[5]) so
	// they live at heights 7..10 — overlapping the assume-valid height.
	for _, sn := range sideNodes {
		if sn.height > avNode.height {
			continue
		}
		assert("side-chain block", sn, false)
	}

	// Disabling the optimization (zero hash) keeps full validation for all.
	chain.assumeValid = chainhash.Hash{}
	assert("disabled: ancestor", nodes[0], false)
	assert("disabled: av block", avNode, false)

	// An assume-valid hash that is not (yet) in the index conservatively
	// keeps full validation rather than skipping anything.
	var unknown chainhash.Hash
	unknown[0] = 0xff
	chain.assumeValid = unknown
	assert("unknown hash: ancestor", nodes[0], false)
}
