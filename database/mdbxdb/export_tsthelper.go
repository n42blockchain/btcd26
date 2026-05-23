// Copyright (c) 2015-2016 The btcsuite developers
// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package mdbxdb

import (
	"github.com/btcsuite/btcd/database"
)

// TstRunWithMaxBlockFileSize runs the passed function with the maximum allowed
// flat file size for the database set to the provided value.  The value is
// restored upon completion.  For use by tests only.
func TstRunWithMaxBlockFileSize(idb database.DB, size uint32, fn func()) {
	pdb := idb.(*db)
	orig := pdb.store.maxBlockFileSize
	pdb.store.maxBlockFileSize = size
	fn()
	pdb.store.maxBlockFileSize = orig
}
