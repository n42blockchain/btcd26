// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package blockchain

import (
	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/database/ffldb"
	"github.com/btcsuite/btcd/database/mdbxdb"
)

// runWithMaxBlockFileSize dispatches to the active driver's
// TstRunWithMaxBlockFileSize helper so tests don't have to special-case the
// driver name.  Both ffldb and mdbxdb expose this helper with the same
// shape; we pick at runtime based on the concrete type of db.
func runWithMaxBlockFileSize(db database.DB, size uint32, fn func()) {
	switch db.Type() {
	case "ffldb":
		ffldb.TstRunWithMaxBlockFileSize(db, size, fn)
	case "mdbxdb":
		mdbxdb.TstRunWithMaxBlockFileSize(db, size, fn)
	default:
		// Unknown driver: just run fn at the default block file size.
		fn()
	}
}
