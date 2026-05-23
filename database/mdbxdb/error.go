// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package mdbxdb

import (
	"encoding/binary"

	"github.com/btcsuite/btcd/database"
	"github.com/erigontech/mdbx-go/mdbx"
)

// byteOrder is the preferred byte order used through the database and block
// files.  Sometimes big endian will be used to allow ordered byte sortable
// integer values; little endian is used for backwards compatibility with the
// ffldb on-disk block file format.
var byteOrder = binary.LittleEndian

// Common error strings shared with ffldb to keep test expectations identical.
const (
	errDbNotOpenStr = "database is not open"
	errTxClosedStr  = "database tx is closed"
)

// makeDbErr creates a database.Error with the provided code, description, and
// underlying error.
func makeDbErr(c database.ErrorCode, desc string, err error) database.Error {
	return database.Error{ErrorCode: c, Description: desc, Err: err}
}

// convertMdbxErr translates an MDBX C library error into a database.Error,
// mapping recognized cases to the appropriate ErrorCode and falling back to
// ErrDriverSpecific for everything else.  Pass nil through unchanged.
func convertMdbxErr(desc string, mErr error) error {
	if mErr == nil {
		return nil
	}
	code := database.ErrDriverSpecific
	switch {
	case mdbx.IsErrno(mErr, mdbx.Corrupted),
		mdbx.IsErrno(mErr, mdbx.Panic):
		code = database.ErrCorruption
	case mdbx.IsErrno(mErr, mdbx.BadTxn),
		mdbx.IsErrno(mErr, mdbx.TxnFull):
		code = database.ErrTxClosed
	case mdbx.IsErrno(mErr, mdbx.BadValSize),
		mdbx.IsErrno(mErr, mdbx.BadDBI),
		mdbx.IsErrno(mErr, mdbx.Invalid):
		code = database.ErrInvalid
	}
	return database.Error{ErrorCode: code, Description: desc, Err: mErr}
}
