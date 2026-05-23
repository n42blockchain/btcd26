// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package mdbxdb

import (
	"fmt"

	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/btcsuite/btclog"
)

// dbType is the database driver name used to register this backend.
const dbType = "mdbxdb"

// log is the package logger.  It defaults to a disabled logger and is replaced
// by UseLogger when registered drivers receive a logger.
var log = btclog.Disabled

// parseArgs parses the arguments from the database Open/Create methods.
//
// Open/Create both take (dbPath string, network wire.BitcoinNet).
func parseArgs(funcName string, args ...interface{}) (string, wire.BitcoinNet, error) {
	if len(args) != 2 {
		return "", 0, fmt.Errorf("invalid arguments to %s.%s -- "+
			"expected database path and block network", dbType,
			funcName)
	}

	dbPath, ok := args[0].(string)
	if !ok {
		return "", 0, fmt.Errorf("first argument to %s.%s is invalid -- "+
			"expected database path string", dbType, funcName)
	}

	network, ok := args[1].(wire.BitcoinNet)
	if !ok {
		return "", 0, fmt.Errorf("second argument to %s.%s is invalid -- "+
			"expected block network", dbType, funcName)
	}

	return dbPath, network, nil
}

// openDBDriver is the callback provided during driver registration that opens
// an existing database for use.
func openDBDriver(args ...interface{}) (database.DB, error) {
	dbPath, network, err := parseArgs("Open", args...)
	if err != nil {
		return nil, err
	}

	return openDB(dbPath, network, false)
}

// createDBDriver is the callback provided during driver registration that
// creates, initializes, and opens a database for use.
func createDBDriver(args ...interface{}) (database.DB, error) {
	dbPath, network, err := parseArgs("Create", args...)
	if err != nil {
		return nil, err
	}

	return openDB(dbPath, network, true)
}

// useLogger is the callback provided during driver registration that sets the
// current logger to the provided one.
func useLogger(logger btclog.Logger) {
	log = logger
}

func init() {
	driver := database.Driver{
		DbType:    dbType,
		Create:    createDBDriver,
		Open:      openDBDriver,
		UseLogger: useLogger,
	}
	if err := database.RegisterDriver(driver); err != nil {
		panic(fmt.Sprintf("Failed to register database driver %q: %v",
			dbType, err))
	}
}
