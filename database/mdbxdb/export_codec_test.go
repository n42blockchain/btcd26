// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package mdbxdb

import "github.com/klauspost/compress/zstd"

// Mdbxdb_SetTableCodec is a test-only re-export of SetTableCodec for use by
// other-package tests that need to install a custom codec.  Outside of
// tests, SetTableCodec is the public entry point.
//
// The leading underscore in the name keeps it out of normal autocomplete
// while staying exported so _test packages can call it.
func Mdbxdb_SetTableCodec(name string, c Codec) Codec {
	return SetTableCodec(name, c)
}

// Mdbxdb_NewZstdCodecForTest creates a zstd codec with the given name and
// the default speed level.  Test helper.
func Mdbxdb_NewZstdCodecForTest(name string) Codec {
	return newZstdCodec(name, zstd.SpeedDefault)
}
