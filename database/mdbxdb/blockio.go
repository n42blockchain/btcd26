// Copyright (c) 2015-2016 The btcsuite developers
// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// This file implements reading, writing, and otherwise working with the flat
// files that house the actual blocks.  It is intentionally byte-compatible
// with ffldb so that on-disk block files can be shared between drivers during
// the ffldb→mdbxdb migration.

package mdbxdb

import (
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/wire/v2"
)

const (
	// blockFileExtension is the extension that's used to store the block
	// files on the disk.  The value matches ffldb to allow sharing files.
	blockFileExtension = ".fdb"

	// blockFilenameTemplate is the printf template used to name block files.
	blockFilenameTemplate = "%09d" + blockFileExtension

	// maxOpenFiles is the max number of open block files to maintain in the
	// LRU cache (not counting the current write file).
	maxOpenFiles = 25

	// maxBlockFileSize is the maximum size for each .fdb file.
	maxBlockFileSize uint32 = 512 * 1024 * 1024 // 512 MiB

	// blockLocSize is the size of a serialized block location row.
	//
	// Format:  [0:4] file, [4:8] offset, [8:12] length.
	blockLocSize = 12
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// filer mirrors *os.File enough for mockability in tests.
type filer interface {
	io.Closer
	io.WriterAt
	io.ReaderAt
	Truncate(size int64) error
	Sync() error
}

// lockableFile pairs a block file with a RWMutex that synchronizes readers
// against an exclusive writer (truncation / append).
type lockableFile struct {
	sync.RWMutex
	file filer
}

// writeCursor tracks where new blocks will be written.
type writeCursor struct {
	sync.RWMutex
	curFile    *lockableFile
	curFileNum uint32
	curOffset  uint32
}

// blockStore manages flat-file block storage with a bounded open-file LRU.
type blockStore struct {
	network          wire.BitcoinNet
	basePath         string
	maxBlockFileSize uint32

	obfMutex         sync.RWMutex
	lruMutex         sync.Mutex
	openBlocksLRU    *list.List
	fileNumToLRUElem map[uint32]*list.Element
	openBlockFiles   map[uint32]*lockableFile

	writeCursor *writeCursor

	openFileFunc      func(fileNum uint32) (*lockableFile, error)
	openWriteFileFunc func(fileNum uint32) (filer, error)
	deleteFileFunc    func(fileNum uint32) error

	// segs is the pool of cold-segment readers.  Lazily populated as cold
	// blocks are requested.
	segs *segPool
}

// blockLocation identifies a particular block file and location.
type blockLocation struct {
	blockFileNum uint32
	fileOffset   uint32
	blockLen     uint32
}

func deserializeBlockLoc(serializedLoc []byte) blockLocation {
	return blockLocation{
		blockFileNum: byteOrder.Uint32(serializedLoc[0:4]),
		fileOffset:   byteOrder.Uint32(serializedLoc[4:8]),
		blockLen:     byteOrder.Uint32(serializedLoc[8:12]),
	}
}

func serializeBlockLoc(loc blockLocation) []byte {
	var serializedData [12]byte
	byteOrder.PutUint32(serializedData[0:4], loc.blockFileNum)
	byteOrder.PutUint32(serializedData[4:8], loc.fileOffset)
	byteOrder.PutUint32(serializedData[8:12], loc.blockLen)
	return serializedData[:]
}

func blockFilePath(dbPath string, fileNum uint32) string {
	return filepath.Join(dbPath, fmt.Sprintf(blockFilenameTemplate, fileNum))
}

func (s *blockStore) openWriteFile(fileNum uint32) (filer, error) {
	filePath := blockFilePath(s.basePath, fileNum)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		str := fmt.Sprintf("failed to open file %q: %v", filePath, err)
		return nil, makeDbErr(database.ErrDriverSpecific, str, err)
	}
	return file, nil
}

// openFile opens a read-only file handle, evicting the LRU tail if needed.
// MUST be called with s.obfMutex locked for writes.
func (s *blockStore) openFile(fileNum uint32) (*lockableFile, error) {
	filePath := blockFilePath(s.basePath, fileNum)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, makeDbErr(database.ErrDriverSpecific, err.Error(), err)
	}
	blockFile := &lockableFile{file: file}

	s.lruMutex.Lock()
	lruList := s.openBlocksLRU
	if lruList.Len() >= maxOpenFiles {
		lruFileNum := lruList.Remove(lruList.Back()).(uint32)
		s.closeFile(lruFileNum)
	}
	s.fileNumToLRUElem[fileNum] = lruList.PushFront(fileNum)
	s.lruMutex.Unlock()

	s.openBlockFiles[fileNum] = blockFile
	return blockFile, nil
}

func (s *blockStore) closeFile(fileNum uint32) {
	blockFile := s.openBlockFiles[fileNum]
	if blockFile == nil {
		return
	}
	blockFile.Lock()
	_ = blockFile.file.Close()
	blockFile.Unlock()

	delete(s.openBlockFiles, fileNum)
	delete(s.fileNumToLRUElem, fileNum)
}

func (s *blockStore) deleteFile(fileNum uint32) error {
	filePath := blockFilePath(s.basePath, fileNum)
	if s.openBlockFiles[fileNum] != nil {
		err := fmt.Errorf("attempted to delete open file at %v", filePath)
		return makeDbErr(database.ErrDriverSpecific, err.Error(), err)
	}
	if err := os.Remove(filePath); err != nil {
		return makeDbErr(database.ErrDriverSpecific, err.Error(), err)
	}
	return nil
}

// blockFile returns the handle for fileNum with a read-lock held; the caller
// MUST release that lock with .RUnlock() once done.
func (s *blockStore) blockFile(fileNum uint32) (*lockableFile, error) {
	wc := s.writeCursor
	wc.RLock()
	if fileNum == wc.curFileNum && wc.curFile.file != nil {
		obf := wc.curFile
		obf.RLock()
		wc.RUnlock()
		return obf, nil
	}
	wc.RUnlock()

	s.obfMutex.RLock()
	if obf, ok := s.openBlockFiles[fileNum]; ok {
		s.lruMutex.Lock()
		s.openBlocksLRU.MoveToFront(s.fileNumToLRUElem[fileNum])
		s.lruMutex.Unlock()

		obf.RLock()
		s.obfMutex.RUnlock()
		return obf, nil
	}
	s.obfMutex.RUnlock()

	s.obfMutex.Lock()
	if obf, ok := s.openBlockFiles[fileNum]; ok {
		obf.RLock()
		s.obfMutex.Unlock()
		return obf, nil
	}
	obf, err := s.openFileFunc(fileNum)
	if err != nil {
		s.obfMutex.Unlock()
		return nil, err
	}
	obf.RLock()
	s.obfMutex.Unlock()
	return obf, nil
}

// writeData is called with the write-cursor file lock held.
func (s *blockStore) writeData(data []byte, fieldName string) error {
	wc := s.writeCursor
	n, err := wc.curFile.file.WriteAt(data, int64(wc.curOffset))
	wc.curOffset += uint32(n)
	if err != nil {
		if errors.Is(err, syscall.ENOSPC) {
			log.Errorf("%v. Cannot save any more blocks due to the "+
				"disk being full -- exiting", err)
			os.Exit(1)
		}
		str := fmt.Sprintf("failed to write %s to file %d at offset %d: %v",
			fieldName, wc.curFileNum, wc.curOffset-uint32(n), err)
		return makeDbErr(database.ErrDriverSpecific, str, err)
	}
	return nil
}

// writeBlock appends a raw block to the current write file.
//
// Format: <network><block length><serialized block><checksum>.
func (s *blockStore) writeBlock(rawBlock []byte) (blockLocation, error) {
	blockLen := uint32(len(rawBlock))
	fullLen := blockLen + 12

	wc := s.writeCursor
	finalOffset := wc.curOffset + fullLen
	if finalOffset < wc.curOffset || finalOffset > s.maxBlockFileSize {
		wc.Lock()
		wc.curFile.Lock()
		if wc.curFile.file != nil {
			_ = wc.curFile.file.Close()
			wc.curFile.file = nil
		}
		wc.curFile.Unlock()

		wc.curFileNum++
		wc.curOffset = 0
		wc.Unlock()
	}

	wc.curFile.Lock()
	defer wc.curFile.Unlock()

	if wc.curFile.file == nil {
		file, err := s.openWriteFileFunc(wc.curFileNum)
		if err != nil {
			return blockLocation{}, err
		}
		wc.curFile.file = file
	}

	origOffset := wc.curOffset
	hasher := crc32.New(castagnoli)
	var scratch [4]byte
	byteOrder.PutUint32(scratch[:], uint32(s.network))
	if err := s.writeData(scratch[:], "network"); err != nil {
		return blockLocation{}, err
	}
	_, _ = hasher.Write(scratch[:])

	byteOrder.PutUint32(scratch[:], blockLen)
	if err := s.writeData(scratch[:], "block length"); err != nil {
		return blockLocation{}, err
	}
	_, _ = hasher.Write(scratch[:])

	if err := s.writeData(rawBlock, "block"); err != nil {
		return blockLocation{}, err
	}
	_, _ = hasher.Write(rawBlock)

	if err := s.writeData(hasher.Sum(nil), "checksum"); err != nil {
		return blockLocation{}, err
	}

	return blockLocation{
		blockFileNum: wc.curFileNum,
		fileOffset:   origOffset,
		blockLen:     fullLen,
	}, nil
}

// readBlock reads a full block from disk and verifies its CRC + network magic.
//
// If the location's blockFileNum has the cold-segment bit set, the read is
// routed through the cold-segment reader pool instead of the hot .fdb path.
func (s *blockStore) readBlock(hash *chainhash.Hash, loc blockLocation) ([]byte, error) {
	if isColdLocation(loc) {
		r, err := s.segs.reader(coldSegNum(loc))
		if err != nil {
			return nil, err
		}
		return r.readBlock(hash)
	}
	blockFile, err := s.blockFile(loc.blockFileNum)
	if err != nil {
		return nil, err
	}

	serializedData := make([]byte, loc.blockLen)
	n, err := blockFile.file.ReadAt(serializedData, int64(loc.fileOffset))
	blockFile.RUnlock()
	if err != nil {
		str := fmt.Sprintf("failed to read block %s from file %d, offset %d: %v",
			hash, loc.blockFileNum, loc.fileOffset, err)
		return nil, makeDbErr(database.ErrDriverSpecific, str, err)
	}

	serializedChecksum := binary.BigEndian.Uint32(serializedData[n-4:])
	calculatedChecksum := crc32.Checksum(serializedData[:n-4], castagnoli)
	if serializedChecksum != calculatedChecksum {
		str := fmt.Sprintf("block data for block %s checksum does not match "+
			"- got %x, want %x", hash, calculatedChecksum, serializedChecksum)
		return nil, makeDbErr(database.ErrCorruption, str, nil)
	}

	serializedNet := byteOrder.Uint32(serializedData[:4])
	if serializedNet != uint32(s.network) {
		str := fmt.Sprintf("block data for block %s is for the wrong "+
			"network - got %d, want %d", hash, serializedNet,
			uint32(s.network))
		return nil, makeDbErr(database.ErrDriverSpecific, str, nil)
	}

	return serializedData[8 : n-4], nil
}

func (s *blockStore) readBlockRegion(loc blockLocation, offset, numBytes uint32) ([]byte, error) {
	if isColdLocation(loc) {
		// Cold locations don't carry the original block hash inline.
		// The caller (FetchBlockRegion) only has hash available at the
		// surrounding scope, so this code path is unreachable directly
		// — readBlockRegionByHash is used by the routing layer.  Keep
		// a defensive ErrInvalid here so any future caller learns.
		return nil, makeDbErr(database.ErrInvalid,
			"readBlockRegion(loc) on cold location: use "+
				"readBlockRegionCold(hash, ...) instead", nil)
	}
	blockFile, err := s.blockFile(loc.blockFileNum)
	if err != nil {
		return nil, err
	}

	readOffset := loc.fileOffset + 8 + offset
	serializedData := make([]byte, numBytes)
	_, err = blockFile.file.ReadAt(serializedData, int64(readOffset))
	blockFile.RUnlock()
	if err != nil {
		str := fmt.Sprintf("failed to read region from block file %d, "+
			"offset %d, len %d: %v", loc.blockFileNum, readOffset,
			numBytes, err)
		return nil, makeDbErr(database.ErrDriverSpecific, str, err)
	}
	return serializedData, nil
}

func (s *blockStore) syncBlocks() error {
	wc := s.writeCursor
	wc.RLock()
	defer wc.RUnlock()

	wc.curFile.RLock()
	defer wc.curFile.RUnlock()
	if wc.curFile.file == nil {
		return nil
	}

	if err := wc.curFile.file.Sync(); err != nil {
		if errors.Is(err, syscall.ENOSPC) {
			log.Errorf("%v. Cannot save any more blocks due to the "+
				"disk being full -- exiting", err)
			os.Exit(1)
		}
		str := fmt.Sprintf("failed to sync file %d: %v", wc.curFileNum, err)
		return makeDbErr(database.ErrDriverSpecific, str, err)
	}
	return nil
}

// handleRollback truncates/deletes block files until the write cursor matches
// the supplied (fileNum, offset).  Best-effort: errors are only logged.
func (s *blockStore) handleRollback(oldBlockFileNum, oldBlockOffset uint32) {
	wc := s.writeCursor
	wc.Lock()
	defer wc.Unlock()

	if wc.curFileNum == oldBlockFileNum && wc.curOffset == oldBlockOffset {
		return
	}

	defer func() {
		wc.curFileNum = oldBlockFileNum
		wc.curOffset = oldBlockOffset
	}()

	log.Debugf("ROLLBACK: Rolling back to file %d, offset %d",
		oldBlockFileNum, oldBlockOffset)

	if wc.curFileNum > oldBlockFileNum {
		wc.curFile.Lock()
		if wc.curFile.file != nil {
			_ = wc.curFile.file.Close()
			wc.curFile.file = nil
		}
		wc.curFile.Unlock()
	}
	for ; wc.curFileNum > oldBlockFileNum; wc.curFileNum-- {
		if err := s.deleteFileFunc(wc.curFileNum); err != nil {
			log.Warnf("ROLLBACK: Failed to delete block file %d: %v",
				wc.curFileNum, err)
			return
		}
	}

	wc.curFile.Lock()
	if wc.curFile.file == nil {
		obf, err := s.openWriteFileFunc(wc.curFileNum)
		if err != nil {
			wc.curFile.Unlock()
			log.Warnf("ROLLBACK: %v", err)
			return
		}
		wc.curFile.file = obf
	}

	if err := wc.curFile.file.Truncate(int64(oldBlockOffset)); err != nil {
		wc.curFile.Unlock()
		log.Warnf("ROLLBACK: Failed to truncate file %d: %v",
			wc.curFileNum, err)
		return
	}

	err := wc.curFile.file.Sync()
	wc.curFile.Unlock()
	if err != nil {
		log.Warnf("ROLLBACK: Failed to sync file %d: %v",
			wc.curFileNum, err)
	}
}

// scanBlockFiles finds the on-disk first/last/last-len block file.
func scanBlockFiles(dbPath string) (int, int, uint32, error) {
	firstFile, lastFile, lastFileLen := -1, -1, uint32(0)

	files, err := filepath.Glob(filepath.Join(dbPath, "*"+blockFileExtension))
	if err != nil {
		return 0, 0, 0, err
	}
	sort.Strings(files)

	if len(files) == 0 {
		return firstFile, lastFile, lastFileLen, nil
	}

	firstFile, err = strconv.Atoi(strings.TrimSuffix(filepath.Base(files[0]),
		blockFileExtension))
	if err != nil {
		return 0, 0, 0, fmt.Errorf("scanBlockFiles error: %v", err)
	}
	lastFile, err = strconv.Atoi(strings.TrimSuffix(
		filepath.Base(files[len(files)-1]), blockFileExtension))
	if err != nil {
		return 0, 0, 0, fmt.Errorf("scanBlockFiles error: %v", err)
	}

	filePath := blockFilePath(dbPath, uint32(lastFile))
	st, err := os.Stat(filePath)
	if err != nil {
		return 0, 0, 0, err
	}
	lastFileLen = uint32(st.Size())

	log.Tracef("Scan found latest block file #%d with length %d",
		lastFile, lastFileLen)
	return firstFile, lastFile, lastFileLen, nil
}

func newBlockStore(basePath string, network wire.BitcoinNet) (*blockStore, error) {
	_, fileNum, fileOff, err := scanBlockFiles(basePath)
	if err != nil {
		return nil, err
	}
	if fileNum == -1 {
		fileNum = 0
		fileOff = 0
	}

	store := &blockStore{
		network:          network,
		basePath:         basePath,
		maxBlockFileSize: maxBlockFileSize,
		openBlockFiles:   make(map[uint32]*lockableFile),
		openBlocksLRU:    list.New(),
		fileNumToLRUElem: make(map[uint32]*list.Element),

		writeCursor: &writeCursor{
			curFile:    &lockableFile{},
			curFileNum: uint32(fileNum),
			curOffset:  fileOff,
		},

		segs: newSegPool(basePath),
	}
	store.openFileFunc = store.openFile
	store.openWriteFileFunc = store.openWriteFile
	store.deleteFileFunc = store.deleteFile
	return store, nil
}

// readBlockColdRegion reads `numBytes` starting at `offset` of the
// decompressed block identified by `hash` within the cold segment encoded
// in `loc`.  Used by tx.FetchBlockRegion when the routed location is cold.
func (s *blockStore) readBlockColdRegion(hash *chainhash.Hash,
	loc blockLocation, offset, numBytes uint32) ([]byte, error) {

	r, err := s.segs.reader(coldSegNum(loc))
	if err != nil {
		return nil, err
	}
	return r.readBlockRegion(hash, offset, numBytes)
}
