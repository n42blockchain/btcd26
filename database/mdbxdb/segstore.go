// Copyright (c) 2026 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

// segstore.go implements cold-segment block storage.  A "cold segment" is an
// immutable file under <db>/cold/<segNum>.seg holding many old blocks
// compressed with dict-zstd in fixed-size frames.  An accompanying .idx
// file maps block-hash → (frameIdx, blockOffsetWithinFrame, blockLen).
//
// The block-index row (stored in MDBX) re-uses ffldb's 12-byte blockLocation
// format unchanged; cold segments are signaled by setting the high bit of
// the blockFileNum field.  Routing code in blockStore inspects that bit and
// dispatches accordingly.
//
// Segments are never modified after they are written.  The dictionary bytes
// embedded in the .seg header pin the decoder; rebuilding a segment with a
// new dictionary requires writing a new segment file (different segNum).

package mdbxdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/btcsuite/btcd/chainhash/v2"
	"github.com/btcsuite/btcd/database"
	"github.com/klauspost/compress/zstd"
)

const (
	// coldSegBit is the high bit of blockFileNum that flags a block
	// stored in a cold segment instead of a hot .fdb file.
	coldSegBit uint32 = 0x80000000

	// segDirName is the subdirectory under <db>/ where cold segments
	// live alongside the hot .fdb files.
	segDirName = "cold"

	// segExtension is the file extension for cold segment data files.
	segExtension = ".seg"

	// segIdxExtension is the file extension for cold segment index files.
	segIdxExtension = ".idx"

	// segMagic is the 4-byte signature at the head of every .seg file.
	// It encodes "MSEG" little-endian as 0x4745534D.
	segMagic uint32 = 0x4745534D

	// segVersion is the on-disk format version.  Bumped on incompatible
	// layout changes.
	segVersion uint32 = 1
)

// isColdLocation returns whether the blockFileNum field of loc points to a
// cold segment rather than a hot .fdb file.
func isColdLocation(loc blockLocation) bool {
	return loc.blockFileNum&coldSegBit != 0
}

// coldSegNum returns the underlying segment number stripping the high bit.
func coldSegNum(loc blockLocation) uint32 {
	return loc.blockFileNum &^ coldSegBit
}

// makeColdLocation packs a cold-segment location into the 12-byte
// blockLocation representation: segNum (high bit set) + offset within the
// decoded block + decoded block length.  The offset is logical (post
// decompression) so that FetchBlockRegion semantics match the hot path.
func makeColdLocation(segNum uint32, blockLen uint32) blockLocation {
	return blockLocation{
		blockFileNum: segNum | coldSegBit,
		fileOffset:   0, // hashedKey lookup resolves frame + offset
		blockLen:     blockLen,
	}
}

// segHeader is the fixed-size head of every .seg file.
//
// Layout (little-endian):
//
//	0..4    magic (uint32)
//	4..8    version (uint32)
//	8..12   network (uint32)
//	12..16  frame count (uint32)
//	16..20  block count (uint32)
//	20..24  dictionary length (uint32)
//	24..28  reserved
//	28..32  header crc32c (uint32)
//	32..32+dictLen  zstd training dictionary bytes
//
// Frames follow immediately after the dictionary.
type segHeader struct {
	Magic      uint32
	Version    uint32
	Network    uint32
	FrameCount uint32
	BlockCount uint32
	DictLen    uint32
	Reserved   uint32
	HeaderCRC  uint32
}

// segIndexEntry is the index row for a single block, packed in the .idx
// file.  It is 60 bytes wide.
//
//	0..32    block hash
//	32..36   frame index (uint32)
//	36..40   block offset within decompressed frame (uint32)
//	40..44   block length (uint32) — decompressed
//	44..48   reserved
//	48..52   frame raw offset within seg (uint32) — start of the encoded frame
//	52..56   frame compressed length (uint32)
//	56..60   frame decoded length (uint32)
type segIndexEntry struct {
	Hash         chainhash.Hash
	FrameIdx     uint32
	BlockOffInFr uint32
	BlockLen     uint32
	Reserved     uint32
	FrameOff     uint32
	FrameCompLen uint32
	FrameDecLen  uint32
}

const segIndexEntrySize = 60

// segReader opens a single cold segment for read-only access.  It is
// concurrency-safe — multiple goroutines may read different blocks at the
// same time.  Internally it holds the .seg file mmap-style via os.File
// ReadAt, plus an in-memory hash→entry map loaded from the .idx file.
type segReader struct {
	segNum uint32

	file *os.File

	header segHeader
	dict   []byte

	dec *zstd.Decoder

	// idx maps block hash → segIndexEntry.  Sized once, then read-only.
	idx map[chainhash.Hash]segIndexEntry

	// frameCache caches recently decoded frames keyed by frame index.
	// Sized by config; default is bounded to keep memory in check.
	frameCacheMu sync.Mutex
	frameCache   map[uint32][]byte
	frameCacheLR []uint32 // simple LRU; oldest at index 0
	frameCacheN  int
}

const defaultFrameCacheSize = 8

// openSegReader opens the segment file for segNum within basePath/cold/.
func openSegReader(basePath string, segNum uint32) (*segReader, error) {
	segPath := segFilePath(basePath, segNum)
	idxPath := segIndexPath(basePath, segNum)

	f, err := os.Open(segPath)
	if err != nil {
		return nil, makeDbErr(database.ErrDriverSpecific,
			fmt.Sprintf("open seg %q: %v", segPath, err), err)
	}

	r := &segReader{segNum: segNum, file: f, frameCacheN: defaultFrameCacheSize}
	if err := r.readHeader(); err != nil {
		f.Close()
		return nil, err
	}
	if err := r.readIndex(idxPath); err != nil {
		f.Close()
		return nil, err
	}

	decOpts := []zstd.DOption{
		zstd.WithDecoderConcurrency(1),
	}
	if len(r.dict) >= 4 && r.dict[0] == 0x37 && r.dict[1] == 0xA4 &&
		r.dict[2] == 0x30 && r.dict[3] == 0xEC {
		decOpts = append(decOpts, zstd.WithDecoderDicts(r.dict))
	}
	dec, err := zstd.NewReader(nil, decOpts...)
	if err != nil {
		f.Close()
		return nil, makeDbErr(database.ErrDriverSpecific,
			fmt.Sprintf("init zstd decoder: %v", err), err)
	}
	r.dec = dec

	r.frameCache = make(map[uint32][]byte, r.frameCacheN)
	return r, nil
}

func segFilePath(basePath string, segNum uint32) string {
	return filepath.Join(basePath, segDirName, fmt.Sprintf("%09d%s",
		segNum, segExtension))
}

func segIndexPath(basePath string, segNum uint32) string {
	return filepath.Join(basePath, segDirName, fmt.Sprintf("%09d%s",
		segNum, segIdxExtension))
}

// readHeader parses the segHeader and the embedded dictionary bytes.
func (r *segReader) readHeader() error {
	var raw [32]byte
	if _, err := r.file.ReadAt(raw[:], 0); err != nil {
		return makeDbErr(database.ErrCorruption,
			fmt.Sprintf("read seg header: %v", err), err)
	}
	r.header = segHeader{
		Magic:      binary.LittleEndian.Uint32(raw[0:4]),
		Version:    binary.LittleEndian.Uint32(raw[4:8]),
		Network:    binary.LittleEndian.Uint32(raw[8:12]),
		FrameCount: binary.LittleEndian.Uint32(raw[12:16]),
		BlockCount: binary.LittleEndian.Uint32(raw[16:20]),
		DictLen:    binary.LittleEndian.Uint32(raw[20:24]),
		Reserved:   binary.LittleEndian.Uint32(raw[24:28]),
		HeaderCRC:  binary.LittleEndian.Uint32(raw[28:32]),
	}
	if r.header.Magic != segMagic {
		return makeDbErr(database.ErrCorruption,
			fmt.Sprintf("bad seg magic %#x", r.header.Magic), nil)
	}
	if r.header.Version != segVersion {
		return makeDbErr(database.ErrInvalid,
			fmt.Sprintf("unsupported seg version %d", r.header.Version), nil)
	}
	// Verify header CRC over bytes 0..28.
	got := crc32.Checksum(raw[:28], castagnoli)
	if got != r.header.HeaderCRC {
		return makeDbErr(database.ErrCorruption,
			fmt.Sprintf("seg header crc mismatch: got %x want %x",
				got, r.header.HeaderCRC), nil)
	}
	// Read dictionary.
	if r.header.DictLen > 0 {
		r.dict = make([]byte, r.header.DictLen)
		if _, err := r.file.ReadAt(r.dict, 32); err != nil {
			return makeDbErr(database.ErrCorruption,
				fmt.Sprintf("read seg dict: %v", err), err)
		}
	}
	return nil
}

// readIndex loads the .idx file into r.idx.
func (r *segReader) readIndex(idxPath string) error {
	data, err := os.ReadFile(idxPath)
	if err != nil {
		return makeDbErr(database.ErrCorruption,
			fmt.Sprintf("read seg idx %q: %v", idxPath, err), err)
	}
	if len(data)%segIndexEntrySize != 0 {
		return makeDbErr(database.ErrCorruption,
			fmt.Sprintf("seg idx %q has length %d not a multiple of %d",
				idxPath, len(data), segIndexEntrySize), nil)
	}
	n := len(data) / segIndexEntrySize
	r.idx = make(map[chainhash.Hash]segIndexEntry, n)
	for i := 0; i < n; i++ {
		off := i * segIndexEntrySize
		var e segIndexEntry
		copy(e.Hash[:], data[off:off+32])
		e.FrameIdx = binary.LittleEndian.Uint32(data[off+32 : off+36])
		e.BlockOffInFr = binary.LittleEndian.Uint32(data[off+36 : off+40])
		e.BlockLen = binary.LittleEndian.Uint32(data[off+40 : off+44])
		e.Reserved = binary.LittleEndian.Uint32(data[off+44 : off+48])
		e.FrameOff = binary.LittleEndian.Uint32(data[off+48 : off+52])
		e.FrameCompLen = binary.LittleEndian.Uint32(data[off+52 : off+56])
		e.FrameDecLen = binary.LittleEndian.Uint32(data[off+56 : off+60])
		r.idx[e.Hash] = e
	}
	return nil
}

// readBlock returns the raw block bytes for hash from this segment.
func (r *segReader) readBlock(hash *chainhash.Hash) ([]byte, error) {
	e, ok := r.idx[*hash]
	if !ok {
		return nil, makeDbErr(database.ErrBlockNotFound,
			fmt.Sprintf("block %s not in segment %d", hash, r.segNum), nil)
	}
	frame, err := r.frame(e.FrameIdx, e.FrameOff, e.FrameCompLen, e.FrameDecLen)
	if err != nil {
		return nil, err
	}
	if e.BlockOffInFr+e.BlockLen > uint32(len(frame)) {
		return nil, makeDbErr(database.ErrCorruption,
			fmt.Sprintf("block %s spans past frame end", hash), nil)
	}
	out := make([]byte, e.BlockLen)
	copy(out, frame[e.BlockOffInFr:e.BlockOffInFr+e.BlockLen])
	return out, nil
}

// readBlockRegion returns offset..offset+numBytes of the decoded block bytes
// for hash.  Useful for FetchBlockRegion-style consumers.
func (r *segReader) readBlockRegion(hash *chainhash.Hash, offset, numBytes uint32) ([]byte, error) {
	e, ok := r.idx[*hash]
	if !ok {
		return nil, makeDbErr(database.ErrBlockNotFound,
			fmt.Sprintf("block %s not in segment %d", hash, r.segNum), nil)
	}
	if offset+numBytes > e.BlockLen || offset+numBytes < offset {
		return nil, makeDbErr(database.ErrBlockRegionInvalid,
			fmt.Sprintf("region offset %d len %d exceeds block %d",
				offset, numBytes, e.BlockLen), nil)
	}
	frame, err := r.frame(e.FrameIdx, e.FrameOff, e.FrameCompLen, e.FrameDecLen)
	if err != nil {
		return nil, err
	}
	out := make([]byte, numBytes)
	copy(out, frame[e.BlockOffInFr+offset:e.BlockOffInFr+offset+numBytes])
	return out, nil
}

// frame returns the decoded bytes of frame frameIdx, hitting the small LRU
// cache when possible.
func (r *segReader) frame(frameIdx, frameOff, compLen, decLen uint32) ([]byte, error) {
	r.frameCacheMu.Lock()
	defer r.frameCacheMu.Unlock()
	if cached, ok := r.frameCache[frameIdx]; ok {
		// Promote to MRU.
		for i, n := range r.frameCacheLR {
			if n == frameIdx {
				r.frameCacheLR = append(r.frameCacheLR[:i],
					r.frameCacheLR[i+1:]...)
				break
			}
		}
		r.frameCacheLR = append(r.frameCacheLR, frameIdx)
		return cached, nil
	}

	encoded := make([]byte, compLen)
	if _, err := r.file.ReadAt(encoded, int64(frameOff)); err != nil {
		return nil, makeDbErr(database.ErrDriverSpecific,
			fmt.Sprintf("read seg frame: %v", err), err)
	}
	dec, err := r.dec.DecodeAll(encoded, make([]byte, 0, decLen))
	if err != nil {
		return nil, makeDbErr(database.ErrCorruption,
			fmt.Sprintf("decode seg frame: %v", err), err)
	}
	if uint32(len(dec)) != decLen {
		return nil, makeDbErr(database.ErrCorruption,
			fmt.Sprintf("seg frame decoded length mismatch: got %d want %d",
				len(dec), decLen), nil)
	}

	if len(r.frameCacheLR) >= r.frameCacheN {
		victim := r.frameCacheLR[0]
		r.frameCacheLR = r.frameCacheLR[1:]
		delete(r.frameCache, victim)
	}
	r.frameCache[frameIdx] = dec
	r.frameCacheLR = append(r.frameCacheLR, frameIdx)
	return dec, nil
}

func (r *segReader) close() error {
	if r.dec != nil {
		r.dec.Close()
	}
	return r.file.Close()
}

// segPool keeps one segReader per segNum open and shared across goroutines.
type segPool struct {
	basePath string

	mu      sync.Mutex
	readers map[uint32]*segReader
}

func newSegPool(basePath string) *segPool {
	return &segPool{basePath: basePath, readers: make(map[uint32]*segReader)}
}

// reader returns the cached segReader for segNum or opens and caches a new
// one.  Concurrent callers see the same instance.
func (p *segPool) reader(segNum uint32) (*segReader, error) {
	p.mu.Lock()
	if r, ok := p.readers[segNum]; ok {
		p.mu.Unlock()
		return r, nil
	}
	p.mu.Unlock()

	r, err := openSegReader(p.basePath, segNum)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	if existing, ok := p.readers[segNum]; ok {
		// Another goroutine raced us.
		p.mu.Unlock()
		_ = r.close()
		return existing, nil
	}
	p.readers[segNum] = r
	p.mu.Unlock()
	return r, nil
}

func (p *segPool) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range p.readers {
		_ = r.close()
	}
	p.readers = nil
}

// --------------------------------------------------------------------------
// Writer side: building a new segment from a set of (hash, rawBlock) inputs.

// SegBuilder accumulates blocks and writes a single segment file pair.  Not
// safe for concurrent use.  Intended to be called from an offline tool.
type SegBuilder struct {
	segNum     uint32
	network    uint32
	dict       []byte
	maxFrameSz int

	frames []segFrameInfo
	idx    []segIndexEntry

	// Pending bytes for the in-flight frame.
	pending    []byte
	pendingMap []pendingBlockRec
}

type segFrameInfo struct {
	rawOff   uint32 // offset in the seg file where this frame's encoded bytes start
	compLen  uint32
	decLen   uint32
}

type pendingBlockRec struct {
	hash    chainhash.Hash
	blockSz uint32
}

// NewSegBuilder begins building a new segment.  maxFrameSz is the soft cap
// of decompressed bytes per frame (default 2 MiB if 0).  dict is the zstd
// training dictionary to bake into the segment.
func NewSegBuilder(segNum uint32, network uint32, dict []byte, maxFrameSz int) *SegBuilder {
	if maxFrameSz <= 0 {
		maxFrameSz = 2 * 1024 * 1024
	}
	cp := make([]byte, len(dict))
	copy(cp, dict)
	return &SegBuilder{
		segNum:     segNum,
		network:    network,
		dict:       cp,
		maxFrameSz: maxFrameSz,
	}
}

// AddBlock queues a block for inclusion in the segment.  Blocks within a
// single frame are emitted in insertion order.
func (b *SegBuilder) AddBlock(hash *chainhash.Hash, rawBlock []byte) {
	b.pendingMap = append(b.pendingMap, pendingBlockRec{
		hash: *hash, blockSz: uint32(len(rawBlock)),
	})
	b.pending = append(b.pending, rawBlock...)
}

// Finalize seals any in-flight frame and writes the .seg + .idx files under
// basePath/cold/.  Returns the resulting segment number for convenience.
func (b *SegBuilder) Finalize(basePath string) (uint32, error) {
	if err := os.MkdirAll(filepath.Join(basePath, segDirName), 0o700); err != nil {
		return 0, fmt.Errorf("mkdir cold dir: %w", err)
	}
	segPath := segFilePath(basePath, b.segNum)
	idxPath := segIndexPath(basePath, b.segNum)

	encOpts := []zstd.EOption{
		zstd.WithEncoderLevel(zstd.SpeedBetterCompression),
		zstd.WithEncoderConcurrency(1),
	}
	// Only attach a dictionary if it is non-empty AND looks like a valid
	// zstd-trained dictionary (magic 0xEC30A437).  Anything else would
	// trip zstd.NewWriter's strict dict validation.  Production callers
	// pass dictionaries built by zstd.BuildDict or `zstd --train`.
	if len(b.dict) >= 4 && b.dict[0] == 0x37 && b.dict[1] == 0xA4 &&
		b.dict[2] == 0x30 && b.dict[3] == 0xEC {
		encOpts = append(encOpts, zstd.WithEncoderDict(b.dict))
	}
	enc, err := zstd.NewWriter(nil, encOpts...)
	if err != nil {
		return 0, fmt.Errorf("zstd writer: %w", err)
	}
	defer enc.Close()

	// Plan frames: pack pending blocks into frames not exceeding maxFrameSz.
	type framePlan struct {
		startIdx int   // index into b.pendingMap
		endIdx   int   // exclusive
		raw      []byte
	}
	var plans []framePlan
	{
		cursor := 0
		startIdx := 0
		curSize := 0
		curStart := 0
		for i, rec := range b.pendingMap {
			sz := int(rec.blockSz)
			if curSize > 0 && curSize+sz > b.maxFrameSz {
				plans = append(plans, framePlan{
					startIdx: startIdx,
					endIdx:   i,
					raw:      b.pending[curStart : curStart+curSize],
				})
				startIdx = i
				curStart += curSize
				curSize = 0
			}
			curSize += sz
			cursor += sz
		}
		if curSize > 0 {
			plans = append(plans, framePlan{
				startIdx: startIdx,
				endIdx:   len(b.pendingMap),
				raw:      b.pending[curStart : curStart+curSize],
			})
		}
	}

	// Write to a temporary file, then rename.
	tmpSeg := segPath + ".tmp"
	f, err := os.OpenFile(tmpSeg, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return 0, fmt.Errorf("open tmp seg: %w", err)
	}
	defer func() {
		if f != nil {
			_ = f.Close()
		}
	}()

	// Reserve header + dict space; we'll write the header last with the
	// final frame count.  We DO know dict length up-front so we can
	// position frame writes immediately after dict.
	headerSize := int64(32 + len(b.dict))
	if _, err := f.Seek(headerSize, io.SeekStart); err != nil {
		return 0, err
	}

	curOff := uint32(headerSize)
	blockCount := uint32(0)
	for frameIdx, p := range plans {
		encoded := enc.EncodeAll(p.raw, nil)
		if _, err := f.Write(encoded); err != nil {
			return 0, fmt.Errorf("write frame %d: %w", frameIdx, err)
		}
		b.frames = append(b.frames, segFrameInfo{
			rawOff:  curOff,
			compLen: uint32(len(encoded)),
			decLen:  uint32(len(p.raw)),
		})

		// Record index entries for each block in this frame.
		var blockOffInFr uint32 = 0
		for i := p.startIdx; i < p.endIdx; i++ {
			rec := b.pendingMap[i]
			b.idx = append(b.idx, segIndexEntry{
				Hash:         rec.hash,
				FrameIdx:     uint32(frameIdx),
				BlockOffInFr: blockOffInFr,
				BlockLen:     rec.blockSz,
				FrameOff:     curOff,
				FrameCompLen: uint32(len(encoded)),
				FrameDecLen:  uint32(len(p.raw)),
			})
			blockOffInFr += rec.blockSz
			blockCount++
		}
		curOff += uint32(len(encoded))
	}

	// Write the header now that we know the frame/block counts.
	hdr := segHeader{
		Magic:      segMagic,
		Version:    segVersion,
		Network:    b.network,
		FrameCount: uint32(len(plans)),
		BlockCount: blockCount,
		DictLen:    uint32(len(b.dict)),
	}
	var raw [32]byte
	binary.LittleEndian.PutUint32(raw[0:4], hdr.Magic)
	binary.LittleEndian.PutUint32(raw[4:8], hdr.Version)
	binary.LittleEndian.PutUint32(raw[8:12], hdr.Network)
	binary.LittleEndian.PutUint32(raw[12:16], hdr.FrameCount)
	binary.LittleEndian.PutUint32(raw[16:20], hdr.BlockCount)
	binary.LittleEndian.PutUint32(raw[20:24], hdr.DictLen)
	binary.LittleEndian.PutUint32(raw[24:28], 0)
	hdr.HeaderCRC = crc32.Checksum(raw[:28], castagnoli)
	binary.LittleEndian.PutUint32(raw[28:32], hdr.HeaderCRC)

	if _, err := f.WriteAt(raw[:], 0); err != nil {
		return 0, fmt.Errorf("write header: %w", err)
	}
	if len(b.dict) > 0 {
		if _, err := f.WriteAt(b.dict, 32); err != nil {
			return 0, fmt.Errorf("write dict: %w", err)
		}
	}

	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("sync seg: %w", err)
	}
	if err := f.Close(); err != nil {
		return 0, fmt.Errorf("close seg: %w", err)
	}
	f = nil
	if err := os.Rename(tmpSeg, segPath); err != nil {
		return 0, fmt.Errorf("rename seg: %w", err)
	}

	// Write the index file.
	tmpIdx := idxPath + ".tmp"
	idxF, err := os.OpenFile(tmpIdx, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return 0, fmt.Errorf("open tmp idx: %w", err)
	}
	w := make([]byte, segIndexEntrySize)
	for _, e := range b.idx {
		copy(w[0:32], e.Hash[:])
		binary.LittleEndian.PutUint32(w[32:36], e.FrameIdx)
		binary.LittleEndian.PutUint32(w[36:40], e.BlockOffInFr)
		binary.LittleEndian.PutUint32(w[40:44], e.BlockLen)
		binary.LittleEndian.PutUint32(w[44:48], 0)
		binary.LittleEndian.PutUint32(w[48:52], e.FrameOff)
		binary.LittleEndian.PutUint32(w[52:56], e.FrameCompLen)
		binary.LittleEndian.PutUint32(w[56:60], e.FrameDecLen)
		if _, err := idxF.Write(w); err != nil {
			return 0, fmt.Errorf("write idx: %w", err)
		}
	}
	if err := idxF.Sync(); err != nil {
		return 0, fmt.Errorf("sync idx: %w", err)
	}
	if err := idxF.Close(); err != nil {
		return 0, fmt.Errorf("close idx: %w", err)
	}
	if err := os.Rename(tmpIdx, idxPath); err != nil {
		return 0, fmt.Errorf("rename idx: %w", err)
	}
	return b.segNum, nil
}

// scanColdSegs returns the list of segment numbers present in <db>/cold/.
// Used by reconcile to know which segments need to be openable on startup.
func scanColdSegs(basePath string) ([]uint32, error) {
	dir := filepath.Join(basePath, segDirName)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var nums []uint32
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) < len(segExtension)+1 ||
			name[len(name)-len(segExtension):] != segExtension {
			continue
		}
		var n uint32
		if _, err := fmt.Sscanf(name, "%09d"+segExtension, &n); err != nil {
			continue
		}
		nums = append(nums, n)
	}
	return nums, nil
}
