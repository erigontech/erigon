// Copyright 2022 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package commitment

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/state/stateifs"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/rlp"
)

type DomainPutter = stateifs.DomainPutter

type CommitmentWrite = stateifs.CommitmentWrite

type CollapseTracer func(hashedKeyPath, branchPrefix []byte)

type witnessTracer interface {
	onNode(rlp, hash []byte)
}

// With a nil tracer every method is a no-op.
type witness struct {
	tracer          witnessTracer
	leafBuf         bytes.Buffer
	branchBuf       bytes.Buffer
	leafWriterCache io.Writer
}

func (w *witness) active() bool { return w.tracer != nil }

// Buffers keep their capacity across pooled reuse; reset only detaches the tracer.
func (w *witness) reset() { w.tracer = nil }

func (w *witness) leafWriter(keccak io.Writer) io.Writer {
	if w.tracer == nil {
		return keccak
	}
	w.leafBuf.Reset()
	if w.leafWriterCache == nil {
		w.leafWriterCache = io.MultiWriter(keccak, &w.leafBuf)
	}
	return w.leafWriterCache
}

func (w *witness) emitLeaf(hash []byte) {
	if w.tracer != nil {
		w.tracer.onNode(w.leafBuf.Bytes(), hash)
	}
}

func (w *witness) beginBranch(prefix []byte) {
	if w.tracer != nil {
		w.branchBuf.Reset()
		w.branchBuf.Write(prefix)
	}
}

func (w *witness) writeBranch(b []byte) { w.branchBuf.Write(b) }

func (w *witness) emitBranch(hash []byte) {
	if w.tracer != nil {
		w.tracer.onNode(w.branchBuf.Bytes(), hash)
	}
}

type HexPatriciaHashed struct {
	root          cell
	activeRows    int
	currentKeyLen int16
	accountKeyLen int16
	grid          [128][16]cell // rows 0-63 account trie, 64-127 storage trie
	currentKey    [128]byte
	depths        [128]int16
	branchBefore  [128]bool
	// touchMap: cell present before modification, or modified, or deleted. afterMap: cell present after. Not symmetric.
	touchMap      [128]uint16
	afterMap      [128]uint16
	keccak        keccak.KeccakState
	keccak2       keccak.KeccakState
	rootChecked   bool
	rootTouched   bool
	rootPresent   bool
	traceW        io.Writer
	ctx           PatriciaContext
	hashAuxBuffer [128]byte
	cellHashBuf   common.Hash
	leafHashBuf   [33]byte
	leafRlpBuf    [maxLeafRlpLen]byte
	rlpPrefixBuf  [8]byte
	auxBuffer     *bytes.Buffer
	branchEncoder *BranchEncoder

	mounted    bool
	mountedNib int   // -1 means it's a storage subtrie, must not be folded above depth 63
	mountWall  int16 // depth the mounted subtree folds down to; foldMounted stops here

	memoizationOff bool
	accValBuf      rlp.RlpEncodedBytes

	leaveDeferredForCaller bool
	collapseTracer         CollapseTracer

	witness witness

	cfg TrieConfig

	metrics       *Metrics
	depthsToTxNum [129]uint64

	// lastUpdateCellWasEmpty means the key is absent from the pre-state trie.
	lastUpdateCellWasEmpty bool
	hadToLoadL             map[uint64]skipStat
}

func (hph *HexPatriciaHashed) SpawnSubTrie(ctx PatriciaContext, forNibble int) *HexPatriciaHashed {
	subCfg := hph.cfg.Subtrie()
	subTrie := NewHexPatriciaHashed(hph.accountKeyLen, ctx, subCfg)

	subTrie.mountTo(hph, forNibble)
	return subTrie
}

var hphPool sync.Pool

func NewHexPatriciaHashed(accountKeyLen int16, ctx PatriciaContext, cfg TrieConfig) *HexPatriciaHashed {
	hph, ok := hphPool.Get().(*HexPatriciaHashed)
	if !ok {
		hph = newHexPatriciaHashed()
	}
	hph.accountKeyLen = accountKeyLen
	hph.ctx = ctx
	hph.applyConfig(cfg)
	return hph
}

func (hph *HexPatriciaHashed) applyConfig(cfg TrieConfig) {
	hph.cfg = cfg
	hph.branchEncoder.setDeferUpdates(cfg.DeferBranchUpdates)
	hph.branchEncoder.maxDeferredUpdates = DefaultMaxDeferredUpdates
	hph.leaveDeferredForCaller = cfg.LeaveDeferredForCaller
	hph.memoizationOff = cfg.MemoizationOff
	hph.metrics.SetCsvMetrics(cfg.CsvMetricsFilePrefix)
}

func newHexPatriciaHashed() *HexPatriciaHashed {
	hph := &HexPatriciaHashed{
		keccak:        keccak.NewFastKeccak(),
		keccak2:       keccak.NewFastKeccak(),
		auxBuffer:     bytes.NewBuffer(make([]byte, 8192)),
		hadToLoadL:    make(map[uint64]skipStat),
		accValBuf:     make(rlp.RlpEncodedBytes, 128),
		metrics:       NewMetrics(""),
		branchEncoder: NewBranchEncoder(1024),
	}

	hph.branchEncoder.setMetrics(hph.metrics)
	return hph
}

func (hph *HexPatriciaHashed) SetCollapseTracer(tracer CollapseTracer) {
	hph.collapseTracer = tracer
}

// The grid array is NOT zeroed; activeRows=0 means no cells are live.
func (hph *HexPatriciaHashed) resetForReuse() {
	hph.root.reset()
	hph.rootTouched = false
	hph.rootChecked = false
	hph.rootPresent = false
	hph.currentKeyLen = 0
	hph.activeRows = 0
	for i := range hph.depths {
		hph.depths[i] = 0
		hph.branchBefore[i] = false
		hph.touchMap[i] = 0
		hph.afterMap[i] = 0
	}

	hph.ctx = nil

	clear(hph.hadToLoadL)

	hph.mounted = false
	hph.mountedNib = 0
	hph.mountWall = 0

	hph.traceW = nil
	hph.collapseTracer = nil
	hph.witness.reset()

	hph.memoizationOff = false
	hph.leaveDeferredForCaller = false

	hph.auxBuffer.Reset()

	hph.branchEncoder.ClearDeferred()
	hph.branchEncoder.buf.Reset()
	hph.branchEncoder.setDeferUpdates(false)

	hph.cfg = TrieConfig{}

	clear(hph.depthsToTxNum[:])
}

func (hph *HexPatriciaHashed) Release() {
	hph.resetForReuse()
	hphPool.Put(hph)
}

type cell struct {
	hashedExtension [128]byte
	extension       [64]byte
	accountAddr     common.Address
	storageAddr     [length.Addr + length.Hash]byte
	hash            common.Hash
	stateHash       common.Hash
	hashedExtLen    int16
	extLen          int16
	accountAddrLen  int16
	storageAddrLen  int16
	hashLen         int16
	stateHashLen    int16
	loaded          loadFlags
	Update
}

type loadFlags uint8

const (
	cellLoadNone    = loadFlags(0)
	cellLoadAccount = loadFlags(1)
	cellLoadStorage = loadFlags(2)
)

// Sized off the nibble array, not today's shorter depth-derived keys, so a caller slicing deeper cannot overflow.
const maxLeafRlpLen = 9 + 1 + (len(cell{}.hashedExtension)/2 + 1)

func (f loadFlags) String() string {
	var b strings.Builder
	if f == cellLoadNone {
		b.WriteString("false")
	} else {
		if f.account() {
			b.WriteString("Account ")
		}
		if f.storage() {
			b.WriteString("Storage ")
		}
	}
	return b.String()
}

func (f loadFlags) account() bool {
	return f&cellLoadAccount != 0
}

func (f loadFlags) storage() bool {
	return f&cellLoadStorage != 0
}

func (f loadFlags) addFlag(loadFlags loadFlags) loadFlags {
	if loadFlags == cellLoadNone {
		return f
	}
	return f | loadFlags
}

var (
	emptyRootHashBytes = empty.RootHash[:]
	// Package-level: a local copy would heap-allocate on every hashed row (keccak2.Write is an interface call).
	emptyBranchSlotBytes = []byte{rlp.EmptyStringCode}
)

func traceHex(b []byte) string { return hex.EncodeToString(b) }

func (cell *cell) hashAccKey(keccak keccak.KeccakState, depth int16, hashBuf []byte) error {
	return hashKey(keccak, cell.accountAddr[:cell.accountAddrLen], cell.hashedExtension[:], depth, hashBuf)
}

func (cell *cell) hashStorageKey(keccak keccak.KeccakState, accountKeyLen, downOffset int16, hashedKeyOffset int16, hashBuf []byte) error {
	return hashKey(keccak, cell.storageAddr[accountKeyLen:cell.storageAddrLen], cell.hashedExtension[downOffset:], hashedKeyOffset, hashBuf)
}

func (cell *cell) reset() {
	cell.accountAddrLen = 0
	cell.storageAddrLen = 0
	cell.hashedExtLen = 0
	cell.extLen = 0
	cell.hashLen = 0
	cell.stateHashLen = 0
	cell.loaded = cellLoadNone
	clear(cell.hashedExtension[:])
	clear(cell.extension[:])
	clear(cell.accountAddr[:])
	clear(cell.storageAddr[:])
	clear(cell.hash[:])
	cell.Update.Reset()
}

func (cell *cell) FullString() string {
	b := new(strings.Builder)
	b.WriteString("{")
	fmt.Fprintf(b, "loaded=%v", cell.loaded)
	if cell.Deleted() {
		b.WriteString(" DELETED ")
	}

	if cell.accountAddrLen > 0 {
		fmt.Fprintf(b, " addr=%x", cell.accountAddr[:cell.accountAddrLen])
		fmt.Fprintf(b, " balance=%s", cell.Balance.String())
		fmt.Fprintf(b, " nonce=%d", cell.Nonce)
		if cell.CodeHash != empty.CodeHash {
			fmt.Fprintf(b, " codeHash=%x", cell.CodeHash[:])
		} else {
			b.WriteString(" codeHash=EMPTY")
		}
	}
	if cell.storageAddrLen > 0 {
		fmt.Fprintf(b, " addr[s]=%x", cell.storageAddr[:cell.storageAddrLen])
		fmt.Fprintf(b, " storage=%x", cell.Storage[:cell.StorageLen])
	}
	if cell.hashLen > 0 {
		fmt.Fprintf(b, " h=%x", cell.hash[:cell.hashLen])
	}
	if cell.stateHashLen > 0 {
		fmt.Fprintf(b, " memHash=%x", cell.stateHash[:cell.stateHashLen])
	}
	if cell.extLen > 0 {
		fmt.Fprintf(b, " extension=%x", cell.extension[:cell.extLen])
	}
	if cell.hashedExtLen > 0 {
		fmt.Fprintf(b, " hashedExtension=%x", cell.hashedExtension[:cell.hashedExtLen])
	}

	b.WriteString("}")
	return b.String()
}

func (cell *cell) setFromUpdate(update *Update) {
	cell.Update.Merge(update)
	if update.Flags&StorageUpdate != 0 {
		cell.loaded = cell.loaded.addFlag(cellLoadStorage)
		hadToLoad.Add(1)
	}
	if update.Flags&BalanceUpdate != 0 || update.Flags&NonceUpdate != 0 || update.Flags&CodeUpdate != 0 {
		cell.loaded = cell.loaded.addFlag(cellLoadAccount)
		hadToLoad.Add(1)
	}
}

func (cell *cell) fillFromUpperCell(upCell *cell, depth, depthIncrement int16) {
	if upCell.hashedExtLen >= depthIncrement {
		cell.hashedExtLen = upCell.hashedExtLen - depthIncrement
	} else {
		cell.hashedExtLen = 0
	}
	if upCell.hashedExtLen > depthIncrement {
		copy(cell.hashedExtension[:], upCell.hashedExtension[depthIncrement:upCell.hashedExtLen])
	}
	if upCell.extLen >= depthIncrement {
		cell.extLen = upCell.extLen - depthIncrement
	} else {
		cell.extLen = 0
	}
	if upCell.extLen > depthIncrement {
		copy(cell.extension[:], upCell.extension[depthIncrement:upCell.extLen])
	}
	if depth <= 64 {
		cell.accountAddrLen = upCell.accountAddrLen
		if upCell.accountAddrLen > 0 {
			copy(cell.accountAddr[:], upCell.accountAddr[:cell.accountAddrLen])
			cell.Balance.Set(&upCell.Balance)
			cell.Nonce = upCell.Nonce
			cell.CodeHash = upCell.CodeHash
			cell.extLen = upCell.extLen
			if upCell.extLen > 0 {
				copy(cell.extension[:], upCell.extension[:upCell.extLen])
			}
		}
	} else {
		cell.accountAddrLen = 0
	}
	cell.storageAddrLen = upCell.storageAddrLen
	if upCell.storageAddrLen > 0 {
		copy(cell.storageAddr[:], upCell.storageAddr[:upCell.storageAddrLen])
		cell.StorageLen = upCell.StorageLen
		if upCell.StorageLen > 0 {
			copy(cell.Storage[:], upCell.Storage[:upCell.StorageLen])
		}
	}
	cell.hashLen = upCell.hashLen
	if upCell.hashLen > 0 {
		copy(cell.hash[:], upCell.hash[:upCell.hashLen])
	}
	cell.loaded = upCell.loaded
}

func (cell *cell) fillFromLowerCell(lowCell *cell, lowDepth int16, preExtension []byte, nibble int) {
	if lowCell.accountAddrLen > 0 || lowDepth < 64 {
		cell.accountAddrLen = lowCell.accountAddrLen
	}
	if lowCell.accountAddrLen > 0 {
		copy(cell.accountAddr[:], lowCell.accountAddr[:cell.accountAddrLen])
		cell.Balance.Set(&lowCell.Balance)
		cell.Nonce = lowCell.Nonce
		cell.CodeHash = lowCell.CodeHash
	}
	cell.storageAddrLen = lowCell.storageAddrLen
	if lowCell.storageAddrLen > 0 {
		copy(cell.storageAddr[:], lowCell.storageAddr[:cell.storageAddrLen])
		cell.StorageLen = lowCell.StorageLen
		if lowCell.StorageLen > 0 {
			copy(cell.Storage[:], lowCell.Storage[:lowCell.StorageLen])
		}
	}
	if lowCell.hashLen > 0 {
		if (lowCell.accountAddrLen == 0 && lowDepth < 64) || (lowCell.storageAddrLen == 0 && lowDepth > 64) {
			if len(preExtension) > 0 {
				copy(cell.extension[:], preExtension)
			}
			cell.extension[len(preExtension)] = byte(nibble)
			if lowCell.extLen > 0 {
				copy(cell.extension[1+len(preExtension):], lowCell.extension[:lowCell.extLen])
			}
			cell.extLen = lowCell.extLen + 1 + int16(len(preExtension))
			if cell.accountAddrLen == 0 && cell.storageAddrLen == 0 {
				copy(cell.hashedExtension[:], cell.extension[:cell.extLen])
				cell.hashedExtLen = cell.extLen
			}
		} else {
			cell.extLen = lowCell.extLen
			if lowCell.extLen > 0 {
				copy(cell.extension[:], lowCell.extension[:lowCell.extLen])
			}
		}
	}
	cell.hashLen = lowCell.hashLen
	if lowCell.hashLen > 0 {
		copy(cell.hash[:], lowCell.hash[:lowCell.hashLen])
	}
	if lowDepth > 64 {
		cell.loaded = cell.loaded.addFlag(lowCell.loaded)
	} else {
		cell.loaded = lowCell.loaded
	}
}

func (cell *cell) deriveHashedKeys(depth int16, keccak keccak.KeccakState, accountKeyLen int16, hashBuf []byte) error {
	extraLen := int16(0)
	if cell.accountAddrLen > 0 {
		if depth > 64 {
			return errors.New("deriveHashedKeys accountAddr present at depth > 64")
		}
		extraLen = 64 - depth
	}
	if cell.storageAddrLen > 0 {
		if depth >= 64 {
			extraLen = 128 - depth
		} else {
			extraLen += 64
		}
	}
	if extraLen > 0 {
		if cell.hashedExtLen > 0 {
			copy(cell.hashedExtension[extraLen:], cell.hashedExtension[:cell.hashedExtLen])
		}
		cell.hashedExtLen = min(extraLen+cell.hashedExtLen, int16(len(cell.hashedExtension)))
		var hashedKeyOffset, downOffset int16
		if cell.accountAddrLen > 0 {
			if err := cell.hashAccKey(keccak, depth, hashBuf); err != nil {
				return err
			}
			downOffset = 64 - depth
		}
		if cell.storageAddrLen > 0 {
			if depth >= 64 {
				hashedKeyOffset = depth - 64
			}
			if depth == 0 {
				accountKeyLen = 0
			}
			if err := cell.hashStorageKey(keccak, accountKeyLen, downOffset, hashedKeyOffset, hashBuf); err != nil {
				return err
			}
		}
	}
	return nil
}

func (cell *cell) fillFromFields(data []byte, pos int, fieldBits cellFields) (int, error) {
	fields := []struct {
		flag      cellFields
		lenField  *int16
		dataField []byte
		extraFunc func(int16)
	}{
		{fieldExtension, &cell.hashedExtLen, cell.hashedExtension[:], func(l int16) {
			cell.extLen = l
			if l > 0 {
				copy(cell.extension[:], cell.hashedExtension[:l])
			}
		}},
		{fieldAccountAddr, &cell.accountAddrLen, cell.accountAddr[:], nil},
		{fieldStorageAddr, &cell.storageAddrLen, cell.storageAddr[:], nil},
		{fieldHash, &cell.hashLen, cell.hash[:], nil},
		{fieldStateHash, &cell.stateHashLen, cell.stateHash[:], nil},
	}

	for _, f := range fields {
		if fieldBits&f.flag != 0 {
			l, n, err := readUvarint(data[pos:])
			if err != nil {
				return 0, err
			}
			pos += n

			if len(data) < pos+int(l) {
				return 0, fmt.Errorf("buffer too small for %v", f.flag)
			}

			*f.lenField = int16(l)
			if l > 0 {
				copy(f.dataField, data[pos:pos+int(l)])
				pos += int(l)
			}
			if f.extraFunc != nil {
				f.extraFunc(int16(l))
			}
		} else {
			*f.lenField = 0
			if f.flag == fieldExtension {
				cell.extLen = 0
			}
		}
	}

	if fieldBits&fieldAccountAddr != 0 {
		cell.CodeHash = empty.CodeHash
	}
	return pos, nil
}

func readUvarint(data []byte) (uint64, int, error) {
	l, n := binary.Uvarint(data)
	if n == 0 {
		return 0, 0, errors.New("buffer too small for length")
	} else if n < 0 {
		return 0, 0, errors.New("value overflow for length")
	}
	return l, n, nil
}

func skipCellFields(data []byte, pos int, fieldBits byte) int {
	for bit := byte(1); bit <= 16; bit <<= 1 {
		if fieldBits&bit != 0 {
			if pos >= len(data) {
				return pos
			}
			l, n := binary.Uvarint(data[pos:])
			if n <= 0 {
				return pos
			}
			pos += n + int(l)
		}
	}
	return pos
}

func (cell *cell) accountForHashing(buffer []byte, storageRootHash common.Hash) int {
	balanceBytes := 0
	if !cell.Balance.LtUint64(128) {
		balanceBytes = cell.Balance.ByteLen()
	}

	var nonceBytes int
	if cell.Nonce < 128 && cell.Nonce != 0 {
		nonceBytes = 0
	} else {
		nonceBytes = common.BitLenToByteLen(bits.Len64(cell.Nonce))
	}

	var structLength = uint(balanceBytes + nonceBytes + 2)
	structLength += 66

	var pos int
	if structLength < 56 {
		buffer[0] = byte(192 + structLength)
		pos = 1
	} else {
		lengthBytes := common.BitLenToByteLen(bits.Len(structLength))
		buffer[0] = byte(247 + lengthBytes)

		for i := lengthBytes; i > 0; i-- {
			buffer[i] = byte(structLength)
			structLength >>= 8
		}

		pos = lengthBytes + 1
	}

	if cell.Nonce < 128 && cell.Nonce != 0 {
		buffer[pos] = byte(cell.Nonce)
	} else {
		buffer[pos] = byte(128 + nonceBytes)
		var nonce = cell.Nonce
		for i := nonceBytes; i > 0; i-- {
			buffer[pos+i] = byte(nonce)
			nonce >>= 8
		}
	}
	pos += 1 + nonceBytes

	if cell.Balance.LtUint64(128) && !cell.Balance.IsZero() {
		buffer[pos] = byte(cell.Balance.Uint64())
		pos++
	} else {
		buffer[pos] = byte(128 + balanceBytes)
		pos++
		cell.Balance.WriteToSlice(buffer[pos : pos+balanceBytes])
		pos += balanceBytes
	}

	buffer[pos] = 128 + 32
	pos++
	copy(buffer[pos:], storageRootHash[:])
	pos += 32
	buffer[pos] = 128 + 32
	pos++
	copy(buffer[pos:], cell.CodeHash[:])
	pos += 32
	return pos
}

func completeLeafHash[V rlp.RlpSerializable](hph *HexPatriciaHashed, buf []byte, compactLen int, key []byte, compact0 byte, ni int, val V, singleton bool) ([]byte, error) {
	var kp, kl int
	if compactLen > 1 {
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}

	totalLen := kp + kl + val.DoubleRLPLen()
	// Assembled into one scratch buffer: per-write stack arrays would heap-allocate through the io.Writer interface.
	header := hph.leafRlpBuf[:]
	pl := rlp.EncodeListPrefixToBuf(totalLen, header)
	n := pl
	if kp > 0 {
		header[n] = 0x80 + byte(compactLen)
		n++
	}
	header[n] = compact0
	n++
	for i := 1; i < compactLen; i++ {
		header[n] = key[ni]*16 + key[ni+1]
		n++
		ni += 2
	}

	canEmbed := !singleton && totalLen+pl < length.Hash
	var writer io.Writer
	if canEmbed {
		hph.auxBuffer.Reset()
		writer = hph.auxBuffer
	} else {
		hph.keccak.Reset()
		writer = hph.witness.leafWriter(hph.keccak)
	}
	if _, err := writer.Write(header[:n]); err != nil {
		return nil, err
	}
	if err := val.ToDoubleRLP(writer, hph.rlpPrefixBuf[:]); err != nil {
		return nil, err
	}
	if canEmbed {
		buf = hph.auxBuffer.Bytes()
	} else {
		hph.leafHashBuf[0] = 0x80 + length.Hash
		if _, err := hph.keccak.Read(hph.leafHashBuf[1:]); err != nil {
			return nil, err
		}
		buf = append(buf, hph.leafHashBuf[:]...)
		hph.witness.emitLeaf(hph.leafHashBuf[1:])
	}
	return buf, nil
}

func (hph *HexPatriciaHashed) leafHashWithKeyVal(buf, key []byte, val rlp.RlpSerializableBytes, singleton bool) ([]byte, error) {
	var compactLen int
	var ni int
	var compact0 byte
	compactLen = (len(key)-1)/2 + 1
	if len(key)&1 == 0 {
		compact0 = 0x30 + key[0]
		ni = 1
	} else {
		compact0 = 0x20
	}
	return completeLeafHash(hph, buf, compactLen, key, compact0, ni, val, singleton)
}

func (hph *HexPatriciaHashed) accountLeafHashWithKey(buf, key []byte, val rlp.RlpEncodedBytes) ([]byte, error) {
	var compactLen int
	var ni int
	var compact0 byte
	if nibbles.HasTerm(key) {
		compactLen = (len(key)-1)/2 + 1
		if len(key)&1 == 0 {
			compact0 = 48 + key[0]
			ni = 1
		} else {
			compact0 = 32
		}
	} else {
		compactLen = len(key)/2 + 1
		if len(key)&1 == 1 {
			compact0 = terminatorHexByte + key[0]
			ni = 1
		}
	}
	return completeLeafHash(hph, buf, compactLen, key, compact0, ni, val, true)
}

func (hph *HexPatriciaHashed) extensionHash(key []byte, hash []byte) (common.Hash, error) {
	var hashBuf common.Hash

	var kp, kl int
	var compactLen int
	var ni int
	var compact0 byte
	if nibbles.HasTerm(key) {
		compactLen = (len(key)-1)/2 + 1
		if len(key)&1 == 0 {
			compact0 = 0x30 + key[0]
			ni = 1
		} else {
			compact0 = 0x20
		}
	} else {
		compactLen = len(key)/2 + 1
		if len(key)&1 == 1 {
			compact0 = 0x10 + key[0]
			ni = 1
		}
	}
	var keyPrefix [1]byte
	if compactLen > 1 {
		keyPrefix[0] = 0x80 + byte(compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}
	totalLen := kp + kl + 33
	var lenPrefix [4]byte
	pt := rlp.EncodeListPrefixToBuf(totalLen, lenPrefix[:])

	hph.keccak.Reset()
	w := hph.witness.leafWriter(hph.keccak)
	if _, err := w.Write(lenPrefix[:pt]); err != nil {
		return hashBuf, err
	}
	if _, err := w.Write(keyPrefix[:kp]); err != nil {
		return hashBuf, err
	}
	var b [1]byte
	b[0] = compact0
	if _, err := w.Write(b[:]); err != nil {
		return hashBuf, err
	}
	for i := 1; i < compactLen; i++ {
		b[0] = key[ni]*16 + key[ni+1]
		if _, err := w.Write(b[:]); err != nil {
			return hashBuf, err
		}
		ni += 2
	}
	b[0] = 0x80 + length.Hash
	if _, err := w.Write(b[:]); err != nil {
		return hashBuf, err
	}
	if _, err := w.Write(hash); err != nil {
		return hashBuf, err
	}
	if _, err := hph.keccak.Read(hashBuf[:]); err != nil {
		return hashBuf, err
	}
	hph.witness.emitLeaf(hashBuf[:])
	return hashBuf, nil
}

func (hph *HexPatriciaHashed) computeCellHashLen(cell *cell, depth int16) int16 {
	if cell.storageAddrLen > 0 && depth >= 64 {
		if cell.stateHashLen > 0 {
			return cell.stateHashLen + 1
		}

		keyLen := 128 - depth + 1
		var kp, kl int
		compactLen := (keyLen-1)/2 + 1
		if compactLen > 1 {
			kp = 1
			kl = int(compactLen)
		} else {
			kl = 1
		}
		val := rlp.RlpSerializableBytes(cell.Storage[:cell.StorageLen])
		totalLen := kp + kl + val.DoubleRLPLen()
		var lenPrefix [4]byte
		pt := rlp.EncodeListPrefixToBuf(totalLen, lenPrefix[:])
		if totalLen+pt < length.Hash {
			return int16(totalLen + pt)
		}
	}
	return length.Hash + 1
}

func (hph *HexPatriciaHashed) witnessComputeCellHashWithStorage(cell *cell, depth int16, buf []byte) ([]byte, bool, []byte, error) {
	var err error
	var storageRootHash common.Hash
	var storageRootHashIsSet bool
	if hph.memoizationOff {
		cell.stateHashLen = 0
	}

	// Temporary buffer avoids corrupting cell.hashedExtension, still needed by later witness operations.
	var hashedKeyBuf [128]byte

	if cell.storageAddrLen > 0 {
		var hashedKeyOffset int16
		if depth >= 64 {
			hashedKeyOffset = depth - 64
		}
		singleton := depth <= 64
		koffset := hph.accountKeyLen
		if depth == 0 && cell.accountAddrLen == 0 {
			koffset = 0
		}
		if err := hashKey(hph.keccak, cell.storageAddr[koffset:cell.storageAddrLen], hashedKeyBuf[:], hashedKeyOffset, hph.cellHashBuf[:]); err != nil {
			return nil, storageRootHashIsSet, nil, err
		}
		hashedKeyBuf[64-hashedKeyOffset] = terminatorHexByte

		if cell.stateHashLen > 0 {
			res := append([]byte{160}, cell.stateHash[:cell.stateHashLen]...)
			hph.keccak.Reset()
			if hph.traceW != nil {
				fmt.Fprintf(hph.traceW, "REUSED stateHash %x spk %x\n", res, cell.storageAddr[:cell.storageAddrLen])
			}
			skippedLoad.Add(1)
			if !singleton {
				return res, storageRootHashIsSet, nil, err
			} else {
				storageRootHashIsSet = true
				storageRootHash = *(*common.Hash)(res[1:])
			}
		} else {
			if !cell.loaded.storage() {
				hph.metrics.StorageLoad(cell.storageAddr[:cell.storageAddrLen])
				update, err := hph.storageFromCacheOrDB(cell.storageAddr[:cell.storageAddrLen])
				if err != nil {
					return nil, storageRootHashIsSet, nil, err
				}
				cell.setFromUpdate(update)
				if hph.traceW != nil {
					fmt.Fprintf(hph.traceW, "Storage %x was not loaded\n", cell.storageAddr[:cell.storageAddrLen])
				}
			}
			if singleton {
				if hph.traceW != nil {
					fmt.Fprintf(hph.traceW, "leafHashWithKeyVal(singleton) for [%s]=>[%x]\n", traceHex(hashedKeyBuf[:64-hashedKeyOffset+1]), cell.Storage[:cell.StorageLen])
				}
				aux := hph.hashAuxBuffer[:0]
				if aux, err = hph.leafHashWithKeyVal(aux, hashedKeyBuf[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen], true); err != nil {
					return nil, storageRootHashIsSet, nil, err
				}
				if hph.traceW != nil {
					fmt.Fprintf(hph.traceW, "leafHashWithKeyVal(singleton) storage hash [%x]\n", aux)
				}
				storageRootHash = *(*common.Hash)(aux[1:])
				storageRootHashIsSet = true
				cell.stateHashLen = 0
				hadToReset.Add(1)
			} else {
				if hph.traceW != nil {
					fmt.Fprintf(hph.traceW, "leafHashWithKeyVal for [%s]=>[%x] %v\n", traceHex(hashedKeyBuf[:64-hashedKeyOffset+1]), cell.Storage[:cell.StorageLen], cell.String())
				}
				leafHash, err := hph.leafHashWithKeyVal(buf, hashedKeyBuf[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen], false)
				if err != nil {
					return nil, storageRootHashIsSet, nil, err
				}

				copy(cell.stateHash[:], leafHash[1:])
				cell.stateHashLen = int16(len(leafHash) - 1)
				if hph.traceW != nil {
					fmt.Fprintf(hph.traceW, "STATE HASH storage memoized %x spk %x\n", leafHash, cell.storageAddr[:cell.storageAddrLen])
				}

				return leafHash, storageRootHashIsSet, storageRootHash[:], nil
			}
		}
	}
	if cell.accountAddrLen > 0 {
		if err := hashKey(hph.keccak, cell.accountAddr[:cell.accountAddrLen], hashedKeyBuf[:], depth, hph.cellHashBuf[:]); err != nil {
			return nil, storageRootHashIsSet, nil, err
		}
		hashedKeyBuf[64-depth] = terminatorHexByte
		if !storageRootHashIsSet {
			switch {
			case cell.extLen > 0:
				if cell.hashLen == 0 {
					return nil, storageRootHashIsSet, nil, errors.New("computeCellHash extension without hash")
				}
				if hph.traceW != nil {
					fmt.Fprintf(hph.traceW, "extensionHash for [%x]=>[%x]\n", cell.extension[:cell.extLen], cell.hash[:cell.hashLen])
				}
				if storageRootHash, err = hph.extensionHash(cell.extension[:cell.extLen], cell.hash[:cell.hashLen]); err != nil {
					return nil, storageRootHashIsSet, nil, err
				}
				if hph.traceW != nil {
					fmt.Fprintf(hph.traceW, "EXTENSION HASH %x DROPS stateHash\n", storageRootHash)
				}
				cell.stateHashLen = 0
				hadToReset.Add(1)
				storageRootHashIsSet = true
			case cell.hashLen > 0:
				storageRootHash = cell.hash
				storageRootHashIsSet = true
			default:
				storageRootHash = empty.RootHash
			}
		}
		if !cell.loaded.account() {
			if cell.stateHashLen > 0 {
				res := append([]byte{160}, cell.stateHash[:cell.stateHashLen]...)
				hph.keccak.Reset()

				skippedLoad.Add(1)
				if hph.traceW != nil {
					fmt.Fprintf(hph.traceW, "REUSED stateHash %x apk %x\n", res, cell.accountAddr[:cell.accountAddrLen])
				}
				return res, storageRootHashIsSet, storageRootHash[:], nil
			}
			hph.metrics.AccountLoad(cell.accountAddr[:cell.accountAddrLen])
			update, err := hph.accountFromCacheOrDB(cell.accountAddr[:cell.accountAddrLen])
			if err != nil {
				return nil, storageRootHashIsSet, storageRootHash[:], err
			}
			cell.setFromUpdate(update)
		}

		var valBuf [128]byte
		valLen := cell.accountForHashing(valBuf[:], storageRootHash)
		if hph.traceW != nil {
			fmt.Fprintf(hph.traceW, "accountLeafHashWithKey for [%s]=>[%s]\n", traceHex(hashedKeyBuf[:65-depth]), traceHex(valBuf[:valLen]))
		}
		leafHash, err := hph.accountLeafHashWithKey(buf, hashedKeyBuf[:65-depth], rlp.RlpEncodedBytes(valBuf[:valLen]))
		if err != nil {
			return nil, storageRootHashIsSet, nil, err
		}
		if hph.traceW != nil {
			fmt.Fprintf(hph.traceW, "STATE HASH account memoized %x\n", leafHash)
		}
		copy(cell.stateHash[:], leafHash[1:])
		cell.stateHashLen = int16(len(leafHash) - 1)
		return leafHash, storageRootHashIsSet, storageRootHash[:], nil
	}

	buf = append(buf, 0x80+32)
	switch {
	case cell.extLen > 0:
		if cell.hashLen > 0 {
			if hph.traceW != nil {
				fmt.Fprintf(hph.traceW, "extensionHash for [%x]=>[%x]\n", cell.extension[:cell.extLen], cell.hash[:cell.hashLen])
			}
			var hash common.Hash
			if hash, err = hph.extensionHash(cell.extension[:cell.extLen], cell.hash[:cell.hashLen]); err != nil {
				return nil, storageRootHashIsSet, storageRootHash[:], err
			}
			buf = append(buf, hash[:]...)
		} else {
			return nil, storageRootHashIsSet, storageRootHash[:], errors.New("computeCellHash extension without hash")
		}
	case cell.hashLen > 0:
		buf = append(buf, cell.hash[:cell.hashLen]...)
	case storageRootHashIsSet:
		buf = append(buf, storageRootHash[:]...)
		copy(cell.hash[:], storageRootHash[:])
		cell.hashLen = int16(len(storageRootHash))
	default:
		buf = append(buf, emptyRootHashBytes...)
	}
	return buf, storageRootHashIsSet, storageRootHash[:], nil
}

func (hph *HexPatriciaHashed) computeCellHash(cell *cell, depth int16, buf []byte) ([]byte, error) {
	var err error
	var storageRootHash common.Hash
	var storageRootHashIsSet bool
	if hph.memoizationOff {
		cell.stateHashLen = 0
	}
	if cell.storageAddrLen > 0 {
		var hashedKeyOffset int16
		if depth >= 64 {
			hashedKeyOffset = depth - 64
		}
		singleton := depth <= 64

		if cell.stateHashLen > 0 {
			if hph.traceW != nil {
				fmt.Fprintf(hph.traceW, "REUSED stateHash %x spk %x\n", cell.stateHash[:cell.stateHashLen], cell.storageAddr[:cell.storageAddrLen])
			}
			skippedLoad.Add(1)
			if !singleton {
				return append(append(buf[:0], byte(160)), cell.stateHash[:cell.stateHashLen]...), nil
			}
			storageRootHashIsSet = true
			storageRootHash = *(*common.Hash)(cell.stateHash[:cell.stateHashLen])
		} else {
			koffset := hph.accountKeyLen
			if depth == 0 && cell.accountAddrLen == 0 {
				koffset = 0
			}
			if err := cell.hashStorageKey(hph.keccak, koffset, 0, hashedKeyOffset, hph.cellHashBuf[:]); err != nil {
				return nil, err
			}
			cell.hashedExtension[64-hashedKeyOffset] = terminatorHexByte
			if !cell.loaded.storage() {
				return nil, fmt.Errorf("storage %x was not loaded as expected: cell %v", cell.storageAddr[:cell.storageAddrLen], cell.String())
			}

			leafHash, err := hph.leafHashWithKeyVal(buf, cell.hashedExtension[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen], singleton)
			if err != nil {
				return nil, err
			}
			if hph.traceW != nil {
				fmt.Fprintf(hph.traceW, "leafHashWithKeyVal(singleton=%t) {%x} for [%x]=>[%x] %v\n",
					singleton, leafHash, cell.hashedExtension[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen], cell.String())
			}
			if !singleton {
				copy(cell.stateHash[:], leafHash[1:])
				cell.stateHashLen = int16(len(leafHash) - 1)
				return leafHash, nil
			}
			storageRootHash = *(*common.Hash)(leafHash[1:])
			storageRootHashIsSet = true
			cell.stateHashLen = 0
			hadToReset.Add(1)
		}
	}
	if cell.accountAddrLen > 0 {
		if err := cell.hashAccKey(hph.keccak, depth, hph.cellHashBuf[:]); err != nil {
			return nil, err
		}
		cell.hashedExtension[64-depth] = terminatorHexByte
		if !storageRootHashIsSet {
			switch {
			case cell.extLen > 0:
				if cell.hashLen == 0 {
					return nil, errors.New("computeCellHash extension without hash")
				}
				if hph.traceW != nil {
					fmt.Fprintf(hph.traceW, "extensionHash for [%x]=>[%x]\n", cell.extension[:cell.extLen], cell.hash[:cell.hashLen])
				}
				if storageRootHash, err = hph.extensionHash(cell.extension[:cell.extLen], cell.hash[:cell.hashLen]); err != nil {
					return nil, err
				}
				if hph.traceW != nil {
					fmt.Fprintf(hph.traceW, "EXTENSION HASH %x DROPS stateHash\n", storageRootHash)
				}
				cell.stateHashLen = 0
				hadToReset.Add(1)
			case cell.hashLen > 0:
				storageRootHash = cell.hash
			default:
				storageRootHash = empty.RootHash
			}
		}
		if !cell.loaded.account() {
			if cell.stateHashLen > 0 {
				hph.keccak.Reset()

				skippedLoad.Add(1)
				if hph.traceW != nil {
					fmt.Fprintf(hph.traceW, "REUSED stateHash %x apk %x\n", cell.stateHash[:cell.stateHashLen], cell.accountAddr[:cell.accountAddrLen])
				}
				return append(append(buf[:0], byte(160)), cell.stateHash[:cell.stateHashLen]...), nil
			}
			hph.metrics.AccountLoad(cell.accountAddr[:cell.accountAddrLen])
			update, err := hph.accountFromCacheOrDB(cell.accountAddr[:cell.accountAddrLen])
			if err != nil {
				return nil, err
			}
			cell.setFromUpdate(update)
		}

		valLen := cell.accountForHashing(hph.accValBuf, storageRootHash)
		buf, err = hph.accountLeafHashWithKey(buf, cell.hashedExtension[:65-depth], hph.accValBuf[:valLen])
		if err != nil {
			return nil, err
		}
		if hph.traceW != nil {
			fmt.Fprintf(hph.traceW, "accountLeafHashWithKey {%x} (memorised) for [%x]=>[%x]\n", buf, cell.hashedExtension[:65-depth], hph.accValBuf[:valLen])
		}
		copy(cell.stateHash[:], buf[1:])
		cell.stateHashLen = int16(len(buf)) - 1
		return buf, nil
	}

	buf = append(buf, 0x80+32)
	switch {
	case cell.extLen > 0:
		if cell.hashLen > 0 {
			if hph.traceW != nil {
				fmt.Fprintf(hph.traceW, "extensionHash for [%x]=>[%x]\n", cell.extension[:cell.extLen], cell.hash[:cell.hashLen])
			}
			if storageRootHash, err = hph.extensionHash(cell.extension[:cell.extLen], cell.hash[:cell.hashLen]); err != nil {
				return nil, err
			}
			buf = append(buf, storageRootHash[:]...)
		} else {
			return nil, errors.New("computeCellHash extension without hash")
		}
	case cell.hashLen > 0:
		buf = append(buf, cell.hash[:cell.hashLen]...)
	case storageRootHashIsSet:
		buf = append(buf, storageRootHash[:]...)
		copy(cell.hash[:], storageRootHash[:])
		cell.hashLen = int16(len(storageRootHash))
	default:
		buf = append(buf, emptyRootHashBytes...)
	}
	return buf, nil
}

// Stops a descent from above the 64-nibble account boundary exactly at it, where storage subtrees start.
func clampToAccountBoundary(depth, length int16) int16 {
	if depth < 64 && depth+length > 64 {
		return 64 - depth
	}
	return length
}

func (hph *HexPatriciaHashed) needUnfolding(hashedKey []byte) int16 {
	var cell *cell
	var depth int16
	if hph.activeRows == 0 {
		if hph.traceW != nil {
			fmt.Fprintf(hph.traceW, "needUnfolding root, rootChecked = %t\n", hph.rootChecked)
		}
		if hph.root.hashedExtLen == 64 && hph.root.accountAddrLen > 0 && hph.root.storageAddrLen > 0 {
			if err := hph.root.deriveHashedKeys(depth, hph.keccak, hph.accountKeyLen, hph.cellHashBuf[:]); err != nil {
				log.Warn("deriveHashedKeys for root with storage", "err", err, "cell", hph.root.FullString())
				return 0
			}
			if hph.traceW != nil {
				fmt.Fprintf(hph.traceW, "derived prefix %x\n", hph.currentKey[:hph.currentKeyLen])
			}
		}
		if hph.root.hashedExtLen == 0 && hph.root.hashLen == 0 {
			if hph.rootChecked {
				return 0
			}
			return 1
		}
		cell = &hph.root
	} else {
		depth = hph.depths[hph.activeRows-1]
		// Guard before indexing: a probe key shorter than the row's depth (an unfold can
		// consume several extension nibbles at once) needs no further unfolding.
		if int16(len(hashedKey)) <= depth {
			return 0
		}
		nibble := int(hashedKey[hph.currentKeyLen])
		cell = &hph.grid[hph.activeRows-1][nibble]
		if hph.traceW != nil {
			fmt.Fprintf(hph.traceW, "currentKey [%x] needUnfolding cell (%d, %x, depth=%d) cell.hash=[%x]\n", hph.currentKey[:hph.currentKeyLen], hph.activeRows-1, nibble, depth, cell.hash[:cell.hashLen])
		}
	}
	if int16(len(hashedKey)) <= depth {
		return 0
	}
	if cell.hashedExtLen == 0 {
		if cell.hashLen == 0 {
			return 0
		}
		return 1
	}

	cpl := nibbles.CommonPrefixLen(hashedKey[depth:], cell.hashedExtension[:cell.hashedExtLen-1])
	if hph.traceW != nil {
		fmt.Fprintf(hph.traceW, "cpl=%d cell.hashedExtension=[%x] hashedKey[depth=%d:]=[%s]\n", cpl, cell.hashedExtension[:cell.hashedExtLen], depth, traceHex(hashedKey[depth:]))
	}
	unfolding := clampToAccountBoundary(depth, int16(cpl+1))
	if hph.traceW != nil && unfolding != int16(cpl+1) {
		fmt.Fprintf(hph.traceW, "adjusted unfolding=%d <- %d\n", unfolding, cpl+1)
	}
	return unfolding
}

func (c *cell) IsEmpty() bool {
	return c == nil || (c.hashLen == 0 && c.hashedExtLen == 0 && c.extLen == 0 && c.accountAddrLen == 0 && c.storageAddrLen == 0)
}

func (c *cell) String() string {
	var s strings.Builder
	s.WriteString("(")
	if c.hashLen > 0 {
		s.WriteString(fmt.Sprintf("hash(len=%d)=%x, ", c.hashLen, c.hash))
	}
	if c.hashedExtLen > 0 {
		s.WriteString(fmt.Sprintf("hashedExtension(len=%d)=%x, ", c.hashedExtLen, c.hashedExtension[:c.hashedExtLen]))
	}
	if c.extLen > 0 {
		s.WriteString(fmt.Sprintf("extension(len=%d)=%x, ", c.extLen, c.extension[:c.extLen]))
	}
	if c.accountAddrLen > 0 {
		s.WriteString(fmt.Sprintf("accountAddr=%x, ", c.accountAddr))
	}
	if c.storageAddrLen > 0 {
		s.WriteString(fmt.Sprintf("storageAddr=%x, ", c.storageAddr))
	}

	s.WriteString(")")
	return s.String()
}

func (hph *HexPatriciaHashed) PrintGrid() {
	fmt.Printf("GRID:\n")
	for row := 0; row < hph.activeRows; row++ {
		fmt.Printf("row %d depth %d:\n", row, hph.depths[row])
		for col := range 16 {
			cell := &hph.grid[row][col]
			if cell.hashedExtLen > 0 || cell.accountAddrLen > 0 {
				var cellHash []byte
				cellHash, _, _, err := hph.witnessComputeCellHashWithStorage(cell, hph.depths[row], nil)
				if err != nil {
					panic("failed to compute cell hash")
				}
				fmt.Printf("\t %x: %v cellHash=%x, \n", col, cell, cellHash)
			} else {
				fmt.Printf("\t %x: %v , \n", col, cell)
			}
		}
		fmt.Printf("\n")
	}
	fmt.Printf("\n")
}

func (hph *HexPatriciaHashed) witnessMaterializeBranch(branchPrefix []byte, childDepth int16) (*trie.FullNode, error) {
	compact := nibbles.HexToCompact(branchPrefix)
	branchData, err := hph.readBranchAndCheckForFlushing(compact)
	if err != nil {
		return nil, err
	}
	if len(branchData) < 2 {
		return nil, fmt.Errorf("[witness] empty branch data at prefix %x", branchPrefix)
	}
	branchData = branchData[2:]
	bitmap := binary.BigEndian.Uint16(branchData[0:])
	pos := 2
	fullNode := &trie.FullNode{}
	for bitset := bitmap; bitset != 0; {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		var c cell
		fieldBits := branchData[pos]
		pos++
		if pos, err = c.fillFromFields(branchData, pos, cellFields(fieldBits)); err != nil {
			return nil, fmt.Errorf("[witness] fillFromFields at prefix %x: %w", branchPrefix, err)
		}
		if childDepth > 64 {
			c.accountAddrLen = 0
		}
		if err := c.deriveHashedKeys(childDepth, hph.keccak, hph.accountKeyLen, hph.cellHashBuf[:]); err != nil {
			return nil, err
		}
		cellHash, _, _, err := hph.witnessComputeCellHashWithStorage(&c, childDepth, nil)
		if err != nil {
			return nil, err
		}
		if len(cellHash) == length.Hash+1 { // strip the 0xa0 prefix
			cellHash = cellHash[1:]
		}
		fullNode.Children[nibble] = trie.NewHashNode(bytes.Clone(cellHash))
		bitset ^= bit
	}
	return fullNode, nil
}

func (hph *HexPatriciaHashed) witnessMaterializeBranchChild(branchPrefix []byte, childDepth int16, wantHash []byte) (*trie.FullNode, error) {
	branchNode, err := hph.witnessMaterializeBranch(branchPrefix, childDepth)
	if err != nil {
		return nil, err
	}
	subRoot := trie.NewInMemoryTrie(branchNode).Hash()
	if !bytes.Equal(subRoot[:], wantHash) {
		return nil, fmt.Errorf("[witness] materialized branch root mismatch at prefix %x: got %x want %x", branchPrefix, subRoot, wantHash)
	}
	return branchNode, nil
}

// Flushes deferred updates first if the prefix is pending, so a modified-but-unwritten prefix reads fresh.
func (hph *HexPatriciaHashed) readBranchAndCheckForFlushing(prefix []byte) ([]byte, error) {
	be := hph.branchEncoder
	if be.DeferUpdatesEnabled() && be.HasPendingPrefix(prefix) {
		if err := be.ApplyDeferredUpdates(16, hph.ctx.PutBranch); err != nil {
			return nil, err
		}
		be.ClearDeferred()
	}
	return hph.branchFromCacheOrDB(prefix)
}

func (hph *HexPatriciaHashed) unfoldBranchNode(row int, depth int16, deleted bool) error {
	key := nibbles.HexToCompact(hph.currentKey[:hph.currentKeyLen])
	hph.metrics.BranchLoad(hph.currentKey[:hph.currentKeyLen])

	branchData, err := hph.readBranchAndCheckForFlushing(key)
	if err != nil {
		return err
	}

	hph.depthsToTxNum[depth] = 0

	if len(branchData) >= 2 {
		branchData = branchData[2:]
	}
	if hph.traceW != nil {
		fmt.Fprintf(hph.traceW, "unfoldBranchNode prefix '%x', nibbles [%x] depth %d row %d '%x'\n",
			key, hph.currentKey[:hph.currentKeyLen], depth, row, branchData)
	}
	if !hph.rootChecked && hph.currentKeyLen == 0 && len(branchData) == 0 {
		hph.rootChecked = true
		return nil
	}
	if len(branchData) == 0 {
		log.Warn("got empty branch data during unfold", "key", traceHex(key), "row", row, "depth", depth, "deleted", deleted)
		if hph.traceW != nil {
			branchData, _ = hph.branchFromCacheOrDB(key)
			fmt.Fprintf(hph.traceW, "unfoldBranchNode prefix '%x', nibbles [%x] depth %d row %d '%x' %s\n", key, hph.currentKey[:hph.currentKeyLen], depth, row, branchData, BranchData(branchData).String())
		}
		return fmt.Errorf("empty branch data read during unfold, compact prefix %x nibbles %x", key, hph.currentKey[:hph.currentKeyLen])
	}
	hph.branchBefore[row] = true
	if err := hph.decodeBranchIntoRow(row, depth, branchData, deleted); err != nil {
		return fmt.Errorf("prefix [%x] branchData[%x]: %w", hph.currentKey[:hph.currentKeyLen], branchData, err)
	}
	hph.depths[hph.activeRows] = depth
	hph.activeRows++
	return nil
}

func (hph *HexPatriciaHashed) decodeBranchIntoRow(row int, depth int16, branch []byte, deleted bool) error {
	maps, err := DecodeBranchInto(branch, deleted, &hph.grid[row])
	if err != nil {
		return err
	}
	hph.touchMap[row] = maps.TouchMap
	hph.afterMap[row] = maps.AfterMap
	for bitset := maps.Bitmap; bitset != 0; {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		cell := &hph.grid[row][nibble]
		if hph.traceW != nil {
			fmt.Fprintf(hph.traceW, "cell (%d, %x, depth=%d) %s\n", row, nibble, depth, cell.FullString())
		}
		if err := cell.deriveHashedKeys(depth, hph.keccak, hph.accountKeyLen, hph.cellHashBuf[:]); err != nil {
			return err
		}
		bitset ^= bit
	}
	return nil
}

func (hph *HexPatriciaHashed) unfold(hashedKey []byte, unfolding int16) error {
	if hph.traceW != nil {
		fmt.Fprintf(hph.traceW, "unfold %d: activeRows: %d\n", unfolding, hph.activeRows)
	}
	var upCell *cell
	var touched, present bool
	var upDepth, depth int16
	if hph.activeRows == 0 {
		if hph.rootChecked && hph.root.hashLen == 0 && hph.root.hashedExtLen == 0 {
			return nil
		}
		upCell = &hph.root
		touched = hph.rootTouched
		present = hph.rootPresent
		if hph.traceW != nil {
			fmt.Fprintf(hph.traceW, "unfold root: touched: %t present: %t %s\n", touched, present, upCell.FullString())
		}
	} else {
		upRow := hph.activeRows - 1
		upDepth = hph.depths[upRow]
		upNibble := hashedKey[upDepth-1]
		upCell = &hph.grid[upRow][upNibble]

		touched = hph.touchMap[upRow]&(uint16(1)<<upNibble) != 0
		present = hph.afterMap[upRow]&(uint16(1)<<upNibble) != 0
		if hph.traceW != nil {
			fmt.Fprintf(hph.traceW, "upCell (%d, %x, updepth=%d) touched: %t present: %t\n", upRow, upNibble, upDepth, touched, present)
		}
		hph.currentKey[hph.currentKeyLen] = upNibble
		hph.currentKeyLen++
	}
	row := hph.activeRows
	for i := range 16 {
		hph.grid[row][i].reset()
	}
	hph.touchMap[row], hph.afterMap[row], hph.branchBefore[row] = 0, 0, false

	if upCell.hashedExtLen == 0 {
		depth = upDepth + 1
		return hph.unfoldBranchNode(row, depth, touched && !present)
	}

	lowest := min(unfolding, upCell.hashedExtLen)
	depth = upDepth + lowest
	copyLen := lowest - 1
	nibble := upCell.hashedExtension[copyLen]

	if touched {
		hph.touchMap[row] = uint16(1) << nibble
	}
	if present {
		hph.afterMap[row] = uint16(1) << nibble
	}

	cell := &hph.grid[row][nibble]
	cell.fillFromUpperCell(upCell, depth, lowest)
	if hph.traceW != nil {
		fmt.Fprintf(hph.traceW, "unfolded cell (%d, %x, depth=%d) %s\n", row, nibble, depth, cell.FullString())
	}
	if row >= 64 {
		cell.accountAddrLen = 0
	}

	if copyLen > 0 {
		copy(hph.currentKey[hph.currentKeyLen:], upCell.hashedExtension[:copyLen])
		hph.currentKeyLen += copyLen
	}

	hph.depths[hph.activeRows] = depth
	hph.activeRows++
	return nil
}

func (hph *HexPatriciaHashed) needFolding(hashedKey []byte) bool {
	return !bytes.HasPrefix(hashedKey, hph.currentKey[:hph.currentKeyLen])
}

var (
	hadToLoad   atomic.Uint64
	skippedLoad atomic.Uint64
	hadToReset  atomic.Uint64
)

var (
	rateFlushMu       sync.Mutex
	loadRatePublished uint64
	skipRatePublished uint64
)

// Delta-based publish of the monotonic atomics, so the emitted counter value is idempotent per call.
func flushTrieStateRates() {
	rateFlushMu.Lock()
	defer rateFlushMu.Unlock()
	if l := hadToLoad.Load(); l > loadRatePublished {
		mxTrieStateLoadRate.AddUint64(l - loadRatePublished)
		loadRatePublished = l
	}
	if s := skippedLoad.Load(); s > skipRatePublished {
		mxTrieStateSkipRate.AddUint64(s - skipRatePublished)
		skipRatePublished = s
	}
}

type skipStat struct {
	accLoaded, accSkipped, accReset, storReset, storLoaded, storSkipped uint64
}

const terminatorHexByte = 16 // max nibble value +1. Defines end of nibble line in the trie or splits address and storage space in trie.

type updateKind uint8

const (
	updateKindDelete updateKind = 0b0

	// A single surviving cell is not stored as a branch; it's fused into the parent branch instead.
	updateKindPropagate updateKind = 0b01

	updateKindBranch updateKind = 0b10
)

func afterMapUpdateKind(afterMap uint16) (kind updateKind, nibblesAfterUpdate int) {
	nibblesAfterUpdate = bits.OnesCount16(afterMap)
	switch nibblesAfterUpdate {
	case 0:
		return updateKindDelete, nibblesAfterUpdate
	case 1:
		return updateKindPropagate, nibblesAfterUpdate
	default:
		return updateKindBranch, nibblesAfterUpdate
	}
}

func (hph *HexPatriciaHashed) foldBranch(row int, nibble, upDepth, depth int16, upCell *cell, updateKey []byte) error {
	if hph.touchMap[row] != 0 {
		if row == 0 {
			hph.rootTouched = true
			hph.rootPresent = true
		} else {
			hph.touchMap[row-1] |= uint16(1) << nibble
		}
	}
	bitmap := hph.touchMap[row] & hph.afterMap[row]
	if !hph.branchBefore[row] {
		hph.touchMap[row] |= hph.afterMap[row]
		bitmap |= hph.afterMap[row]
	}

	nibblesLeftAfterUpdate := bits.OnesCount16(hph.afterMap[row])
	totalBranchLen, err := hph.prepareBranchCells(row, depth, nibblesLeftAfterUpdate)
	if err != nil {
		return err
	}

	hph.keccak2.Reset()
	pt := rlp.EncodeListPrefixToBuf(int(totalBranchLen), hph.hashAuxBuffer[:])
	if _, err := hph.keccak2.Write(hph.hashAuxBuffer[:pt]); err != nil {
		return err
	}
	hph.witness.beginBranch(hph.hashAuxBuffer[:pt])

	cellData, err := hph.hashRow(row, depth)
	if err != nil {
		return err
	}

	if hph.branchEncoder.DeferUpdatesEnabled() {
		if err := hph.branchEncoder.CollectDeferredUpdate(hph.ctx, updateKey, bitmap, hph.touchMap[row], hph.afterMap[row], &cellData, !hph.branchBefore[row]); err != nil {
			return fmt.Errorf("failed to collect deferred branch update: %w", err)
		}
	} else {
		if err := hph.branchEncoder.CollectUpdate(hph.ctx, updateKey, bitmap, hph.touchMap[row], hph.afterMap[row], &cellData, !hph.branchBefore[row]); err != nil {
			return fmt.Errorf("failed to encode branch update: %w", err)
		}
	}
	upCell.extLen = depth - upDepth - 1
	upCell.hashedExtLen = upCell.extLen
	if upCell.extLen > 0 {
		copy(upCell.extension[:], hph.currentKey[upDepth:hph.currentKeyLen])
		copy(upCell.hashedExtension[:], hph.currentKey[upDepth:hph.currentKeyLen])
	}
	if depth < 64 {
		upCell.accountAddrLen = 0
	}
	upCell.storageAddrLen = 0
	upCell.hashLen = 32
	if _, err := hph.keccak2.Read(upCell.hash[:]); err != nil {
		return err
	}
	hph.witness.emitBranch(upCell.hash[:])
	if hph.traceW != nil {
		fmt.Fprintf(hph.traceW, "} [%x]\n", upCell.hash[:])
	}
	return nil
}

func (hph *HexPatriciaHashed) hashRow(row int, depth int16) ([16]cellEncodeData, error) {
	var cellData [16]cellEncodeData
	capture := hph.witness.active()

	for bitset, lastNib := hph.afterMap[row], 0; ; {
		if bitset == 0 {
			for i := lastNib; i < 17; i++ {
				if _, err := hph.keccak2.Write(emptyBranchSlotBytes); err != nil {
					return cellData, err
				}
				if capture {
					hph.witness.writeBranch(emptyBranchSlotBytes)
				}
			}
			break
		}
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)

		for i := lastNib; i < nibble; i++ {
			if _, err := hph.keccak2.Write(emptyBranchSlotBytes); err != nil {
				return cellData, err
			}
			if capture {
				hph.witness.writeBranch(emptyBranchSlotBytes)
			}
			if hph.traceW != nil {
				fmt.Fprintf(hph.traceW, "  %x: empty(%d, %x, depth=%d)\n", i, row, i, depth)
			}
		}
		lastNib = nibble + 1

		cell := &hph.grid[row][nibble]

		if cell.accountAddrLen > 0 && cell.stateHashLen == 0 && !cell.loaded.account() && !cell.Deleted() {
			log.Warn("account not loaded", "row", row, "nibble", fmt.Sprintf("%x", nibble), "depth", depth, "cell", cell.String())
		}
		if cell.storageAddrLen > 0 && cell.stateHashLen == 0 && !cell.loaded.storage() && !cell.Deleted() {
			log.Warn("storage not loaded", "row", row, "nibble", fmt.Sprintf("%x", nibble), "depth", depth, "cell", cell.String())
		}

		var hashBefore []byte
		if dbg.KVReadLevelledMetrics && (cell.accountAddrLen > 0 || cell.storageAddrLen > 0) {
			hashBefore = make([]byte, cell.stateHashLen)
			copy(hashBefore, cell.stateHash[:cell.stateHashLen])
		}
		loadedBefore := cell.loaded

		cellHash, err := hph.computeCellHash(cell, depth, hph.hashAuxBuffer[:0])
		if err != nil {
			return cellData, err
		}
		if hph.traceW != nil {
			fmt.Fprintf(hph.traceW, "  %x: computeCellHash(%d, %x, depth=%d)=[%x]\n", nibble, row, nibble, depth, cellHash)
		}

		if dbg.KVReadLevelledMetrics && hashBefore != nil {
			counters := hph.hadToLoadL[hph.depthsToTxNum[depth]]
			if !bytes.Equal(hashBefore, cell.stateHash[:cell.stateHashLen]) {
				if cell.accountAddrLen > 0 {
					counters.accReset++
					counters.accLoaded++
				}
				if cell.storageAddrLen > 0 {
					counters.storReset++
					counters.storLoaded++
				}
			} else {
				if cell.accountAddrLen > 0 && !loadedBefore.account() && !cell.loaded.account() {
					counters.accSkipped++
				}
				if cell.storageAddrLen > 0 && !loadedBefore.storage() && !cell.loaded.storage() {
					counters.storSkipped++
				}
			}
			hph.hadToLoadL[hph.depthsToTxNum[depth]] = counters
		}

		if _, err := hph.keccak2.Write(cellHash); err != nil {
			return cellData, err
		}
		if capture {
			hph.witness.writeBranch(cellHash)
		}

		cellData[nibble] = cellEncodeDataFromCell(cell)

		bitset ^= bit
	}
	return cellData, nil
}

func (hph *HexPatriciaHashed) prepareBranchCells(row int, depth int16, nibblesLeftAfterUpdate int) (int16, error) {
	totalBranchLen := int16(17 - nibblesLeftAfterUpdate)
	for bitset, j := hph.afterMap[row], 0; bitset != 0; j++ {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		cell := &hph.grid[row][nibble]

		if hph.memoizationOff {
			cell.stateHashLen = 0
		}
		var counters skipStat
		if dbg.KVReadLevelledMetrics {
			counters = hph.hadToLoadL[hph.depthsToTxNum[depth]]
		}
		if cell.stateHashLen > 0 && (hph.touchMap[row]&hph.afterMap[row]&uint16(1<<nibble) > 0 || cell.stateHashLen != length.Hash) {
			if hph.traceW != nil {
				fmt.Fprintf(hph.traceW, "DROP hash for (%d, %x, depth=%d) %s\n", row, nibble, depth, cell.FullString())
			}
			cell.stateHashLen = 0
			hadToReset.Add(1)
			if cell.accountAddrLen > 0 {
				counters.accReset++
			}
			if cell.storageAddrLen > 0 {
				counters.storReset++
			}
		}
		var err error
		counters, err = hph.loadStateIfNeeded(cell, counters)
		if err != nil {
			return 0, err
		}
		if dbg.KVReadLevelledMetrics {
			hph.hadToLoadL[hph.depthsToTxNum[depth]] = counters
		}

		totalBranchLen += hph.computeCellHashLen(cell, depth)
		bitset ^= bit
	}
	return totalBranchLen, nil
}

func (hph *HexPatriciaHashed) foldPropagate(row int, nibble, upDepth, depth int16, upCell *cell, updateKey []byte) error {
	if hph.touchMap[row] != 0 {
		if row == 0 {
			hph.rootTouched = true
			// A propagate fold leaves exactly one survivor, so the root exists; without this
			// the next unfold reads touched && !present and deletes the whole subtree.
			hph.rootPresent = true
		} else {
			hph.touchMap[row-1] |= uint16(1) << nibble
		}
	}
	childNibble := bits.TrailingZeros16(hph.afterMap[row])
	cell := &hph.grid[row][childNibble]
	upCell.extLen = 0
	upCell.stateHashLen = 0
	var counters skipStat
	if dbg.KVReadLevelledMetrics {
		counters = hph.hadToLoadL[hph.depthsToTxNum[depth]]
	}
	counters, err := hph.loadStateIfNeeded(cell, counters)
	if err != nil {
		return err
	}
	if dbg.KVReadLevelledMetrics {
		hph.hadToLoadL[hph.depthsToTxNum[depth]] = counters
	}
	upCell.fillFromLowerCell(cell, depth, hph.currentKey[upDepth:hph.currentKeyLen], childNibble)

	if err := hph.collectDeleteUpdate(updateKey, row); err != nil {
		return err
	}
	if hph.traceW != nil {
		fmt.Fprintf(hph.traceW, "formed leaf (%d %x, depth=%d) [%x] %s\n", row, childNibble, depth, updateKey, cell.FullString())
	}
	return nil
}

func (hph *HexPatriciaHashed) foldDelete(row int, nibble, upDepth int16, upCell *cell, updateKey []byte) error {
	if hph.touchMap[row] != 0 {
		switch {
		case row == 0:
			hph.rootTouched = true
			hph.rootPresent = false
		case upDepth == 64:
			// All storage of an account was deleted, but that doesn't delete the account itself;
			// turn it into a modification instead of propagating the deletion upward.
			hph.touchMap[row-1] |= uint16(1) << nibble
		default:
			hph.touchMap[row-1] |= uint16(1) << nibble
			hph.afterMap[row-1] &^= uint16(1) << nibble
			if hph.collapseTracer != nil && bits.OnesCount16(hph.afterMap[row-1]) == 1 {
				hph.detectCascadingCollapseAtRow(row - 1)
			}
		}
	}

	upCell.reset()
	return hph.collectDeleteUpdate(updateKey, row)
}

func (hph *HexPatriciaHashed) collectDeleteUpdate(updateKey []byte, row int) error {
	if hph.branchBefore[row] {
		if err := hph.branchEncoder.CollectUpdate(hph.ctx, updateKey, 0, hph.touchMap[row], 0, nil, false); err != nil {
			return fmt.Errorf("failed to encode branch deletion: %w", err)
		}
	}
	return nil
}

// Reduces hph.currentKey; invoke until needFolding returns false for the next key.
func (hph *HexPatriciaHashed) fold() error {
	updateKeyLen := hph.currentKeyLen
	if hph.activeRows == 0 {
		return errors.New("cannot fold - no active rows")
	}
	if hph.traceW != nil {
		fmt.Fprintf(hph.traceW, "fold [%x] activeRows: %d touchMap: %016b afterMap: %016b\n", hph.currentKey[:hph.currentKeyLen], hph.activeRows, hph.touchMap[hph.activeRows-1], hph.afterMap[hph.activeRows-1])
	}
	var upCell *cell
	var nibble, upDepth int16
	row := hph.activeRows - 1
	upRow := row - 1
	if row == 0 {
		if hph.traceW != nil {
			fmt.Fprintf(hph.traceW, "fold: parent is root %s\n", hph.root.FullString())
		}
		upCell = &hph.root
	} else {
		upDepth = hph.depths[upRow]
		nibble = int16(hph.currentKey[upDepth-1])
		if hph.traceW != nil {
			fmt.Fprintf(hph.traceW, "fold: parent (%d, %x, depth=%d)\n", upRow, nibble, upDepth)
		}
		upCell = &hph.grid[upRow][nibble]
	}

	depth := hph.depths[row]

	updateKey := nibbles.HexToCompact(hph.currentKey[:updateKeyLen])
	defer func() { hph.depthsToTxNum[depth] = 0 }()

	if hph.traceW != nil {
		fmt.Fprintf(hph.traceW, "fold: (row=%d, {%s}, depth=%d) prefix [%x] touchMap: %016b afterMap: %016b \n",
			row, updatedNibs(hph.touchMap[row]&hph.afterMap[row]), depth, hph.currentKey[:hph.currentKeyLen], hph.touchMap[row], hph.afterMap[row])
	}

	updateKind, _ := afterMapUpdateKind(hph.afterMap[row])
	var err error
	switch updateKind {
	case updateKindDelete:
		err = hph.foldDelete(row, nibble, upDepth, upCell, updateKey)
	case updateKindPropagate:
		err = hph.foldPropagate(row, nibble, upDepth, depth, upCell, updateKey)
	case updateKindBranch:
		err = hph.foldBranch(row, nibble, upDepth, depth, upCell, updateKey)
	}
	if err != nil {
		return err
	}

	hph.activeRows--
	hph.currentKeyLen = max(upDepth-1, 0)
	return nil
}

func (hph *HexPatriciaHashed) loadStateIfNeeded(cell *cell, counters skipStat) (skipStat, error) {
	if cell.stateHashLen == 0 {
		if !cell.loaded.account() && cell.accountAddrLen > 0 {
			hph.metrics.AccountLoad(cell.accountAddr[:cell.accountAddrLen])
			upd, err := hph.accountFromCacheOrDB(cell.accountAddr[:cell.accountAddrLen])
			if err != nil {
				return counters, err
			}
			cell.setFromUpdate(upd)
			cell.loaded = cell.loaded.addFlag(cellLoadAccount)
			counters.accLoaded++
		}
		if !cell.loaded.storage() && cell.storageAddrLen > 0 {
			hph.metrics.StorageLoad(cell.storageAddr[:cell.storageAddrLen])
			upd, err := hph.storageFromCacheOrDB(cell.storageAddr[:cell.storageAddrLen])
			if err != nil {
				return counters, err
			}
			cell.setFromUpdate(upd)
			cell.loaded = cell.loaded.addFlag(cellLoadStorage)
			counters.storLoaded++
		}
	}
	return counters, nil
}

func (hph *HexPatriciaHashed) deleteCell(hashedKey []byte) {
	if hph.traceW != nil {
		fmt.Fprintf(hph.traceW, "deleteCell, activeRows = %d\n", hph.activeRows)
	}
	var cell *cell
	if hph.activeRows == 0 {
		cell = &hph.root
		hph.rootTouched, hph.rootPresent = true, false
	} else {
		row := hph.activeRows - 1
		if hph.depths[row] < int16(len(hashedKey)) {
			if hph.traceW != nil {
				fmt.Fprintf(hph.traceW, "deleteCell skipping spurious delete depth=%d, len(hashedKey)=%d\n", hph.depths[row], len(hashedKey))
			}
			return
		}
		nibble := int(hashedKey[hph.currentKeyLen])
		cell = &hph.grid[row][nibble]
		col := uint16(1) << nibble
		if hph.afterMap[row]&col != 0 {
			hph.touchMap[row] |= col
			hph.afterMap[row] &^= col
			if hph.traceW != nil {
				fmt.Fprintf(hph.traceW, "deleteCell setting (%d, %x)\n", row, nibble)
			}
		} else if hph.traceW != nil {
			fmt.Fprintf(hph.traceW, "deleteCell ignoring (%d, %x)\n", row, nibble)
		}
	}
	cell.reset()
}

// If the row above has exactly 2 non-empty cells, deleting collapses it to the surviving sibling.
func (hph *HexPatriciaHashed) detectCollapseBeforeDelete(hashedKey []byte) {
	if hph.activeRows < 2 {
		return
	}
	parentRow := hph.activeRows - 2
	children := bits.OnesCount16(hph.afterMap[parentRow])
	if children != 2 {
		return
	}

	if hph.traceW != nil {
		fmt.Fprintf(hph.traceW, "[collapse] updateCell: hashedKey=%s (len=%d nibbles), deleted=true, activeRows=%d\n",
			NibblesToString(hashedKey), len(hashedKey), hph.activeRows)
	}

	depth := hph.depths[parentRow] - 1
	deleteNibble := int(hashedKey[depth])

	siblingNibble := -1
	for i := range 16 {
		if hph.afterMap[parentRow]&(1<<i) != 0 && i != deleteNibble {
			siblingNibble = i
			break
		}
	}
	if siblingNibble < 0 {
		return
	}

	siblingCell := &hph.grid[parentRow][siblingNibble]

	siblingPath := make([]byte, int(depth)+1+int(siblingCell.hashedExtLen))
	copy(siblingPath, hph.currentKey[:depth])
	siblingPath[depth] = byte(siblingNibble)
	if siblingCell.hashedExtLen > 0 {
		copy(siblingPath[int(depth)+1:], siblingCell.hashedExtension[:siblingCell.hashedExtLen])
	}

	if hph.traceW != nil {
		fmt.Fprintf(hph.traceW, "[collapse] found at parentRow=%d depth=%d: deleteNibble=%x, siblingNibble=%x, siblingPath=%s (len=%d), hashLen=%d, extLen=%d\n",
			parentRow, depth, deleteNibble, siblingNibble, NibblesToString(siblingPath), len(siblingPath), siblingCell.hashLen, siblingCell.hashedExtLen)
	}
	hph.collapseTracer(siblingPath, bytes.Clone(hph.currentKey[:depth]))
}

// Called when a fold() clearing a nibble leaves afterMap[row] with exactly 1 remaining child.
func (hph *HexPatriciaHashed) detectCascadingCollapseAtRow(row int) {
	depth := hph.depths[row] - 1
	survivingNibble := bits.TrailingZeros16(hph.afterMap[row])
	survivingCell := &hph.grid[row][survivingNibble]

	siblingPath := make([]byte, int(depth)+1+int(survivingCell.hashedExtLen))
	copy(siblingPath, hph.currentKey[:depth])
	siblingPath[depth] = byte(survivingNibble)
	if survivingCell.hashedExtLen > 0 {
		copy(siblingPath[int(depth)+1:], survivingCell.hashedExtension[:survivingCell.hashedExtLen])
	}

	if hph.traceW != nil {
		fmt.Fprintf(hph.traceW, "[cascade-collapse] found at row=%d depth=%d: survivingNibble=%x, siblingPath=%s (len=%d), hashLen=%d, hashedExtLen=%d\n",
			row, depth, survivingNibble, NibblesToString(siblingPath), len(siblingPath), survivingCell.hashLen, survivingCell.hashedExtLen)
	}
	hph.collapseTracer(siblingPath, bytes.Clone(hph.currentKey[:depth]))
}

// Requires that prefix to already be unfolded.
func (hph *HexPatriciaHashed) updateCell(plainKey, hashedKey []byte, u *Update) (cell *cell) {
	hph.metrics.Updates(plainKey)

	if u.Deleted() {
		if hph.collapseTracer != nil && hph.activeRows > 0 {
			hph.detectCollapseBeforeDelete(hashedKey)
		}

		hph.deleteCell(hashedKey)
		hph.lastUpdateCellWasEmpty = false
		return nil
	}

	var depth int16
	if hph.activeRows == 0 {
		cell = &hph.root
		hph.rootTouched, hph.rootPresent = true, true
	} else {
		row := hph.activeRows - 1
		depth = hph.depths[row]
		nibble := int(hashedKey[hph.currentKeyLen])
		cell = &hph.grid[row][nibble]
		col := uint16(1) << nibble

		hph.touchMap[row] |= col
		hph.afterMap[row] |= col
		if hph.traceW != nil {
			fmt.Fprintf(hph.traceW, "updateCell setting (%d, %x, depth=%d)\n", row, nibble, depth)
		}
	}
	hph.lastUpdateCellWasEmpty = cell.IsEmpty()
	if cell.hashedExtLen == 0 {
		copy(cell.hashedExtension[:], hashedKey[depth:])
		cell.hashedExtLen = int16(len(hashedKey)) - depth
		if hph.traceW != nil {
			fmt.Fprintf(hph.traceW, "set downHasheKey=[%x]\n", cell.hashedExtension[:cell.hashedExtLen])
		}
	} else if hph.traceW != nil {
		fmt.Fprintf(hph.traceW, "keep downHasheKey=[%x]\n", cell.hashedExtension[:cell.hashedExtLen])
	}
	if int16(len(plainKey)) == hph.accountKeyLen {
		cell.accountAddrLen = int16(len(plainKey))
		copy(cell.accountAddr[:], plainKey)

		cell.CodeHash = empty.CodeHash
	} else {
		cell.storageAddrLen = int16(len(plainKey))
		copy(cell.storageAddr[:], plainKey)
	}
	cell.stateHashLen = 0

	cell.setFromUpdate(u)
	if hph.traceW != nil {
		fmt.Fprintf(hph.traceW, "updateCell %x => %s\n", plainKey, u.String())
	}
	return cell
}

func (hph *HexPatriciaHashed) RootHash() ([]byte, error) {
	hph.root.stateHashLen = 0
	rootHash, err := hph.computeCellHash(&hph.root, 0, nil)
	if err != nil {
		return nil, err
	}
	return rootHash[1:], nil
}

// plainKey is used only for per-key metrics labelling; pass nil if no attribution is needed.
func (hph *HexPatriciaHashed) unfoldKeyPath(hashedKey, plainKey []byte) error {
	for unfolding := hph.needUnfolding(hashedKey); unfolding > 0; unfolding = hph.needUnfolding(hashedKey) {
		printLater := hph.currentKeyLen == 0 && hph.mounted && hph.traceW != nil
		var unfoldDone func()
		if dbg.KVReadLevelledMetrics {
			unfoldDone = hph.metrics.StartUnfolding(plainKey)
		}
		if err := hph.unfold(hashedKey, unfolding); err != nil {
			return fmt.Errorf("unfold: %w", err)
		}
		if unfoldDone != nil {
			unfoldDone()
		}
		if printLater {
			fmt.Fprintf(hph.traceW, "[%x] subtrie pref '%x' d=%d\n", hph.mountedNib, hph.currentKey[:hph.currentKeyLen], hph.depths[max(0, hph.activeRows-1)])
		}
	}
	return nil
}

func (hph *HexPatriciaHashed) followAndUpdate(hashedKey, plainKey []byte, stateUpdate *Update) (err error) {
	for hph.needFolding(hashedKey) {
		var foldDone func()
		if dbg.KVReadLevelledMetrics {
			foldDone = hph.metrics.StartFolding(plainKey)
		}
		if err := hph.fold(); err != nil {
			return fmt.Errorf("fold: %w", err)
		}
		if foldDone != nil {
			foldDone()
		}
	}
	if err := hph.unfoldKeyPath(hashedKey, plainKey); err != nil {
		return err
	}

	if stateUpdate == nil {
		if int16(len(plainKey)) == hph.accountKeyLen {
			hph.metrics.AccountLoad(plainKey)
			stateUpdate, err = hph.accountFromCacheOrDB(plainKey)
			if err != nil {
				return fmt.Errorf("GetAccount for key %x failed: %w", plainKey, err)
			}
		} else {
			hph.metrics.StorageLoad(plainKey)
			stateUpdate, err = hph.storageFromCacheOrDB(plainKey)
			if err != nil {
				return fmt.Errorf("GetStorage for key %x failed: %w", plainKey, err)
			}
		}
	}
	hph.updateCell(plainKey, hashedKey, stateUpdate)

	mxTrieProcessedKeys.Inc()
	return nil
}

func (hph *HexPatriciaHashed) foldMounted(ctx context.Context, nib int) (cell, error) {
	if nib != hph.mountedNib {
		panic(fmt.Sprintf("foldMounted: nib (%x)!= mountedNib (%x)", nib, hph.mountedNib))
	}

	if hph.traceW != nil {
		fmt.Fprintf(hph.traceW, "====[%x] folding rows %d depths %+v\n", hph.mountedNib, hph.activeRows, hph.depths[:hph.activeRows])
		defer func() { fmt.Fprintf(hph.traceW, "=======[%x] folded =========\n", hph.mountedNib) }()
	}

	for hph.activeRows > 0 {
		if err := ctx.Err(); err != nil {
			return cell{}, err
		}
		if hph.activeRows == 1 && hph.depths[hph.activeRows-1] == hph.mountWall {
			if hph.traceW != nil {
				fmt.Fprintf(hph.traceW, "mount early as nibble %02x %s\n", hph.mountedNib, hph.grid[0][hph.mountedNib].String())
			}
			return hph.grid[0][hph.mountedNib], nil
		}
		if err := hph.fold(); err != nil {
			return cell{}, fmt.Errorf("final fold: %w", err)
		}
	}

	if hph.traceW != nil {
		fmt.Fprintf(hph.traceW, "===[%x] !@folded to the root\n", hph.mountedNib)
	}
	if hph.rootPresent && hph.rootTouched {
		if hph.traceW != nil {
			fmt.Fprintf(hph.traceW, "mount root as %02x %s\n", hph.mountedNib, hph.root.String())
		}
		return hph.root, nil
	}
	return cell{}, fmt.Errorf("foldMounted[%x]: folded past the mount wall to an unrooted base; the base must be seeded with a wall row", hph.mountedNib)
}

// Materializes the branch a folded (never-unfolded) extension diverges into, so a strict verifier can descend it.
// Hash-verified: a wrong prefix errors rather than corrupts.
func (hph *HexPatriciaHashed) captureExtensionDivergence(hashedKey []byte, set *witnessNodeSet) error {
	if hph.activeRows == 0 {
		return nil
	}
	row := hph.activeRows - 1
	depth := hph.depths[row]
	keyPos := hph.currentKeyLen
	if keyPos >= int16(len(hashedKey)) {
		return nil
	}
	cell := &hph.grid[row][hashedKey[keyPos]]
	if cell.hashedExtLen == 0 || cell.hashLen == 0 {
		return nil
	}
	extKeyLength := clampToAccountBoundary(depth, cell.hashedExtLen)
	hashedExtKey := cell.hashedExtension[:extKeyLength]
	endPos := min(keyPos+extKeyLength+1, int16(len(hashedKey)))
	fullPathLength := int(keyPos+1) + len(hashedExtKey)
	if bytes.Equal(hashedExtKey, hashedKey[keyPos+1:endPos]) || fullPathLength == 64 || fullPathLength == 128 {
		return nil
	}
	branchPrefix := make([]byte, 0, fullPathLength)
	branchPrefix = append(branchPrefix, hashedKey[:keyPos+1]...)
	branchPrefix = append(branchPrefix, hashedExtKey...)
	bn, err := hph.witnessMaterializeBranchChild(branchPrefix, int16(len(branchPrefix))+1, cell.hash[:cell.hashLen])
	if err != nil {
		return err
	}
	encoded, err := trie.NewInMemoryTrie(bn).RLPEncode()
	if err != nil {
		return err
	}
	if len(encoded) > 0 {
		set.onNode(encoded[0], cell.hash[:cell.hashLen])
	}
	return nil
}

// Returns the captured node superset (root first), the fold's hashed keys, and the root hash;
// callers prune to the lean set.
func (hph *HexPatriciaHashed) Witnesses(ctx context.Context, updates *Updates, produceExclusionProofs bool, logPrefix string) (nodes [][]byte, provedKeys [][]byte, rootHash []byte, err error) {
	hph.memoizationOff = true
	set := newWitnessNodeSet()
	hph.witness.tracer = set
	defer hph.witness.reset()

	provedKeys = make([][]byte, 0, updates.Size())
	err = updates.HashSort(ctx, nil, func(hashedKey, plainKey []byte, stateUpdate *Update) error {
		provedKeys = append(provedKeys, bytes.Clone(hashedKey))
		if len(plainKey) > 0 {
			if int16(len(plainKey)) == hph.accountKeyLen {
				if _, err := hph.accountFromCacheOrDB(plainKey); err != nil {
					return fmt.Errorf("account with plainkey=%x not found: %w", plainKey, err)
				}
			} else {
				if _, err := hph.storageFromCacheOrDB(plainKey); err != nil {
					return fmt.Errorf("storage with plainkey=%x not found: %w", plainKey, err)
				}
			}
		}

		for hph.needFolding(hashedKey) {
			if err := hph.fold(); err != nil {
				return fmt.Errorf("fold: %w", err)
			}
		}
		for hph.activeRows > 0 && !hph.branchBefore[hph.activeRows-1] {
			if err := hph.fold(); err != nil {
				return fmt.Errorf("fold non-branch: %w", err)
			}
		}

		for hph.currentKeyLen < int16(len(hashedKey)) {
			unfolding := hph.needUnfolding(hashedKey)
			if unfolding <= 0 {
				break
			}
			if produceExclusionProofs {
				if err := hph.captureExtensionDivergence(hashedKey, set); err != nil {
					return fmt.Errorf("capture extension divergence: %w", err)
				}
			}
			if err := hph.unfold(hashedKey, unfolding); err != nil {
				return fmt.Errorf("unfold: %w", err)
			}
		}
		if hph.activeRows > 0 && !hph.branchBefore[hph.activeRows-1] &&
			hph.currentKeyLen < int16(len(hashedKey)) {
			divergeNibble := hashedKey[hph.currentKeyLen]
			if hph.grid[hph.activeRows-1][divergeNibble].IsEmpty() {
				if err := hph.fold(); err != nil {
					return fmt.Errorf("fold empty diverging row: %w", err)
				}
			}
		}

		if hph.activeRows > 0 && hph.currentKeyLen < int16(len(hashedKey)) {
			lastNibble := int(hashedKey[hph.currentKeyLen])
			lastCell := &hph.grid[hph.activeRows-1][lastNibble]
			if int16(len(hashedKey)) == hph.depths[hph.activeRows-1] && len(hashedKey) != 64 && len(hashedKey) != 128 && lastCell.hashLen > 0 {
				if err := hph.unfold(hashedKey, 1); err != nil {
					return fmt.Errorf("extra unfold: %w", err)
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("hash sort failed: %w", err)
	}

	for hph.activeRows > 0 {
		if err := hph.fold(); err != nil {
			return nil, nil, nil, fmt.Errorf("final fold: %w", err)
		}
	}

	rootHash, err = hph.RootHash()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("root hash evaluation failed: %w", err)
	}
	nodes, err = set.nodes(rootHash)
	if err != nil {
		return nil, nil, nil, err
	}
	return nodes, provedKeys, rootHash, nil
}

func (hph *HexPatriciaHashed) Process(ctx context.Context, updates *Updates, logPrefix string, onProgress func(*CommitProgress), warmup WarmupConfig) (rootHash []byte, err error) {
	var (
		m  runtime.MemStats
		ki uint64

		updatesCount = updates.Size()
		start        = time.Now()
		logEvery     = time.NewTicker(20 * time.Second)
	)

	hph.metrics.Reset()
	hph.metrics.updates.Store(updatesCount)
	if hph.metrics.collectCommitmentMetrics {
		defer func() {
			hph.metrics.TotalProcessingTimeInc(start)
			hph.metrics.WriteToCSV()
		}()
	}

	defer func() { logEvery.Stop() }()

	var warmuper *Warmuper
	if warmup.Enabled {
		warmuper = NewWarmuper(ctx, warmup)
		warmuper.Start()
		defer warmuper.CloseAndWait()
	}

	err = updates.HashSort(ctx, warmuper, func(hashedKey, plainKey []byte, stateUpdate *Update) error {
		select {
		case <-logEvery.C:
			if onProgress != nil {
				onProgress(&CommitProgress{
					KeyIndex:    ki,
					UpdateCount: updatesCount,
					Metrics:     hph.metrics.AsValues(),
				})
			} else {
				dbg.ReadMemStats(&m)
				keysPerSec := uint64(float64(ki) / time.Since(start).Seconds())
				log.Info(fmt.Sprintf("[%s][agg] computing trie", logPrefix),
					append(append([]any{"progress", fmt.Sprintf("%s/%s", common.PrettyCounter(ki), common.PrettyCounter(updatesCount)), "keys/s", common.PrettyCounter(keysPerSec)},
						hph.metrics.logMetrics()...), "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))...)
			}
		default:
		}

		if hph.traceW != nil {
			update := stateUpdate

			if update == nil {
				if int16(len(plainKey)) == hph.accountKeyLen {
					update, err = hph.accountFromCacheOrDB(plainKey)
					if err != nil {
						return fmt.Errorf("GetAccount for key %x failed: %w", plainKey, err)
					}
				} else {
					update, err = hph.storageFromCacheOrDB(plainKey)
					if err != nil {
						return fmt.Errorf("GetStorage for key %x failed: %w", plainKey, err)
					}
				}
			}

			trace := fmt.Sprintf("(%d/%d) plainKey [%x] %s hashedKey [%x] currentKey [%x]", ki+1, updatesCount, plainKey, update, hashedKey, hph.currentKey[:hph.currentKeyLen])

			fmt.Fprintf(hph.traceW, "[proc] %s\n", trace)
		}

		if err := hph.followAndUpdate(hashedKey, plainKey, stateUpdate); err != nil {
			return fmt.Errorf("followAndUpdate: %w", err)
		}
		ki++
		if onProgress != nil && ki == updatesCount {
			onProgress(&CommitProgress{
				KeyIndex:    ki,
				UpdateCount: updatesCount,
				Metrics:     hph.metrics.AsValues(),
			})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("hash sort failed: %w", err)
	}

	for hph.activeRows > 0 {
		var foldDone func()
		if dbg.KVReadLevelledMetrics {
			foldDone = hph.metrics.StartFolding(nil)
		}
		if err = hph.fold(); err != nil {
			return nil, fmt.Errorf("final fold: %w", err)
		}
		if foldDone != nil {
			foldDone()
		}
	}

	rootHash, err = hph.RootHash()
	if err != nil {
		return nil, fmt.Errorf("root hash evaluation failed: %w", err)
	}
	if hph.traceW != nil {
		fmt.Fprintf(hph.traceW, "root hash %x updates %d\n", rootHash, updatesCount)
	}
	if warmuper != nil {
		warmuper.DrainPending()
	}

	if hph.branchEncoder.DeferUpdatesEnabled() && !hph.leaveDeferredForCaller {
		if err = hph.branchEncoder.ApplyDeferredUpdates(runtime.NumCPU(), hph.ctx.PutBranch); err != nil {
			return nil, fmt.Errorf("apply deferred updates: %w", err)
		}
		hph.branchEncoder.ClearDeferred()
	}

	flushTrieStateRates()

	if dbg.KVReadLevelledMetrics {
		hph.metrics.CollectFileDepthStats(hph.hadToLoadL)
		log.Debug("commitment finished, counters updated (no reset)",
			"skipRatio", fmt.Sprintf("%.1f%%", 100*(float64(skippedLoad.Load())/float64(hadToLoad.Load()+skippedLoad.Load()))),
			"resetRatio", fmt.Sprintf("%.1f%%", 100*(float64(hadToReset.Load())/float64(hadToLoad.Load()))),
			"keys", common.PrettyCounter(ki), "spent", time.Since(start),
		)
		ends := make([]uint64, 0, len(hph.hadToLoadL))
		for k := range hph.hadToLoadL {
			ends = append(ends, k)
		}
		slices.SortFunc(ends, func(a, b uint64) int { return cmp.Compare(b, a) })
		var Li int
		for _, k := range ends {
			v := hph.hadToLoadL[k]
			accs := fmt.Sprintf("load=%s skip=%s (%.1f%%) reset %.1f%%", common.PrettyCounter(v.accLoaded), common.PrettyCounter(v.accSkipped), 100*(float64(v.accSkipped)/float64(v.accLoaded+v.accSkipped)), 100*(float64(v.accReset)/float64(v.accReset+v.accSkipped)))
			stors := fmt.Sprintf("load=%s skip=%s (%.1f%%) reset %.1f%%", common.PrettyCounter(v.storLoaded), common.PrettyCounter(v.storSkipped), 100*(float64(v.storSkipped)/float64(v.storLoaded+v.storSkipped)), 100*(float64(v.storReset)/float64(v.storReset+v.storSkipped)))
			if k == 0 {
				log.Debug("branchData memoization, new branches", "endStep", k, "accounts", accs, "storages", stors)
			} else {
				log.Debug("branchData memoization", "L", Li, "endStep", k, "accounts", accs, "storages", stors)
				Li++

				mxTrieStateLevelledSkipRatesAccount[min(Li, 5)].Add(float64(v.accSkipped))
				mxTrieStateLevelledSkipRatesStorage[min(Li, 5)].Add(float64(v.storSkipped))
				mxTrieStateLevelledLoadRatesAccount[min(Li, 5)].Add(float64(v.accLoaded))
				mxTrieStateLevelledLoadRatesStorage[min(Li, 5)].Add(float64(v.storLoaded))
			}
		}
	}

	return rootHash, nil
}

func (hph *HexPatriciaHashed) SetTraceWriter(w io.Writer) { hph.traceW = w }

func (hph *HexPatriciaHashed) EnableCsvMetrics(filePathPrefix string) {
	hph.metrics.EnableCsvMetrics(filePathPrefix)
	hph.cfg.CsvMetricsFilePrefix = filePathPrefix
}

func (hph *HexPatriciaHashed) Variant() TrieVariant { return VariantHexPatriciaTrie }

// Caller takes ownership of the returned slice.
func (hph *HexPatriciaHashed) TakeDeferredUpdates() []*DeferredBranchUpdate {
	deferred := hph.branchEncoder.deferred
	hph.branchEncoder.deferred = make([]*DeferredBranchUpdate, 0, 64)
	if hph.branchEncoder.pendingPrefixes != nil {
		hph.branchEncoder.pendingPrefixes.Clear()
	}
	ResetDeferredUpdateMetrics()
	return deferred
}

func (hph *HexPatriciaHashed) HasPendingDeferredUpdates() bool {
	return len(hph.branchEncoder.deferred) > 0
}

func (hph *HexPatriciaHashed) ApplyAndClearInlineDeferredUpdates() error {
	if err := hph.branchEncoder.ApplyDeferredUpdates(runtime.NumCPU(), hph.ctx.PutBranch); err != nil {
		return fmt.Errorf("apply deferred updates: %w", err)
	}
	hph.branchEncoder.ClearDeferred()
	return nil
}

func (hph *HexPatriciaHashed) SetLeaveDeferredForCaller(leave bool) {
	hph.leaveDeferredForCaller = leave
}

// The aggregator-scope BranchCache is intentionally not cleared here; SharedDomains.Unwind
// handles correctness via txN-tagged eviction.
func (hph *HexPatriciaHashed) Reset() {
	hph.root.reset()
	hph.rootTouched = false
	hph.rootChecked = false
	hph.rootPresent = true
}

func (hph *HexPatriciaHashed) ResetContext(ctx PatriciaContext) {
	hph.ctx = ctx
}

func (hph *HexPatriciaHashed) branchFromCacheOrDB(key []byte) ([]byte, error) {
	data, _, err := hph.ctx.Branch(key)
	return data, err
}

func (hph *HexPatriciaHashed) accountFromCacheOrDB(plainKey []byte) (*Update, error) {
	return hph.ctx.Account(plainKey)
}

func (hph *HexPatriciaHashed) storageFromCacheOrDB(plainKey []byte) (*Update, error) {
	return hph.ctx.Storage(plainKey)
}

type stateRootFlag int8

var (
	stateRootPresent stateRootFlag = 1
	stateRootChecked stateRootFlag = 2
	stateRootTouched stateRootFlag = 4
)

type state struct {
	Root   []byte
	Depths [128]int16
	// TouchMap/AfterMap are not symmetric: see HexPatriciaHashed.touchMap/afterMap.
	TouchMap     [128]uint16
	AfterMap     [128]uint16
	BranchBefore [128]bool
	RootChecked  bool
	RootTouched  bool
	RootPresent  bool
}

func (s *state) Encode(buf []byte) ([]byte, error) {
	var rootFlags stateRootFlag
	if s.RootPresent {
		rootFlags |= stateRootPresent
	}
	if s.RootChecked {
		rootFlags |= stateRootChecked
	}
	if s.RootTouched {
		rootFlags |= stateRootTouched
	}

	ee := bytes.NewBuffer(buf)
	if err := binary.Write(ee, binary.BigEndian, int8(rootFlags)); err != nil {
		return nil, fmt.Errorf("encode rootFlags: %w", err)
	}
	if err := binary.Write(ee, binary.BigEndian, uint16(len(s.Root))); err != nil {
		return nil, fmt.Errorf("encode root len: %w", err)
	}
	if n, err := ee.Write(s.Root); err != nil || n != len(s.Root) {
		return nil, fmt.Errorf("encode root: %w", err)
	}
	d := make([]byte, len(s.Depths))
	for i := range len(s.Depths) {
		d[i] = byte(s.Depths[i])
	}
	if n, err := ee.Write(d); err != nil || n != len(s.Depths) {
		return nil, fmt.Errorf("encode depths: %w", err)
	}
	if err := binary.Write(ee, binary.BigEndian, s.TouchMap); err != nil {
		return nil, fmt.Errorf("encode touchMap: %w", err)
	}
	if err := binary.Write(ee, binary.BigEndian, s.AfterMap); err != nil {
		return nil, fmt.Errorf("encode afterMap: %w", err)
	}

	var before1, before2 uint64
	for i := range 64 {
		if s.BranchBefore[i] {
			before1 |= 1 << i
		}
	}
	for i, j := 64, 0; i < 128; i, j = i+1, j+1 {
		if s.BranchBefore[i] {
			before2 |= 1 << j
		}
	}
	if err := binary.Write(ee, binary.BigEndian, before1); err != nil {
		return nil, fmt.Errorf("encode branchBefore_1: %w", err)
	}
	if err := binary.Write(ee, binary.BigEndian, before2); err != nil {
		return nil, fmt.Errorf("encode branchBefore_2: %w", err)
	}
	return ee.Bytes(), nil
}

func (s *state) Decode(buf []byte) error {
	aux := bytes.NewBuffer(buf)
	var rootFlags stateRootFlag
	if err := binary.Read(aux, binary.BigEndian, &rootFlags); err != nil {
		return fmt.Errorf("rootFlags: %w", err)
	}

	if rootFlags&stateRootPresent != 0 {
		s.RootPresent = true
	}
	if rootFlags&stateRootTouched != 0 {
		s.RootTouched = true
	}
	if rootFlags&stateRootChecked != 0 {
		s.RootChecked = true
	}

	var rootSize uint16
	if err := binary.Read(aux, binary.BigEndian, &rootSize); err != nil {
		return fmt.Errorf("root size: %w", err)
	}
	s.Root = make([]byte, rootSize)
	if _, err := aux.Read(s.Root); err != nil {
		return fmt.Errorf("root: %w", err)
	}
	d := make([]byte, len(s.Depths))
	if err := binary.Read(aux, binary.BigEndian, &d); err != nil {
		return fmt.Errorf("depths: %w", err)
	}
	for i := range len(s.Depths) {
		s.Depths[i] = int16(d[i])
	}
	if err := binary.Read(aux, binary.BigEndian, &s.TouchMap); err != nil {
		return fmt.Errorf("touchMap: %w", err)
	}
	if err := binary.Read(aux, binary.BigEndian, &s.AfterMap); err != nil {
		return fmt.Errorf("afterMap: %w", err)
	}
	var branch1, branch2 uint64
	if err := binary.Read(aux, binary.BigEndian, &branch1); err != nil {
		return fmt.Errorf("branchBefore1: %w", err)
	}
	if err := binary.Read(aux, binary.BigEndian, &branch2); err != nil {
		return fmt.Errorf("branchBefore2: %w", err)
	}

	for i := range 64 {
		if branch1&(1<<i) != 0 {
			s.BranchBefore[i] = true
		}
	}
	for i, j := 64, 0; i < 128; i, j = i+1, j+1 {
		if branch2&(1<<j) != 0 {
			s.BranchBefore[i] = true
		}
	}
	return nil
}

func (cell *cell) Encode() []byte {
	var pos = int16(1)
	size := pos + 5 + cell.hashLen + cell.accountAddrLen + cell.storageAddrLen + cell.hashedExtLen + cell.extLen
	buf := make([]byte, size)

	var flags uint8
	if cell.hashLen != 0 {
		flags |= cellFlagHash
		buf[pos] = byte(cell.hashLen)
		pos++
		copy(buf[pos:pos+cell.hashLen], cell.hash[:])
		pos += cell.hashLen
	}
	if cell.accountAddrLen != 0 {
		flags |= cellFlagAccount
		buf[pos] = byte(cell.accountAddrLen)
		pos++
		copy(buf[pos:pos+cell.accountAddrLen], cell.accountAddr[:])
		pos += cell.accountAddrLen
	}
	if cell.storageAddrLen != 0 {
		flags |= cellFlagStorage
		buf[pos] = byte(cell.storageAddrLen)
		pos++
		copy(buf[pos:pos+cell.storageAddrLen], cell.storageAddr[:])
		pos += cell.storageAddrLen
	}
	if cell.hashedExtLen != 0 {
		flags |= cellFlagDownHash
		buf[pos] = byte(cell.hashedExtLen)
		pos++
		copy(buf[pos:pos+cell.hashedExtLen], cell.hashedExtension[:cell.hashedExtLen])
		pos += cell.hashedExtLen
	}
	if cell.extLen != 0 {
		flags |= cellFlagExtension
		buf[pos] = byte(cell.extLen)
		pos++
		copy(buf[pos:pos+cell.extLen], cell.extension[:])
		pos += cell.extLen //nolint:ineffassign
	}
	if cell.Deleted() {
		flags |= cellFlagDelete
	}
	buf[0] = flags
	return buf
}

const (
	cellFlagHash = uint8(1 << iota)
	cellFlagAccount
	cellFlagStorage
	cellFlagDownHash
	cellFlagExtension
	cellFlagDelete
)

func (cell *cell) Decode(buf []byte) error {
	if len(buf) < 1 {
		return errors.New("invalid buffer size to contain cell (at least 1 byte expected)")
	}
	cell.reset()

	var pos int16
	flags := buf[pos]
	pos++

	if flags&cellFlagHash != 0 {
		cell.hashLen = int16(buf[pos])
		pos++
		copy(cell.hash[:], buf[pos:pos+cell.hashLen])
		pos += cell.hashLen
	}
	if flags&cellFlagAccount != 0 {
		cell.accountAddrLen = int16(buf[pos])
		pos++
		copy(cell.accountAddr[:], buf[pos:pos+cell.accountAddrLen])
		pos += cell.accountAddrLen
	}
	if flags&cellFlagStorage != 0 {
		cell.storageAddrLen = int16(buf[pos])
		pos++
		copy(cell.storageAddr[:], buf[pos:pos+cell.storageAddrLen])
		pos += cell.storageAddrLen
	}
	if flags&cellFlagDownHash != 0 {
		cell.hashedExtLen = int16(buf[pos])
		pos++
		copy(cell.hashedExtension[:], buf[pos:pos+cell.hashedExtLen])
		pos += cell.hashedExtLen
	}
	if flags&cellFlagExtension != 0 {
		cell.extLen = int16(buf[pos])
		pos++
		copy(cell.extension[:], buf[pos:pos+cell.extLen])
		pos += cell.extLen //nolint:ineffassign
	}
	if flags&cellFlagDelete != 0 {
		log.Warn("deleted cell should not be encoded", "cell", cell.String())
		cell.Update.Flags = DeleteUpdate
	}
	return nil
}

func (hph *HexPatriciaHashed) EncodeCurrentState(buf []byte) ([]byte, error) {
	s := state{
		RootChecked: hph.rootChecked,
		RootTouched: hph.rootTouched,
		RootPresent: hph.rootPresent,
	}
	if hph.currentKeyLen > 0 {
		panic("currentKeyLen > 0")
	}

	s.Root = hph.root.Encode()
	copy(s.Depths[:], hph.depths[:])
	copy(s.BranchBefore[:], hph.branchBefore[:])
	copy(s.TouchMap[:], hph.touchMap[:])
	copy(s.AfterMap[:], hph.afterMap[:])

	return s.Encode(buf)
}

func (hph *HexPatriciaHashed) SetState(buf []byte) error {
	hph.Reset()

	if buf == nil {
		hph.currentKeyLen = 0
		hph.rootChecked = false
		hph.rootTouched = false
		hph.rootPresent = false
		hph.activeRows = 0

		for i := range len(hph.depths) {
			hph.depths[i] = 0
			hph.branchBefore[i] = false
			hph.touchMap[i] = 0
			hph.afterMap[i] = 0
		}
		return nil
	}
	if hph.activeRows != 0 {
		return errors.New("target trie has active rows, could not reset state before fold")
	}

	var s state
	if err := s.Decode(buf); err != nil {
		return err
	}

	if err := hph.root.Decode(s.Root); err != nil {
		return err
	}
	hph.rootChecked = s.RootChecked
	hph.rootTouched = s.RootTouched
	hph.rootPresent = s.RootPresent

	copy(hph.depths[:], s.Depths[:])
	copy(hph.branchBefore[:], s.BranchBefore[:])
	copy(hph.touchMap[:], s.TouchMap[:])
	copy(hph.afterMap[:], s.AfterMap[:])

	if hph.root.accountAddrLen > 0 {
		if hph.ctx == nil {
			panic("nil ctx")
		}

		update, err := hph.accountFromCacheOrDB(hph.root.accountAddr[:hph.root.accountAddrLen])
		if err != nil {
			return err
		}
		hph.root.setFromUpdate(update)
	}
	if hph.root.storageAddrLen > 0 {
		if hph.ctx == nil {
			panic("nil ctx")
		}
		update, err := hph.storageFromCacheOrDB(hph.root.storageAddr[:hph.root.storageAddrLen])
		if err != nil {
			return err
		}
		hph.root.setFromUpdate(update)
	}
	// A leaf root's navigation path is derivable but not reliably persisted: without it a
	// wall probe sees an unfoldable root and the mount paths overwrite the leaf in place.
	if hph.root.accountAddrLen > 0 || hph.root.storageAddrLen > 0 {
		hph.root.hashedExtLen = 0
		if err := hph.root.deriveHashedKeys(0, hph.keccak, hph.accountKeyLen, hph.cellHashBuf[:]); err != nil {
			return err
		}
	}
	// Blobs written before propagate folds set rootPresent carry false for non-empty
	// roots; a non-empty root is present by definition.
	if !hph.root.IsEmpty() {
		hph.rootPresent = true
	}

	return nil
}

func HexTrieExtractStateRoot(enc []byte) ([]byte, uint64, uint64, error) {
	if len(enc) < 18 { // 8*2+2
		return nil, 0, 0, fmt.Errorf("invalid state length %x (min %d expected)", len(enc), 18)
	}

	txn := binary.BigEndian.Uint64(enc)
	bn := binary.BigEndian.Uint64(enc[8:])
	sl := binary.BigEndian.Uint16(enc[16:18])
	var s state
	if err := s.Decode(enc[18 : 18+sl]); err != nil {
		return nil, 0, 0, err
	}
	root := new(cell)
	if err := root.Decode(s.Root); err != nil {
		return nil, 0, 0, err
	}
	return root.hash[:], bn, txn, nil
}

func HexTrieStateToShortString(enc []byte) (string, error) {
	if len(enc) < 18 {
		return "", fmt.Errorf("invalid state length %x (min %d expected)", len(enc), 18)
	}
	txn := binary.BigEndian.Uint64(enc)
	bn := binary.BigEndian.Uint64(enc[8:])
	sl := binary.BigEndian.Uint16(enc[16:18])

	var s state
	if err := s.Decode(enc[18 : 18+sl]); err != nil {
		return "", err
	}
	root := new(cell)
	if err := root.Decode(s.Root); err != nil {
		return "", err
	}
	return fmt.Sprintf("block: %d txn: %d rootHash: %x", bn, txn, root.hash[:]), nil
}

func HexTrieStateToString(enc []byte) (string, error) {
	if len(enc) < 18 {
		return "", fmt.Errorf("invalid state length %x (min %d expected)", len(enc), 18)
	}
	txn := binary.BigEndian.Uint64(enc)
	bn := binary.BigEndian.Uint64(enc[8:])
	sl := binary.BigEndian.Uint16(enc[16:18])

	var s state
	sb := new(strings.Builder)
	if err := s.Decode(enc[18 : 18+sl]); err != nil {
		return "", err
	}
	fmt.Fprintf(sb, "block: %d txn: %d\n", bn, txn)

	printAfterMap := func(sb *strings.Builder, name string, list []uint16, depths []int16, existedBefore []bool) {
		fmt.Fprintf(sb, "\t::%s::\n\n", name)
		lastNonZero := 0
		for i, l := range slices.Backward(list) {
			if l != 0 {
				lastNonZero = i
				break
			}
		}
		for i, v := range list {
			newBranchSuf := ""
			if !existedBefore[i] {
				newBranchSuf = " NEW"
			}

			fmt.Fprintf(sb, " d=%3d %016b%s\n", depths[i], v, newBranchSuf)
			if i == lastNonZero {
				break
			}
		}
	}
	fmt.Fprintf(sb, " rootNode: %x [touched=%t, present=%t, checked=%t]\n", s.Root, s.RootTouched, s.RootPresent, s.RootChecked)

	root := new(cell)
	if err := root.Decode(s.Root); err != nil {
		return "", err
	}

	fmt.Fprintf(sb, "RootHash: %x\n", root.hash)
	printAfterMap(sb, "afterMap", s.AfterMap[:], s.Depths[:], s.BranchBefore[:])

	return sb.String(), nil
}

func (hph *HexPatriciaHashed) Grid() [128][16]cell {
	return hph.grid
}
