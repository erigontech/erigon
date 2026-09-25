// Copyright 2026 The Erigon Authors
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

package v4

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

type LegacyValues interface {
	Account(plainKey []byte) ([]byte, error)
	Storage(plainKey []byte) ([]byte, error)
}

type LegacyKind byte

const (
	LegacyTombstone LegacyKind = iota
	LegacyKeep
	LegacyDirect
	LegacySynthesized
)

type LegacyEmitFunc func(key, value []byte, kind LegacyKind) error

type LegacyEntry struct {
	Kind  LegacyKind
	Value []byte
}

var (
	errLegacyBranch   = errors.New("commitment v4: invalid legacy branch")
	errLegacyValue    = errors.New("commitment v4: legacy leaf value is missing")
	errLegacyState    = errors.New("commitment v4: invalid legacy state")
	ErrLegacyConflict = errors.New("commitment v4: conflicting legacy conversions")

	errStaleStorageRecord = errors.New("commitment v4: legacy storage record references a deleted slot")
)

type LegacyConverter struct {
	vals        LegacyValues
	keysV2      bool
	incremental bool

	branch  node
	root    node
	prefix  []byte
	scratch []byte
	key     []byte
	rootKey []byte
	out     []byte
	rootOut []byte

	owner     []byte
	ownerHash [32]byte
}

func NewLegacyConverter(vals LegacyValues, keysV2, incremental bool) *LegacyConverter {
	return &LegacyConverter{
		vals:        vals,
		keysV2:      keysV2,
		incremental: incremental,
		branch:      node{slots: make([]childSlot, 0, 16)},
		root:        node{slots: make([]childSlot, 0, 1)},
	}
}

func (c *LegacyConverter) Convert(legacyKey, value, prev []byte, emit LegacyEmitFunc) error {
	prefix, err := c.decodePrefix(legacyKey)
	if err != nil {
		return err
	}
	if len(prefix) >= 128 {
		return fmt.Errorf("%w: prefix of %d nibbles", errLegacyBranch, len(prefix))
	}
	plane, path, ownerHash := byte(planeAccount), prefix, []byte(nil)
	var addrHash [32]byte
	if len(prefix) >= 64 {
		plane, path, ownerHash = planeStorage, prefix[64:], packPath(prefix[:64], addrHash[:0])
	}
	c.key = nodeKey(plane, ownerHash, path, c.key[:0])

	if c.incremental && plane == planeAccount && len(prev) != 0 {
		if err := commitment.BranchData(prev).ForEachCell(func(_ int, cell commitment.BranchCell) error {
			if len(cell.AccountAddr) == 0 || len(cell.StorageAddr) == 0 && len(cell.Extension) == 0 && len(cell.Hash) == 0 {
				return nil
			}
			c.rootKey = StorageNodeKey(keccak.Sum256(cell.AccountAddr), nil, c.rootKey[:0])
			return emit(c.rootKey, nil, LegacyTombstone)
		}); err != nil {
			return fmt.Errorf("previous value of %x: %w", c.key, err)
		}
	}
	if len(value) == 0 || len(value) >= 4 && binary.BigEndian.Uint16(value[2:4]) == 0 {
		return emit(c.key, nil, LegacyDirect)
	}

	n := &c.branch
	resetNode(n, path, plane)
	c.scratch = c.scratch[:0]
	err = commitment.BranchData(value).ForEachCell(func(nib int, cell commitment.BranchCell) error {
		switch {
		case len(cell.AccountAddr) != 0:
			if plane != planeAccount || len(cell.AccountAddr) != length.Addr {
				return fmt.Errorf("%w: account cell %d of %d bytes on plane %02x", errLegacyBranch, nib, len(cell.AccountAddr), plane)
			}
			hashed := keccak.Sum256(cell.AccountAddr)
			suffix, err := c.suffix(&hashed, path, nib)
			if err != nil {
				return err
			}
			leaf, err := c.accountLeaf(cell, &hashed, emit)
			if err != nil {
				return err
			}
			appendLeaf(n, nib, suffix, leaf)
		case len(cell.StorageAddr) != 0:
			if plane != planeStorage || len(cell.StorageAddr) != length.Addr+length.Hash {
				return fmt.Errorf("%w: storage cell %d of %d bytes on plane %02x", errLegacyBranch, nib, len(cell.StorageAddr), plane)
			}
			if owner := c.ownerOf(cell.StorageAddr[:length.Addr]); owner != addrHash {
				return fmt.Errorf("%w: storage cell %d belongs to %x, branch to %x", errLegacyBranch, nib, owner, addrHash)
			}
			slotHash := keccak.Sum256(cell.StorageAddr[length.Addr:])
			suffix, err := c.suffix(&slotHash, path, nib)
			if err != nil {
				return err
			}
			slot, err := c.storageValue(cell.StorageAddr)
			if errors.Is(err, errLegacyValue) {
				return errStaleStorageRecord
			}
			if err != nil {
				return err
			}
			appendLeaf(n, nib, suffix, slot)
		case len(cell.Hash) == length.Hash:
			if err := checkNibbles(cell.Extension); err != nil {
				return err
			}
			if len(path)+1+len(cell.Extension) > 63 {
				return fmt.Errorf("%w: child %d ends at depth %d", errLegacyBranch, nib, len(path)+1+len(cell.Extension))
			}
			appendStoredChild(n, nib, cell.Hash, cell.Extension)
		default:
			return fmt.Errorf("%w: cell %d has no plain key and a %d-byte hash", errLegacyBranch, nib, len(cell.Hash))
		}
		return nil
	})
	if errors.Is(err, errStaleStorageRecord) {
		return emit(c.key, nil, LegacyTombstone)
	}
	if err != nil {
		return fmt.Errorf("%x: %w", c.key, err)
	}
	if bits.OnesCount16(n.childMask) < 2 {
		return fmt.Errorf("%w: %x has %d children", errLegacyBranch, c.key, bits.OnesCount16(n.childMask))
	}
	c.out = encodeRecord(n, len(path), c.out[:0])
	return emit(c.key, c.out, LegacyDirect)
}

func (c *LegacyConverter) decodePrefix(key []byte) ([]byte, error) {
	if c.keysV2 {
		return nibbles.DecodeKeyV2(key)
	}
	if len(key) == 0 || key[0]>>4 > 1 || key[0]>>4 == 0 && key[0]&0x0f != 0 {
		return nil, fmt.Errorf("%w: %x is not a compact branch prefix", errLegacyBranch, key)
	}
	c.prefix = c.prefix[:0]
	if key[0]>>4 == 1 {
		c.prefix = append(c.prefix, key[0]&0x0f)
	}
	for _, b := range key[1:] {
		c.prefix = append(c.prefix, b>>4, b&0x0f)
	}
	return c.prefix, nil
}

func (c *LegacyConverter) accountLeaf(cell commitment.BranchCell, addrHash *[32]byte, emit LegacyEmitFunc) ([]byte, error) {
	storageRoot := empty.RootHash[:]
	var rootHash [32]byte
	switch {
	case len(cell.StorageAddr) != 0:
		if len(cell.StorageAddr) != length.Addr+length.Hash || !bytes.Equal(cell.StorageAddr[:length.Addr], cell.AccountAddr) {
			return nil, fmt.Errorf("%w: account %x carries storage key %x", errLegacyBranch, cell.AccountAddr, cell.StorageAddr)
		}
		slot, err := c.storageValue(cell.StorageAddr)
		if err != nil {
			return nil, err
		}
		slotHash := keccak.Sum256(cell.StorageAddr[length.Addr:])
		resetNode(&c.root, nil, planeStorage)
		appendLeaf(&c.root, int(slotHash[0]>>4), appendHashSuffix(&c.scratch, &slotHash, 1), slot)
		if rootHash, err = c.emitRoot(addrHash, emit); err != nil {
			return nil, err
		}
		storageRoot = rootHash[:]
	case len(cell.Extension) != 0:
		if len(cell.Hash) != length.Hash || len(cell.Extension) > 63 {
			return nil, fmt.Errorf("%w: account %x has a %d-nibble storage extension over a %d-byte hash", errLegacyBranch, cell.AccountAddr, len(cell.Extension), len(cell.Hash))
		}
		if err := checkNibbles(cell.Extension); err != nil {
			return nil, err
		}
		resetNode(&c.root, cell.Extension, planeStorage)
		appendStoredChild(&c.root, int(cell.Extension[0]), cell.Hash, nil)
		var err error
		if rootHash, err = c.emitRoot(addrHash, emit); err != nil {
			return nil, err
		}
		storageRoot = rootHash[:]
	case len(cell.Hash) == length.Hash:
		storageRoot = cell.Hash
		if c.incremental {
			c.rootKey = StorageNodeKey(*addrHash, nil, c.rootKey[:0])
			if err := emit(c.rootKey, nil, LegacyKeep); err != nil {
				return nil, err
			}
		}
	case len(cell.Hash) != 0:
		return nil, fmt.Errorf("%w: account %x has a %d-byte storage root", errLegacyBranch, cell.AccountAddr, len(cell.Hash))
	}
	enc, err := c.vals.Account(cell.AccountAddr)
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, fmt.Errorf("%w: account %x", errLegacyValue, cell.AccountAddr)
	}
	start := len(c.scratch)
	if c.scratch, err = appendAccountLeafSerialised(c.scratch, enc, storageRoot); err != nil {
		return nil, fmt.Errorf("account %x: %w", cell.AccountAddr, err)
	}
	return c.scratch[start:], nil
}

func (c *LegacyConverter) emitRoot(addrHash *[32]byte, emit LegacyEmitFunc) ([32]byte, error) {
	hash, err := fold(&c.root, 0)
	if err != nil {
		return hash, err
	}
	c.rootKey = StorageNodeKey(*addrHash, nil, c.rootKey[:0])
	c.rootOut = encodeRecord(&c.root, 0, c.rootOut[:0])
	return hash, emit(c.rootKey, c.rootOut, LegacySynthesized)
}

func (c *LegacyConverter) storageValue(plainKey []byte) ([]byte, error) {
	v, err := c.vals.Storage(plainKey)
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, fmt.Errorf("%w: storage %x", errLegacyValue, plainKey)
	}
	if len(v) > length.Hash {
		return nil, fmt.Errorf("%w: storage %x has a %d-byte value", errLegacyBranch, plainKey, len(v))
	}
	start := len(c.scratch)
	c.scratch = append(c.scratch, v...)
	return c.scratch[start:], nil
}

func (c *LegacyConverter) ownerOf(addr []byte) [32]byte {
	if !bytes.Equal(addr, c.owner) {
		c.owner = append(c.owner[:0], addr...)
		c.ownerHash = keccak.Sum256(addr)
	}
	return c.ownerHash
}

func (c *LegacyConverter) suffix(hashed *[32]byte, path []byte, nib int) ([]byte, error) {
	if !packedMatches(hashed[:packedLen(len(path))], path) {
		return nil, fmt.Errorf("%w: leaf %x does not sit under %x", errLegacyBranch, *hashed, path)
	}
	if int(hashNibble(hashed, len(path))) != nib {
		return nil, fmt.Errorf("%w: leaf %x does not sit at %x+%x", errLegacyBranch, *hashed, path, nib)
	}
	return appendHashSuffix(&c.scratch, hashed, len(path)+1), nil
}

func appendHashSuffix(scratch *[]byte, hashed *[32]byte, from int) []byte {
	start := len(*scratch)
	if from&1 == 0 {
		*scratch = append(*scratch, hashed[from/2:]...)
	} else {
		for i := from / 2; i < 31; i++ {
			*scratch = append(*scratch, hashed[i]<<4|hashed[i+1]>>4)
		}
		*scratch = append(*scratch, hashed[31]<<4)
	}
	return (*scratch)[start:]
}

func hashNibble(hashed *[32]byte, i int) byte {
	if i&1 == 0 {
		return hashed[i/2] >> 4
	}
	return hashed[i/2] & 0x0f
}

func resetNode(n *node, path []byte, plane byte) {
	n.path = path
	n.slots = n.slots[:0]
	n.raw = nil
	n.record, n.layout, n.refs = Record{}, layout{}, nil
	n.plane = plane
	n.loaded = false
	n.childMask, n.leafMask, n.hashMask, n.slotMask = 0, 0, 0, 0
}

func appendLeaf(n *node, nib int, suffix, value []byte) {
	n.slots = append(n.slots, childSlot{suffix: suffix, value: value})
	bit := uint16(1) << nib
	n.childMask |= bit
	n.leafMask |= bit
	n.slotMask |= bit
}

func appendStoredChild(n *node, nib int, hash, ext []byte) {
	n.slots = append(n.slots, childSlot{ext: ext})
	copy(n.slots[len(n.slots)-1].hash[:], hash)
	bit := uint16(1) << nib
	n.childMask |= bit
	n.hashMask |= bit
	n.slotMask |= bit
}

func appendAccountLeafSerialised(dst, enc, storageRoot []byte) ([]byte, error) {
	var fields [3][]byte
	pos := 0
	for i := range fields {
		if pos >= len(enc) || pos+1+int(enc[pos]) > len(enc) {
			return nil, fmt.Errorf("%w: serialised account of %d bytes", errLegacyValue, len(enc))
		}
		fields[i] = enc[pos+1 : pos+1+int(enc[pos])]
		pos += 1 + int(enc[pos])
	}
	nonceBytes, balance, codeHash := fields[0], bytes.TrimLeft(fields[1], "\x00"), fields[2]
	if len(nonceBytes) > 8 || len(balance) > length.Hash || len(codeHash) != 0 && len(codeHash) != length.Hash {
		return nil, fmt.Errorf("%w: serialised account field lengths %d/%d/%d", errLegacyValue, len(nonceBytes), len(balance), len(codeHash))
	}
	var u commitment.Update
	for _, b := range nonceBytes {
		u.Nonce = u.Nonce<<8 | uint64(b)
	}
	u.Balance.SetBytes(balance)
	copy(u.CodeHash[:], codeHash)
	return encodeAccountLeaf(&u, storageRoot, dst), nil
}

func ResolveLegacy(key []byte, entries []LegacyEntry) (value []byte, write bool, err error) {
	if len(entries) == 0 {
		return nil, false, nil
	}
	best, keep := 0, false
	for i, e := range entries {
		keep = keep || e.Kind == LegacyKeep
		switch {
		case e.Kind > entries[best].Kind:
			best = i
		case i != best && e.Kind == entries[best].Kind && e.Kind >= LegacyDirect && !bytes.Equal(e.Value, entries[best].Value):
			return nil, false, fmt.Errorf("%w: two kind-%d records for %x", ErrLegacyConflict, e.Kind, key)
		}
	}
	switch b := entries[best]; b.Kind {
	case LegacySynthesized:
		return b.Value, true, nil
	case LegacyDirect:
		if len(b.Value) == 0 && keep {
			return nil, false, fmt.Errorf("%w: %x is tombstoned while an account still points at it", ErrLegacyConflict, key)
		}
		return b.Value, true, nil
	case LegacyKeep:
		return nil, false, nil
	default:
		return nil, true, nil
	}
}

func ConvertLegacyState(value []byte) ([]byte, error) {
	if len(value) < 18 {
		return nil, fmt.Errorf("%w: %d bytes", errLegacyState, len(value))
	}
	txNum := binary.BigEndian.Uint64(value[0:8])
	blockNum := binary.BigEndian.Uint64(value[8:16])
	trieState := value[18:]
	if n := int(binary.BigEndian.Uint16(value[16:18])); n <= len(trieState) {
		trieState = trieState[:n]
	} else {
		return nil, fmt.Errorf("%w: trie state of %d bytes in %d", errLegacyState, n, len(trieState))
	}
	root := empty.RootHash[:]
	if len(trieState) != 0 {
		hph := commitment.NewHexPatriciaHashed(length.Addr, nil, commitment.DefaultTrieConfig())
		defer hph.Release()
		if err := hph.SetState(trieState); err != nil {
			return nil, fmt.Errorf("%w: %w", errLegacyState, err)
		}
		var err error
		if root, err = hph.RootHash(); err != nil {
			return nil, fmt.Errorf("%w: %w", errLegacyState, err)
		}
	}
	return commitment.EncodeCommitmentV4State(root, blockNum, txNum, nil)
}

func checkNibbles(nibbles []byte) error {
	for _, nib := range nibbles {
		if nib > 0x0f {
			return fmt.Errorf("%w: nibble %02x", errLegacyBranch, nib)
		}
	}
	return nil
}
