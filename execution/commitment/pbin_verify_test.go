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

package commitment

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// pbinVerifier rebuilds the tree from the records the engine wrote, reading
// nothing out of the engine's own cells. Where the oracle answers "is this the
// right root for these leaves", this answers "is what landed in the database the
// tree that root came from".
//
// It returns errors rather than failing the test directly, so a test can also
// pin that a corrupted record is caught.
type pbinVerifier struct {
	t  *testing.T
	ms *MockState
}

var (
	errPBinVerifyNoRecords = errors.New("pbin verify: no branch records")
	errPBinVerifyPosition  = errors.New("pbin verify: leaf sits where its key does not")
)

// recordPaths decodes the key of every live node record. A record with no data
// is a deletion and names no node; the root cell record is keyed outside the
// bit-path space and is read through rootCell.
func (v *pbinVerifier) recordPaths() ([]pbinBitpath, error) {
	paths := make([]pbinBitpath, 0, len(v.ms.cm))
	for key, data := range v.ms.cm {
		if len(data) == 0 || key == string(pbinRootKey) {
			continue
		}
		p, err := pbinDecodeBitPath([]byte(key))
		if err != nil {
			return nil, fmt.Errorf("pbin verify: record key %x: %w", key, err)
		}
		paths = append(paths, p)
	}
	return paths, nil
}

// rootPath is the record with no record above it. Records are keyed by the full
// descent path, so one record's path being a bit-prefix of another's is exactly
// the ancestor relation.
func (v *pbinVerifier) rootPath() (pbinBitpath, error) {
	paths, err := v.recordPaths()
	if err != nil {
		return pbinBitpath{}, err
	}
	if len(paths) == 0 {
		return pbinBitpath{}, errPBinVerifyNoRecords
	}
	var roots []pbinBitpath
	for _, p := range paths {
		covered := false
		for _, q := range paths {
			if q.bitLen < p.bitLen && p.hasPrefix(&q) {
				covered = true
				break
			}
		}
		if !covered {
			roots = append(roots, p)
		}
	}
	if len(roots) != 1 {
		return pbinBitpath{}, fmt.Errorf("pbin verify: %d of %d records have no ancestor, want 1", len(roots), len(paths))
	}
	return roots[0], nil
}

func (v *pbinVerifier) rootCell() (pbinCell, error) {
	var c pbinCell
	data, _, err := v.ms.Branch(pbinRootKey)
	if err != nil {
		return c, err
	}
	if len(data) == 0 {
		return c, errPBinVerifyNoRecords
	}
	pos, err := pbinDecodeCell(data, 0, &c)
	if err != nil {
		return c, fmt.Errorf("pbin verify: root cell: %w", err)
	}
	if pos != len(data) {
		return c, fmt.Errorf("pbin verify: %d trailing bytes after the root cell", len(data)-pos)
	}
	return c, nil
}

// recomputeRoot hashes the record set bottom up, entering at the stored root
// cell rather than guessing which record has no ancestor.
func (v *pbinVerifier) recomputeRoot() ([]byte, error) {
	c, err := v.rootCell()
	if err != nil {
		return nil, err
	}
	var start pbinBitpath
	return v.cellHash(&start, &c)
}

func (v *pbinVerifier) nodeHash(nodePath, prefix *pbinBitpath) ([]byte, error) {
	cells, err := v.recordAt(nodePath)
	if err != nil {
		return nil, err
	}
	var children [2][]byte
	for bit := range children {
		start := *nodePath
		start.appendBit(uint64(bit))
		if children[bit], err = v.cellHash(&start, &cells[bit]); err != nil {
			return nil, err
		}
	}
	return pbinTestKeccak(v.t, []byte{pbinBranchTag},
		pbinOracleEncodeBitPrefix(pbinVerifyBits(prefix)), children[0], children[1]), nil
}

func (v *pbinVerifier) cellHash(start *pbinBitpath, c *pbinCell) ([]byte, error) {
	switch c.kind {
	case pbinNodeLeaf:
		key, value, err := v.leaf(start, c)
		if err != nil {
			return nil, err
		}
		return pbinTestKeccak(v.t, []byte{pbinLeafTag}, key, value), nil
	case pbinNodeBranch:
		nodePath := *start
		nodePath.append(&c.prefix)
		return v.nodeHash(&nodePath, &c.prefix)
	default:
		return nil, fmt.Errorf("pbin verify: cell at %d bits has no node kind", start.bitLen)
	}
}

// leaf resolves a leaf cell to the key its position spells and the value its
// plain key holds in state.
func (v *pbinVerifier) leaf(start *pbinBitpath, c *pbinCell) (key, value []byte, err error) {
	full := *start
	full.append(&c.prefix)
	if full.bitLen != pbinAccountKeyLength*8 && full.bitLen != pbinStorageKeyLength*8 {
		return nil, nil, fmt.Errorf("pbin verify: leaf key of %d bits is neither zone length", full.bitLen)
	}
	key = pbinVerifyPackBits(pbinVerifyBits(&full))

	update, err := v.plainState(c)
	if err != nil {
		return nil, nil, err
	}
	encoded, err := pbinLeafValue(key, update)
	if err != nil {
		return nil, nil, err
	}
	return key, encoded[:], nil
}

func (v *pbinVerifier) plainState(c *pbinCell) (*Update, error) {
	switch {
	case c.accountAddrLen > 0 && c.storageAddrLen > 0:
		return nil, errors.New("pbin verify: leaf carries both an account and a storage plain key")
	case c.accountAddrLen > 0:
		return v.ms.Account(c.accountAddr[:c.accountAddrLen])
	case c.storageAddrLen > 0:
		return v.ms.Storage(c.storageAddr[:c.storageAddrLen])
	default:
		// A code chunk has no plain key and no state behind it: the record is the
		// only place its value exists, so the check is that it round-tripped.
		return &c.Update, nil
	}
}

func (v *pbinVerifier) recordAt(nodePath *pbinBitpath) ([2]pbinCell, error) {
	var cells [2]pbinCell
	key := pbinEncodeBitPath(nodePath)
	data, _, err := v.ms.Branch(key)
	if err != nil {
		return cells, err
	}
	if len(data) == 0 {
		return cells, fmt.Errorf("pbin verify: no record for the %d-bit node at %x", nodePath.bitLen, key)
	}
	_, afterMap, err := pbinDecodeBranch(data, &cells)
	if err != nil {
		return cells, fmt.Errorf("pbin verify: record at %x: %w", key, err)
	}
	if afterMap != pbinCellBits {
		return cells, fmt.Errorf("pbin verify: record at %x keeps %02b of its children, want both", key, afterMap)
	}
	return cells, nil
}

// checkPlainKeys asserts every stored leaf sits where its own key derivation puts
// it: the record's path, the child bit and the cell's prefix must spell exactly
// treeKey(plainKey). A slot routed into the wrong zone still builds a tree that
// hashes consistently, so position against derivation is what catches it.
func (v *pbinVerifier) checkPlainKeys() (int, error) {
	root, err := v.rootCell()
	if err != nil {
		return 0, err
	}
	leaves := 0
	if root.kind == pbinNodeLeaf {
		var start pbinBitpath
		if err = v.checkLeafPosition(&start, &root); err != nil {
			return 0, err
		}
		leaves++
	}
	paths, err := v.recordPaths()
	if err != nil {
		return 0, err
	}
	for _, path := range paths {
		cells, err := v.recordAt(&path)
		if err != nil {
			return 0, err
		}
		for bit := range cells {
			c := &cells[bit]
			if c.kind != pbinNodeLeaf {
				continue
			}
			start := path
			start.appendBit(uint64(bit))
			if err = v.checkLeafPosition(&start, c); err != nil {
				return 0, err
			}
			leaves++
		}
	}
	return leaves, nil
}

func (v *pbinVerifier) checkLeafPosition(start *pbinBitpath, c *pbinCell) error {
	key, _, err := v.leaf(start, c)
	if err != nil {
		return err
	}
	want, err := pbinVerifyDerivedKey(c, key)
	if err != nil {
		return err
	}
	if !bytes.Equal(want, key) {
		return fmt.Errorf("%w: stored at %x, derives %x", errPBinVerifyPosition, key, want)
	}
	return nil
}

// pbinVerifyDerivedKey re-derives a leaf's tree key from its plain key. The
// sub-index comes from the stored key because the two account-header leaves share
// one address; which of the two it is, the record does not say.
func pbinVerifyDerivedKey(c *pbinCell, key []byte) ([]byte, error) {
	switch {
	case c.accountAddrLen > 0:
		if len(key) != pbinAccountKeyLength {
			return nil, fmt.Errorf("pbin verify: account leaf key of %d bytes, want %d", len(key), pbinAccountKeyLength)
		}
		subIndex := key[pbinAccountKeyLength-1]
		if subIndex != pbinBasicDataLeafKey && subIndex != pbinCodeHashLeafKey {
			return nil, fmt.Errorf("pbin verify: account leaf at sub-index %d is neither header leaf", subIndex)
		}
		return pbinTreeKeyAccount(c.accountAddr[:c.accountAddrLen], subIndex), nil
	case c.storageAddrLen > 0:
		addr, slot := c.storageAddr[:length.Addr], c.storageAddr[length.Addr:c.storageAddrLen]
		return pbinTreeKeyStorage(addr, slot), nil
	default:
		// A record-resident leaf holds no plain key to re-derive from, so what is
		// checked is where it may sit: only a code chunk carries its own value, and
		// chunks live in the code zone — never in the account or storage zone.
		if len(key) != pbinCodeKeyLength || key[0] != pbinCodeZone {
			return nil, fmt.Errorf("%w: value-carrying leaf at %x is no code chunk", errPBinVerifyPosition, key)
		}
		return key, nil
	}
}

// pbinVerifyBits spells a path one bit per byte, the shape the oracle's
// encode_bit_prefix takes.
func pbinVerifyBits(p *pbinBitpath) []byte {
	out := make([]byte, p.bitLen)
	for i := range out {
		out[i] = byte(p.bit(int16(i)))
	}
	return out
}

func pbinVerifyPackBits(bits []byte) []byte {
	out := make([]byte, (len(bits)+7)/8)
	for i, b := range bits {
		out[i/8] |= b << (7 - i%8)
	}
	return out
}

// pbinTestVerifyRecords requires the records to rebuild the root the engine
// returned, with every leaf they hold sitting at its own key.
func pbinTestVerifyRecords(t *testing.T, ms *MockState, root []byte, wantLeaves int) {
	t.Helper()
	v := &pbinVerifier{t: t, ms: ms}

	recomputed, err := v.recomputeRoot()
	require.NoError(t, err)
	require.Equal(t, root, recomputed, "records do not rebuild the engine's root")

	leaves, err := v.checkPlainKeys()
	require.NoError(t, err)
	require.Equal(t, wantLeaves, leaves)
}

// The recompute relies on this shape: one record has no ancestor, and its path
// is the root node's prefix.
func TestPBinVerifyRootRecordIsUnique(t *testing.T) {
	t.Parallel()

	corpus := new(pbinTestCorpus).
		storage(pbinOracleAddr(54), pbinOracleSlot(256), 0x01).
		storage(pbinOracleAddr(54), pbinOracleSlot(257), 0x02)

	pph, ms := pbinTestEngine(t)
	require.NoError(t, ms.applyPlainUpdates(corpus.plainKeys, corpus.updates))
	pbinTestProcess(t, pph, corpus.plainKeys, corpus.updates)

	v := &pbinVerifier{t: t, ms: ms}
	root, err := v.rootPath()
	require.NoError(t, err)
	require.Equal(t, pph.grid.root.prefix, root, "the root record's path is the root node's prefix")
}

// A bare-leaf root writes no node record and is still recoverable, because the
// root cell record carries it.
func TestPBinVerifySingleLeafRoot(t *testing.T) {
	t.Parallel()

	corpus := new(pbinTestCorpus).storage(pbinOracleAddr(55), pbinOracleSlot(1000), 0x01)
	pph, ms := pbinTestEngine(t)
	require.NoError(t, ms.applyPlainUpdates(corpus.plainKeys, corpus.updates))
	root := pbinTestProcess(t, pph, corpus.plainKeys, corpus.updates)

	v := &pbinVerifier{t: t, ms: ms}
	paths, err := v.recordPaths()
	require.NoError(t, err)
	require.Empty(t, paths, "a bare-leaf root has no node record")

	pbinTestVerifyRecords(t, ms, root, 1)
}

func TestPBinVerifyEmptyStateHasNoRecords(t *testing.T) {
	t.Parallel()

	v := &pbinVerifier{t: t, ms: NewMockState(t)}
	_, err := v.recomputeRoot()
	require.ErrorIs(t, err, errPBinVerifyNoRecords)
}

// Swapping a record's two children moves each leaf to a position its key does
// not spell, which the plain-key check must reject and the recompute must no
// longer reproduce. Without it both checks could be vacuous.
func TestPBinVerifyCatchesSwappedCells(t *testing.T) {
	t.Parallel()

	corpus := new(pbinTestCorpus).
		storage(pbinOracleAddr(56), pbinOracleSlot(256), 0x01).
		storage(pbinOracleAddr(56), pbinOracleSlot(257), 0x02)

	pph, ms := pbinTestEngine(t)
	require.NoError(t, ms.applyPlainUpdates(corpus.plainKeys, corpus.updates))
	root := pbinTestProcess(t, pph, corpus.plainKeys, corpus.updates)
	pbinTestVerifyRecords(t, ms, root, len(corpus.entries(t)))

	v := &pbinVerifier{t: t, ms: ms}
	path, err := v.rootPath()
	require.NoError(t, err)
	cells, err := v.recordAt(&path)
	require.NoError(t, err)

	cells[0], cells[1] = cells[1], cells[0]
	var enc pbinBranchEncoder
	swapped, err := enc.encode(pbinCellBits, pbinCellBits, &cells)
	require.NoError(t, err)
	require.NoError(t, ms.PutBranch(pbinEncodeBitPath(&path), bytes.Clone(swapped), nil))

	_, err = v.checkPlainKeys()
	require.ErrorIs(t, err, errPBinVerifyPosition)

	recomputed, err := v.recomputeRoot()
	require.NoError(t, err)
	require.NotEqual(t, root, recomputed)
}
