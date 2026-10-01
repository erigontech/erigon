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

package pbt

import (
	"bytes"
	"fmt"
	"reflect"

	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func (t *Trie) newVerifier() *Trie {
	verifier := &Trie{
		ctx:                   t.ctx,
		rootKey:               bytes.Clone(t.rootKey),
		rootPath:              t.rootPath,
		bucketMode:            t.bucketMode,
		upperOnly:             t.upperOnly,
		suppressRoot:          t.suppressRoot,
		suppressBucketRecords: t.suppressBucketRecords,
		rows:                  make(map[string]*rowNode),
		dirtyRows:             make(map[string]*rowNode),
		bucketDirty:           make(map[string][]byte),
		mergeCreatedStems:     make(map[string]struct{}),
		verifyOnly:            true,
	}
	if t.ownedPrefix != nil {
		prefix := *t.ownedPrefix
		verifier.ownedPrefix = &prefix
	}
	if len(t.upperStops) != 0 {
		verifier.upperStops = append([]eip8297.Bitpath(nil), t.upperStops...)
	}
	return verifier
}

func (t *Trie) Verify() error {
	if t.ctx == nil {
		return fmt.Errorf("nil Patricia context")
	}
	return t.newVerifier().verify()
}

func (t *Trie) verify() error {
	root, err := t.loadRoot()
	if err != nil {
		return err
	}
	if root.row == nil && root.form == RowRoot {
		return t.verifyBuckets()
	}
	var verifyErr error
	switch root.form {
	case LeafRoot:
		verifyErr = t.verifyRootRecord()
	case ExtRoot:
		if err := t.verifyRootRecord(); err != nil {
			verifyErr = err
			break
		}
		row, err := t.extTopRow(root)
		if err != nil {
			verifyErr = err
			break
		}
		result, err := t.verifyRow(row)
		if err != nil {
			verifyErr = err
			break
		}
		if result.Split != root.self.BitLen || result.Left != root.left || result.Right != root.right {
			verifyErr = fmt.Errorf("root extension does not match its top row")
			break
		}
		wantSelf, err := rowTopPrefix(row, result.Split)
		if err != nil {
			verifyErr = err
			break
		}
		if root.self != wantSelf {
			verifyErr = fmt.Errorf("root extension prefix does not match its top row")
		}
	case RowRoot:
		_, verifyErr = t.verifyRow(root.row)
	default:
		verifyErr = fmt.Errorf("unknown root form %d", root.form)
	}
	if verifyErr != nil {
		return verifyErr
	}
	return t.verifyBuckets()
}

func (t *Trie) verifyRootRecord() error {
	record := t.rootRecord()
	rootKey := t.rootRecordKey()
	data, err := EncodeRecord(rootKey, &record)
	if err != nil {
		return err
	}
	if !bytes.Equal(data, t.root.raw) {
		return fmt.Errorf("root record is not canonical")
	}
	decoded, err := DecodeRecord(rootKey, data)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(record, decoded) {
		return fmt.Errorf("root record round trip changed its value")
	}
	return nil
}

func (t *Trie) verifyRow(row *rowNode) (FoldResult, error) {
	record := row.record()
	data, err := EncodeRecord(row.key, &record)
	if err != nil {
		return FoldResult{}, err
	}
	if len(row.raw) != 0 && !bytes.Equal(data, row.raw) && !row.dirty {
		return FoldResult{}, fmt.Errorf("stored row %x changed without a mutation", row.key)
	}
	decoded, err := DecodeRecord(row.key, data)
	if err != nil {
		return FoldResult{}, err
	}
	if !reflect.DeepEqual(record, decoded) {
		return FoldResult{}, fmt.Errorf("row %x is not canonical", row.key)
	}
	for slot := range row.cells {
		cell := row.cell(slot)
		if cell.Kind != BranchCell {
			continue
		}
		child, err := t.loadBranchChild(row, slot)
		if err != nil {
			return FoldResult{}, err
		}
		childResult, err := t.verifyRow(child)
		if err != nil {
			return FoldResult{}, err
		}
		full, err := rowTopPrefix(child, childResult.Split)
		if err != nil {
			return FoldResult{}, err
		}
		wantPrefix := full.Slice(row.path.BitLen+4, childResult.Split)
		if cell.Prefix != wantPrefix || cell.Left != childResult.Left || cell.Right != childResult.Right {
			return FoldResult{}, fmt.Errorf("row %x cell %d does not match its child", row.key, slot)
		}
		t.releaseVerifiedChild(cell, child)
	}
	return rowFoldResult(row)
}

func (t *Trie) verifyBuckets() error {
	if _, err := t.expectedBucketRecords(); err != nil {
		return err
	}
	if verifier, ok := t.ctx.(pbinBucketVerifier); ok {
		return verifier.PBinCheckBucketRecords()
	}
	return nil
}

type pbinBucketVerifier interface {
	PBinResetBucketKeys()
	PBinMarkBucketKey([]byte)
	PBinCheckBucketRecords() error
}

type pbinVerifierReleaseObserver interface {
	PBinObserveReleasedChild(*rowNode)
}

func (t *Trie) releaseVerifiedChild(cell *rowCell, child *rowNode) {
	cell.child = nil
	child.parent = nil
	if observer, ok := t.ctx.(pbinVerifierReleaseObserver); ok {
		observer.PBinObserveReleasedChild(child)
	}
}

func (t *Trie) verifyBucketRecord(key []byte, descriptor bucketDescriptor) error {
	data, _, err := t.ctx.Branch(key)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return fmt.Errorf("bucket record %x is missing", key)
	}
	record := descriptor.record()
	wantData, err := EncodeRecord(key, &record)
	if err != nil {
		return err
	}
	if !bytes.Equal(data, wantData) {
		return fmt.Errorf("bucket record %x does not match its upper cell", key)
	}
	if _, err := DecodeRecord(key, data); err != nil {
		return err
	}
	return nil
}
