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

package state

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon/db/kv"
)

// DomainFileFamily is a coupled set of on-disk files that together form
// one step-range slice of a domain: the .kv values file (plus its
// .bt / .kvi / .kvei accessors), the .ef inverted index (plus .efi),
// and the .v history values (plus .vi). All three FilesItem members
// share the same [startTxNum, endTxNum) range — a partial or
// range-inconsistent family is a corruption invariant break, and the
// constructor refuses to build one.
type DomainFileFamily struct {
	Domain     kv.Domain
	StartTxNum uint64
	EndTxNum   uint64

	Values  *FilesItem // .kv + accessors (.bt, .kvi, .kvei)
	Index   *FilesItem // .ef + .efi
	History *FilesItem // .v + .vi
}

// ErrFileFamilyRangeMismatch is returned by NewDomainFileFamily when the
// three FilesItem members do not share the same [startTxNum, endTxNum).
var ErrFileFamilyRangeMismatch = errors.New("DomainFileFamily: member range mismatch")

// ErrFileFamilyMissingMember is returned by NewDomainFileFamily when a
// required FilesItem member is nil.
var ErrFileFamilyMissingMember = errors.New("DomainFileFamily: missing required member")

// NewDomainFileFamily constructs a family from the three FilesItems that
// together cover one step range of a domain. All three must be non-nil
// and share the same [startTxNum, endTxNum) — otherwise construction
// fails and the caller must Close the members themselves. This is the
// only supported way to build a DomainFileFamily; direct struct
// initialization bypasses the invariant check and should not be used
// outside tests that specifically exercise the invariant.
func NewDomainFileFamily(dom kv.Domain, values, index, history *FilesItem) (*DomainFileFamily, error) {
	if values == nil {
		return nil, fmt.Errorf("%w: values (.kv)", ErrFileFamilyMissingMember)
	}
	if index == nil {
		return nil, fmt.Errorf("%w: index (.ef)", ErrFileFamilyMissingMember)
	}
	if history == nil {
		return nil, fmt.Errorf("%w: history (.v)", ErrFileFamilyMissingMember)
	}
	if values.startTxNum != index.startTxNum || values.startTxNum != history.startTxNum ||
		values.endTxNum != index.endTxNum || values.endTxNum != history.endTxNum {
		return nil, fmt.Errorf("%w: values=[%d,%d) index=[%d,%d) history=[%d,%d)",
			ErrFileFamilyRangeMismatch,
			values.startTxNum, values.endTxNum,
			index.startTxNum, index.endTxNum,
			history.startTxNum, history.endTxNum)
	}
	return &DomainFileFamily{
		Domain:     dom,
		StartTxNum: values.startTxNum,
		EndTxNum:   values.endTxNum,
		Values:     values,
		Index:      index,
		History:    history,
	}, nil
}

// Range returns the [startTxNum, endTxNum) covered by every member of
// this family.
func (f *DomainFileFamily) Range() (startTxNum, endTxNum uint64) {
	return f.StartTxNum, f.EndTxNum
}

// StepRange returns the family's txN range converted to steps. See the
// caveats on FilesItem.StepRange for how the range is calculated.
func (f *DomainFileFamily) StepRange(stepSize uint64) (fromStep, toStep kv.Step) {
	return kv.Step(f.StartTxNum / stepSize), kv.Step(f.EndTxNum / stepSize)
}

// Paths returns every on-disk path across every member of the family,
// each made relative to basePath. Order is values → index → history.
func (f *DomainFileFamily) Paths(basePath string) []string {
	out := make([]string, 0, 8)
	out = append(out, f.Values.FilePaths(basePath)...)
	out = append(out, f.Index.FilePaths(basePath)...)
	out = append(out, f.History.FilePaths(basePath)...)
	return out
}

// Close closes every member's underlying files. Idempotent — safe to
// call after CloseAndRemove.
func (f *DomainFileFamily) Close() {
	if f == nil {
		return
	}
	f.Values.closeFiles()
	f.Index.closeFiles()
	f.History.closeFiles()
}

// CloseAndRemove closes and unlinks every member from disk. Best-effort
// on remove errors — logs and continues so a partial removal doesn't
// leave the family in a limbo state.
func (f *DomainFileFamily) CloseAndRemove() {
	if f == nil {
		return
	}
	f.Values.closeFilesAndRemove()
	f.Index.closeFilesAndRemove()
	f.History.closeFilesAndRemove()
}
