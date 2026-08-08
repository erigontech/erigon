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
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
)

// stepSource is the abstraction a Domain retire step's collate uses
// to iterate every (key, value) pair that must appear in the new
// step-aligned .kv file. It presents ONE sorted stream of entries;
// callers may compose multiple sources via mergedStepSources so a
// single step's coverage can span MDBX plus any mode-C v4 boundary
// files without collate needing to know each source's format.
//
// Iteration model: after construction Current() returns the first
// entry (or ok=false if empty). Advance() moves to the next entry.
// Close() releases underlying resources; caller must invoke it.
//
// Sorted-by-key is a hard requirement for every implementation —
// mergedStepSources depends on it for its heap-pick.
type stepSource interface {
	Current() (key, value []byte, ok bool)
	Advance() error
	Close()
}

// stepSources is a Close-all convenience for a slice of stepSource.
type stepSources []stepSource

func (ss stepSources) Close() {
	for _, s := range ss {
		if s != nil {
			s.Close()
		}
	}
}

// mdbxLargeValuesStepSource iterates a Domain.ValuesTable (LargeValues=true
// layout) for a single step. Keys in the table are `bareKey +
// stepBytes` where stepBytes = ^binary.BigEndian.Uint64(step). Rows
// tagged with the target step (stepBytes == ^step) are the retire
// candidates; every other row is skipped without disrupting cursor
// order. The bareKey is returned to callers with stepBytes stripped.
type mdbxLargeValuesStepSource struct {
	cursor    kv.Cursor
	stepVal   uint64 // ^step encoded as uint64
	curKey    []byte
	curVal    []byte
	exhausted bool
}

func newMdbxLargeValuesStepSource(cursor kv.Cursor, step kv.Step) (*mdbxLargeValuesStepSource, error) {
	s := &mdbxLargeValuesStepSource{cursor: cursor, stepVal: ^uint64(step)}
	k, v, err := cursor.First()
	if err != nil {
		return nil, err
	}
	return s, s.seekMatching(k, v)
}

func (m *mdbxLargeValuesStepSource) seekMatching(k, v []byte) error {
	for {
		if k == nil {
			m.exhausted = true
			return nil
		}
		if len(k) >= 8 && binary.BigEndian.Uint64(k[len(k)-8:]) == m.stepVal {
			m.curKey = k[:len(k)-8]
			m.curVal = v
			return nil
		}
		var err error
		k, v, err = m.cursor.Next()
		if err != nil {
			return err
		}
	}
}

func (m *mdbxLargeValuesStepSource) Current() (key, value []byte, ok bool) {
	if m.exhausted {
		return nil, nil, false
	}
	return m.curKey, m.curVal, true
}

func (m *mdbxLargeValuesStepSource) Advance() error {
	if m.exhausted {
		return nil
	}
	k, v, err := m.cursor.Next()
	if err != nil {
		return err
	}
	return m.seekMatching(k, v)
}

func (m *mdbxLargeValuesStepSource) Close() {
	if m.cursor != nil {
		m.cursor.Close()
		m.cursor = nil
	}
}

// mdbxDupSortStepSource iterates a Domain.ValuesTable (LargeValues=false
// DupSort layout) for a single step. Rows are `key -> stepBytes+value`
// with stepBytes as the first 8 bytes of the value. Filtering by
// stepBytes == ^step yields the retire candidates; the returned value
// is `value[8:]` (stepBytes stripped).
type mdbxDupSortStepSource struct {
	cursor    kv.CursorDupSort
	stepVal   uint64
	curKey    []byte
	curVal    []byte
	exhausted bool
}

func newMdbxDupSortStepSource(cursor kv.CursorDupSort, step kv.Step) (*mdbxDupSortStepSource, error) {
	s := &mdbxDupSortStepSource{cursor: cursor, stepVal: ^uint64(step)}
	k, v, err := cursor.First()
	if err != nil {
		return nil, err
	}
	return s, s.seekMatching(k, v)
}

func (m *mdbxDupSortStepSource) seekMatching(k, v []byte) error {
	for {
		if k == nil {
			m.exhausted = true
			return nil
		}
		if len(v) >= 8 && binary.BigEndian.Uint64(v[:8]) == m.stepVal {
			m.curKey = k
			m.curVal = v[8:]
			return nil
		}
		var err error
		k, v, err = m.cursor.Next()
		if err != nil {
			return err
		}
	}
}

func (m *mdbxDupSortStepSource) Current() (key, value []byte, ok bool) {
	if m.exhausted {
		return nil, nil, false
	}
	return m.curKey, m.curVal, true
}

func (m *mdbxDupSortStepSource) Advance() error {
	if m.exhausted {
		return nil
	}
	k, v, err := m.cursor.NextNoDup()
	if err != nil {
		return err
	}
	return m.seekMatching(k, v)
}

func (m *mdbxDupSortStepSource) Close() {
	if m.cursor != nil {
		m.cursor.Close()
		m.cursor = nil
	}
}

// v4StepSource iterates a mode-C v4 boundary .kv file's (key, value)
// pairs. The file is produced by WriteStateBoundaryFileV4, whose
// walker yields keys in ascending order and whose emit writes each
// key immediately followed by its value; the same ordering is
// preserved here. Reader compression must match what the file was
// written with (seg.CompressNone in the current v4 emit path).
type v4StepSource struct {
	decomp    *seg.Decompressor
	reader    *seg.Reader
	keyBuf    []byte
	valBuf    []byte
	curKey    []byte
	curVal    []byte
	exhausted bool
	path      string
}

func newV4StepSource(path string, compression seg.FileCompression) (*v4StepSource, error) {
	decomp, err := seg.NewDecompressor(path)
	if err != nil {
		return nil, fmt.Errorf("v4StepSource: open %s: %w", path, err)
	}
	s := &v4StepSource{
		decomp: decomp,
		reader: seg.NewReader(decomp.MakeGetter(), compression),
		path:   path,
	}
	if err := s.Advance(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (v *v4StepSource) Current() (key, value []byte, ok bool) {
	if v.exhausted {
		return nil, nil, false
	}
	return v.curKey, v.curVal, true
}

func (v *v4StepSource) Advance() error {
	if !v.reader.HasNext() {
		v.exhausted = true
		return nil
	}
	v.keyBuf, _ = v.reader.Next(v.keyBuf[:0])
	v.curKey = v.keyBuf
	if !v.reader.HasNext() {
		return fmt.Errorf("v4StepSource: %s: key without paired value at end", v.path)
	}
	v.valBuf, _ = v.reader.Next(v.valBuf[:0])
	v.curVal = v.valBuf
	return nil
}

func (v *v4StepSource) Close() {
	if v.decomp != nil {
		v.decomp.Close()
		v.decomp = nil
	}
}

// mergedStepSources presents a single sorted-by-key stream over a
// slice of stepSource inputs. Duplicate keys across sources are
// deduplicated with priority-by-index: source[0]'s value wins over
// source[1]'s, and so on. Used at retire time so MDBX (post-target
// writes, source[0]) takes precedence over v4 boundary files
// (pre-target snapshot, source[1..]) for the same key.
type mergedStepSources struct {
	sources []stepSource
}

func newMergedStepSources(sources []stepSource) *mergedStepSources {
	return &mergedStepSources{sources: sources}
}

// Next returns the next merged (key, value). ok=false when every
// source is exhausted.
func (m *mergedStepSources) Next() (key, value []byte, ok bool, err error) {
	picked := -1
	for i, s := range m.sources {
		k, _, srcOk := s.Current()
		if !srcOk {
			continue
		}
		if picked == -1 {
			picked = i
			continue
		}
		pk, _, _ := m.sources[picked].Current()
		if bytes.Compare(k, pk) < 0 {
			picked = i
		}
		// bytes.Equal(k, pk): keep picked (lower index → priority).
	}
	if picked == -1 {
		return nil, nil, false, nil
	}
	pk, pv, _ := m.sources[picked].Current()
	keyOut := append([]byte(nil), pk...)
	valOut := append([]byte(nil), pv...)

	// Advance every source whose current key equals the picked key —
	// the priority-winning value is already captured, so lower-priority
	// duplicates get consumed and discarded.
	for i, s := range m.sources {
		k, _, srcOk := s.Current()
		if !srcOk {
			continue
		}
		if !bytes.Equal(k, keyOut) {
			continue
		}
		if aerr := s.Advance(); aerr != nil {
			return nil, nil, false, fmt.Errorf("mergedStepSources: advance source %d: %w", i, aerr)
		}
	}
	return keyOut, valOut, true, nil
}
