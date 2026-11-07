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

package protocol

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"slices"

	"github.com/erigontech/erigon/common"
)

// ErrTables reports an L1 settlement entry and the L2 tables realizing it
// that do not execute as one another.
var ErrTables = errors.New("settlement and L2 tables differ")

// This file ports entries/verify.rs of eez-rollupX d0130ab9: the inverse of
// the materializer. Rows are consumed in forward order, their position keys
// select the next call of the other chain's current array, and both chains'
// rolling hashes advance as execution alternates between them.

const maxVerifyDepth = 1024

func failTables(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrTables, fmt.Sprintf(format, args...))
}

// VerifyInbound checks the settlement entry of an L1 call to rollupID
// against its delivery entry, both carrying the call's hash, and returns
// the calls the delivery makes from the rollup to L1, in order, that
// commit.
func VerifyInbound(l1Entry, delivery *ExecutionEntry, rollupID uint64) ([]common.Hash, error) {
	if rollupID == MainnetRollupID {
		return nil, failTables("rollup id 0")
	}
	if err := checkEntryShape(l1Entry); err != nil {
		return nil, err
	}
	if err := checkEntryShape(delivery); err != nil {
		return nil, err
	}
	switch {
	case len(l1Entry.RollupUpdates) == 0:
		return nil, failTables("L1 entry without root updates")
	case len(delivery.RollupUpdates) > 0:
		return nil, failTables("delivery with root updates")
	case l1Entry.ProxyEntryHash == (common.Hash{}) || l1Entry.DestinationRollupID != rollupID ||
		delivery.DestinationRollupID != rollupID || l1Entry.ProxyEntryHash != delivery.ProxyEntryHash:
		return nil, failTables("inbound routing differs")
	}
	l1 := newChainState(l1Entry, MainnetRollupID, L1EntrySeed(l1Entry.RollupUpdates, l1Entry.ProxyEntryHash))
	l2 := newChainState(delivery, rollupID, L2EntrySeed(delivery.ProxyEntryHash))
	f := &facts{}
	root, ok := l2.take()
	if !ok {
		return nil, failTables("delivery without a root call")
	}
	h, err := chainCallHash(&root, MainnetRollupID, rollupID)
	if err != nil {
		return nil, err
	}
	if h != l1Entry.ProxyEntryHash {
		return nil, failTables("inbound root differs from the entry's call")
	}
	if err := verifyCall(l2, l1, root, l1Entry.Success, l1Entry.ReturnData, f, 0); err != nil {
		return nil, err
	}
	matches := delivery.Success && ((l1Entry.Success && bytes.Equal(delivery.ReturnData, l1Entry.ReturnData)) || (!l1Entry.Success && len(delivery.ReturnData) == 0))
	if !matches {
		return nil, failTables("delivery outcome differs from the L1 root's")
	}
	if err := l1.finish(); err != nil {
		return nil, err
	}
	if err := l2.finish(); err != nil {
		return nil, err
	}
	return f.outbound, nil
}

// VerifyOutbound checks an outbound settlement host against the source
// entries of the L2 transaction it settles, mutable ones then static
// reads, and returns the calls the transaction makes from the rollup to L1,
// in order, that commit. Static roots are lookups: they take no cursor slot
// and emit no call.
func VerifyOutbound(host *ExecutionEntry, mutable []ExecutionEntry, statics []L2StaticExecutionEntry, rollupID uint64) ([]common.Hash, error) {
	if rollupID == MainnetRollupID {
		return nil, failTables("rollup id 0")
	}
	if len(mutable)+len(statics) == 0 {
		return nil, failTables("outbound host without source entries")
	}
	if err := checkEntryShape(host); err != nil {
		return nil, err
	}
	for i := range mutable {
		if err := checkEntryShape(&mutable[i]); err != nil {
			return nil, err
		}
		if len(mutable[i].RollupUpdates) > 0 {
			return nil, failTables("source entry %d with root updates", i)
		}
	}
	if len(statics) > 0 {
		// Host rows then answer static callbacks alone, and mutable sources
		// stay flat, successful and outside revert windows.
		for i := range mutable {
			if len(mutable[i].L2ToL1Calls) > 0 || len(mutable[i].ExpectedL1ToL2Calls) > 0 || !mutable[i].Success {
				return nil, failTables("mixed outbound transaction with a nested or failed mutable source")
			}
		}
		if !host.Success || slices.ContainsFunc(host.L2ToL1Calls, func(c CrossChainCall) bool { return c.RevertNextNCalls != 0 }) {
			return nil, failTables("mixed outbound transaction with a failed host or a revert window")
		}
	}
	type key struct {
		index string
		hash  common.Hash
	}
	results := map[key]struct {
		success bool
		data    []byte
	}{}
	for i := range statics {
		s := &statics[i]
		if err := checkL2StaticEntryShape(s); err != nil {
			return nil, err
		}
		k := key{s.ExpectedEntryIndex.String(), s.ProxyEntryHash}
		if first, ok := results[k]; ok && (first.success != s.Success || !bytes.Equal(first.data, s.ReturnData)) {
			return nil, failTables("static reads disagree on lookup %v", k)
		}
		results[k] = struct {
			success bool
			data    []byte
		}{s.Success, s.ReturnData}
	}
	switch {
	case len(host.RollupUpdates) == 0:
		return nil, failTables("outbound host without root updates")
	case host.ProxyEntryHash != (common.Hash{}) || host.DestinationRollupID != rollupID:
		return nil, failTables("outbound host routing differs")
	case len(host.ReturnData) > 0:
		return nil, failTables("outbound host with return data")
	}
	counts, err := OutboundRootCounts([]ExecutionEntry{*host})
	if err != nil {
		return nil, err
	}
	if counts[0] != len(mutable)+len(statics) {
		return nil, failTables("host carries %d source roots, the transaction %d entries", counts[0], len(mutable)+len(statics))
	}
	l1 := newChainState(host, MainnetRollupID, L1EntrySeed(host.RollupUpdates, host.ProxyEntryHash))
	f := &facts{}
	cursor, staticCursor := 0, 0
	seen := make([]bool, len(mutable))
	for call := 0; call < len(host.L2ToL1Calls); {
		marker, _ := l1.peek()
		span := int(marker.RevertNextNCalls)
		end := call + max(span, 1)
		if end > len(host.L2ToL1Calls) {
			return nil, failTables("root %d revert span %d exceeds the host", call, span)
		}
		var saved *facts
		if span != 0 {
			saved = f.clone()
		}
		savedCursor := cursor
		for root := call; root < end; root++ {
			if next, _ := l1.peek(); next.IsStatic {
				if staticCursor >= len(statics) {
					return nil, failTables("static root %d without a source read", root)
				}
				s := &statics[staticCursor]
				r, _ := l1.take()
				h, err := chainCallHash(&r, rollupID, MainnetRollupID)
				if err != nil {
					return nil, err
				}
				if s.ExpectedEntryIndex.Cmp(big.NewInt(int64(cursor))) != 0 || r.RevertNextNCalls != 0 || r.Gas != 0 || r.Value.Sign() != 0 || h != s.ProxyEntryHash {
					return nil, failTables("static root %d differs from its source read", root)
				}
				l1.rolling.CallBegin(h)
				if err := resolveStaticCallbacks(l1, s, rollupID); err != nil {
					return nil, err
				}
				l1.rolling.CallEnd(s.Success, s.ReturnData)
				staticCursor++
				continue
			}
			if cursor >= len(mutable) {
				return nil, failTables("root %d beyond %d source slots", root, len(mutable))
			}
			source := &mutable[cursor]
			seen[cursor] = true
			if source.DestinationRollupID != MainnetRollupID {
				return nil, failTables("source entry %d routing differs", cursor)
			}
			r, _ := l1.take()
			if root != call && r.RevertNextNCalls != 0 {
				return nil, failTables("root %d starts a nested revert window", root)
			}
			h, err := chainCallHash(&r, rollupID, MainnetRollupID)
			if err != nil {
				return nil, err
			}
			if h != source.ProxyEntryHash {
				return nil, failTables("root %d differs from its source entry", root)
			}
			var reverted *facts
			if !source.Success {
				reverted = f.clone()
			}
			if err := f.value(rollupID, r.Value); err != nil {
				return nil, err
			}
			f.outbound = append(f.outbound, h)
			state := newChainState(source, rollupID, L2EntrySeed(source.ProxyEntryHash))
			if err := verifyCall(l1, state, r, source.Success, source.ReturnData, f, 0); err != nil {
				return nil, err
			}
			if err := state.finish(); err != nil {
				return nil, err
			}
			if reverted != nil {
				*f = *reverted
			}
			cursor++
		}
		if saved != nil {
			*f = *saved
			cursor = savedCursor
		}
		call = end
	}
	if slices.Contains(seen, false) || staticCursor != len(statics) {
		return nil, failTables("source entries left unused")
	}
	if err := l1.finish(); err != nil {
		return nil, err
	}
	return f.outbound, nil
}

// resolveStaticCallbacks binds a static read's callbacks to the host rows
// that answer them: each row is keyed by the callback and the host's rolling
// hash, which static rows leave unchanged, so repeats share a key and must
// share a result; the rows' results fold to the read's rolling hash.
func resolveStaticCallbacks(l1 *chainState, s *L2StaticExecutionEntry, rollupID uint64) error {
	var results common.Hash
	resolved := map[common.Hash][]byte{}
	for i := range s.IncomingCalls {
		c := s.IncomingCalls[i]
		h, err := chainCallHash(&c, MainnetRollupID, rollupID)
		if err != nil {
			return err
		}
		row, ok := l1.nextRow()
		if !ok {
			return failTables("static callback %d without a host row", i)
		}
		if !c.IsStatic || row.Hash != positionKey(h, l1.rolling.Sum()) || !row.Success || len(row.Calls) > 0 || row.RevertedOrStaticRollingHash != (common.Hash{}) {
			return failTables("static callback %d differs from its host row", i)
		}
		if first, ok := resolved[row.Hash]; ok && !bytes.Equal(first, row.ReturnData) {
			return failTables("static callback %d repeats a lookup with another result", i)
		}
		resolved[row.Hash] = row.ReturnData
		l1.markStaticRow()
		results = StaticResult(results, row.Success, row.ReturnData)
	}
	if results != s.RollingHash {
		return failTables("static callback results differ from the read's rolling hash")
	}
	return nil
}

func verifyCall(destination, source *chainState, call CrossChainCall, success bool, returnData []byte, f *facts, depth int) error {
	if depth > maxVerifyDepth {
		return failTables("nesting deeper than %d", maxVerifyDepth)
	}
	h, err := chainCallHash(&call, source.chain, destination.chain)
	if err != nil {
		return err
	}
	destination.rolling.CallBegin(h)
	for {
		next, ok := source.peek()
		if !ok {
			break
		}
		span := int(next.RevertNextNCalls)
		n := max(span, 1)
		if n > source.remaining() {
			return failTables("revert span %d exceeds its array", span)
		}
		type saved struct {
			rolling RollingHash
			row     int
			cursors []int
			facts   *facts
		}
		var region *saved
		if span != 0 {
			region = &saved{destination.rolling, destination.next, slices.Clone(destination.cursors), f.clone()}
		}
		matched := true
		for k := range n {
			if cur, _ := source.peek(); k != 0 && cur.RevertNextNCalls != 0 {
				return failTables("revert windows overlap")
			}
			ok, err := verifyNestedCall(destination, source, f, depth)
			if err != nil {
				return err
			}
			if !ok {
				if span != 0 {
					return failTables("revert window call without its row")
				}
				matched = false
				break
			}
		}
		if region != nil {
			destination.rolling, destination.next, destination.cursors = region.rolling, region.row, region.cursors
			*f = *region.facts
		} else if !matched {
			break
		}
	}
	destination.rolling.CallEnd(success, returnData)
	return nil
}

func verifyNestedCall(destination, source *chainState, f *facts, depth int) (bool, error) {
	next, ok := source.peek()
	if !ok {
		return false, nil
	}
	row, ok := destination.nextRow()
	if !ok {
		return false, nil
	}
	// The executing chain makes the call; the other chain's array lists it.
	h, err := chainCallHash(&next, destination.chain, source.chain)
	if err != nil {
		return false, err
	}
	if row.Hash != positionKey(h, destination.rolling.Sum()) {
		return false, nil
	}
	if next.IsStatic {
		if next.Value.Sign() != 0 || next.RevertNextNCalls != 0 || next.Gas != 0 || len(row.Calls) > 0 || row.RevertedOrStaticRollingHash != (common.Hash{}) {
			return false, failTables("static row of an unsupported shape")
		}
		destination.markStaticRow()
		source.take()
		source.rolling.CallBegin(h)
		source.rolling.CallEnd(row.Success, row.ReturnData)
		return true, nil
	}
	index := destination.openRow(h)
	call, _ := source.take()
	saved := f.clone()
	if err := f.value(destination.chain, call.Value); err != nil {
		return false, err
	}
	if destination.chain != MainnetRollupID {
		f.outbound = append(f.outbound, h)
	}
	if err := verifyCall(source, destination, call, row.Success, row.ReturnData, f, depth+1); err != nil {
		return false, err
	}
	if err := destination.closeRow(index, row.Success); err != nil {
		return false, err
	}
	if !row.Success {
		*f = *saved
	}
	return true, nil
}

// chainState walks one chain's entry: its row cursor, the open rows, and a
// call cursor per call array.
type chainState struct {
	entry   *ExecutionEntry
	chain   uint64
	rolling RollingHash
	next    int
	frames  []int
	saved   []int
	hashes  []RollingHash
	seen    []bool
	cursors []int
}

func newChainState(e *ExecutionEntry, chain uint64, rolling RollingHash) *chainState {
	return &chainState{
		entry:   e,
		chain:   chain,
		rolling: rolling,
		seen:    make([]bool, len(e.ExpectedL1ToL2Calls)),
		cursors: make([]int, len(e.ExpectedL1ToL2Calls)+1),
	}
}

func (s *chainState) slot() int {
	if k := len(s.frames); k > 0 {
		return s.frames[k-1] + 1
	}
	return 0
}

func (s *chainState) array() []CrossChainCall {
	if k := len(s.frames); k > 0 {
		return s.entry.ExpectedL1ToL2Calls[s.frames[k-1]].Calls
	}
	return s.entry.L2ToL1Calls
}

func (s *chainState) peek() (CrossChainCall, bool) {
	a, c := s.array(), s.cursors[s.slot()]
	if c >= len(a) {
		return CrossChainCall{}, false
	}
	return a[c], true
}

func (s *chainState) remaining() int { return len(s.array()) - s.cursors[s.slot()] }

func (s *chainState) take() (CrossChainCall, bool) {
	c, ok := s.peek()
	if ok {
		s.cursors[s.slot()]++
	}
	return c, ok
}

func (s *chainState) nextRow() (*ExpectedCall, bool) {
	if s.next >= len(s.entry.ExpectedL1ToL2Calls) {
		return nil, false
	}
	return &s.entry.ExpectedL1ToL2Calls[s.next], true
}

func (s *chainState) openRow(callHash common.Hash) int {
	row := s.next
	s.saved = append(s.saved, s.next)
	s.next++
	s.seen[row] = true
	s.hashes = append(s.hashes, s.rolling)
	s.rolling.NestedBegin(callHash)
	s.frames = append(s.frames, row)
	return row
}

func (s *chainState) markStaticRow() {
	s.seen[s.next] = true
	s.next++
}

func (s *chainState) closeRow(row int, success bool) error {
	s.frames = s.frames[:len(s.frames)-1]
	saved, hash := s.saved[len(s.saved)-1], s.hashes[len(s.hashes)-1]
	s.saved, s.hashes = s.saved[:len(s.saved)-1], s.hashes[:len(s.hashes)-1]
	if success {
		s.rolling.NestedEnd()
		return nil
	}
	if s.rolling.Sum() != s.entry.ExpectedL1ToL2Calls[row].RevertedOrStaticRollingHash {
		return failTables("chain %d failed row %d reverted rolling hash differs", s.chain, row)
	}
	// A terminal revert restores the host's rolling hash and row cursor.
	s.rolling, s.next = hash, saved
	return nil
}

func (s *chainState) finish() error {
	lengths := make([]int, 0, 1+len(s.entry.ExpectedL1ToL2Calls))
	lengths = append(lengths, len(s.entry.L2ToL1Calls))
	for i := range s.entry.ExpectedL1ToL2Calls {
		lengths = append(lengths, len(s.entry.ExpectedL1ToL2Calls[i].Calls))
	}
	if len(s.frames) > 0 || slices.Contains(s.seen, false) || !slices.Equal(s.cursors, lengths) {
		return failTables("chain %d table not consumed: row cursor %d, call cursors %v of %v", s.chain, s.next, s.cursors, lengths)
	}
	if s.rolling.Sum() != s.entry.RollingHash {
		return failTables("chain %d rolling hash differs", s.chain)
	}
	return nil
}

// facts are what verification accumulates: the committed calls leaving the
// rollup, and the value crossing in each direction.
type facts struct {
	outbound []common.Hash
	toL2     big.Int
	toL1     big.Int
}

func (f *facts) clone() *facts {
	c := &facts{outbound: slices.Clone(f.outbound)}
	c.toL2.Set(&f.toL2)
	c.toL1.Set(&f.toL1)
	return c
}

// value records value sent from chain source.
func (f *facts) value(source uint64, v *big.Int) error {
	if err := checkUint256(v); err != nil {
		return err
	}
	total := &f.toL1
	if source == MainnetRollupID {
		total = &f.toL2
	}
	total.Add(total, v)
	if total.BitLen() > 256 {
		return failTables("value overflows")
	}
	return nil
}

// chainCallHash is the hash of call made from chain source, which the call
// must name, to chain target.
func chainCallHash(call *CrossChainCall, source, target uint64) (common.Hash, error) {
	if call.SourceRollupID != source {
		return common.Hash{}, failTables("call from rollup %d made on %d", call.SourceRollupID, source)
	}
	return CrossChainCallHash(&CallHashInput{
		IsStatic:       call.IsStatic,
		SourceAddress:  call.SourceAddress,
		SourceRollupID: source,
		TargetAddress:  call.TargetAddress,
		TargetRollupID: target,
		Value:          call.Value,
		Data:           call.Data,
	})
}
