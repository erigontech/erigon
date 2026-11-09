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
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/eez/protocol/da"
)

// ErrMaterialize reports a cross-chain call forest the execution tables of
// eez-rollupX cannot represent.
var ErrMaterialize = errors.New("cross-chain transaction outside the execution-table profile")

// This file ports entries/materialize.rs of eez-rollupX d0130ab9, which
// a50c326d matches but for an always-empty callback partition: the tables
// a cross-chain transaction produces on every chain it executes on. One
// entry is active per executing chain; a call to a chain appends to that
// entry's current flat array, and a call leaving it opens a row of the
// entry's reentrant table, before its children.

// traceKind is a step of an entry's symbolic execution trace.
type traceKind int

const (
	traceCallBegin traceKind = iota
	traceCallEnd
	traceNestedBegin
	traceNestedEnd
	traceNestedRevert
	traceStaticLookup
	traceRegionBegin
	traceRegionRestore
)

type traceEvent struct {
	kind       traceKind
	row        int
	callHash   common.Hash
	success    bool
	returnData []byte
}

// entryTrace is the execution of one mutable entry. L2 entries fold it at
// once; an L1 entry keeps it until settlement supplies the root updates its
// rolling hash starts from.
type entryTrace struct {
	events []traceEvent
}

// materializedBatch is one chain's tables and their traces.
type materializedBatch struct {
	entries         []ExecutionEntry
	staticEntries   []StaticExecutionEntry
	l2StaticEntries []L2StaticExecutionEntry
	traces          []entryTrace
	immediateCount  int
	// sourceEntryCount is the prefix of entries the transaction's origin
	// consumes.
	sourceEntryCount int
	// sourceOutboundHashes are the L2-origin proxy calls whose frames
	// committed.
	sourceOutboundHashes []common.Hash
}

func failMaterialize(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrMaterialize, fmt.Sprintf(format, args...))
}

// ensureMaterializable rejects calls the tables cannot represent: calls to
// the calling chain, static calls with value or a revert span, failed calls
// with descendants, and static calls with descendants other than a
// top-level read's terminal static callbacks to its own chain.
func ensureMaterializable(calls []da.SemanticAction) error {
	for i := range calls {
		c := &calls[i]
		if c.SourceRollupID == c.TargetRollupID {
			return failMaterialize("call %d to its own chain", i)
		}
		if c.Static {
			if !c.Value.IsZero() {
				return failMaterialize("static call %d carries value", i)
			}
			if c.RevertSpan != 0 {
				return failMaterialize("static call %d carries a revert span", i)
			}
			if err := checkStaticCallbacks(calls, i); err != nil {
				return err
			}
		}
		if !c.Success && hasChild(calls, i) {
			return failMaterialize("failed call %d has descendants", i)
		}
	}
	return nil
}

func hasChild(calls []da.SemanticAction, i int) bool {
	for j := range calls {
		if calls[j].Parent == i {
			return true
		}
	}
	return false
}

func checkStaticCallbacks(calls []da.SemanticAction, read int) error {
	if !hasChild(calls, read) {
		return nil
	}
	if calls[read].Parent >= 0 {
		return failMaterialize("nested static call %d has descendants", read)
	}
	for j := range calls {
		c := &calls[j]
		if c.Parent != read {
			continue
		}
		switch {
		case !c.Static:
			return failMaterialize("static read %d makes a mutable callback", read)
		case c.TargetRollupID != calls[read].SourceRollupID:
			return failMaterialize("static callback %d does not return to the reading chain", j)
		case !c.Success:
			return failMaterialize("static callback %d failed", j)
		case hasChild(calls, j):
			return failMaterialize("static callback %d has descendants", j)
		}
	}
	return nil
}

// callForest is the preorder of a transaction's calls: each call's
// children, the end of each subtree, and the revert regions.
type callForest struct {
	roots       []int
	children    [][]int
	subtreeEnds []int
	regions     map[int]revertRegion
}

type revertRegion struct {
	end       int
	hostChain uint64
	rootLevel bool
}

func newCallForest(calls []da.SemanticAction, entryRollupID uint64) (*callForest, error) {
	if err := ensureMaterializable(calls); err != nil {
		return nil, err
	}
	f := &callForest{children: make([][]int, len(calls)), regions: map[int]revertRegion{}}
	for i := range calls {
		c := &calls[i]
		if c.Parent < 0 {
			if c.SourceRollupID != entryRollupID {
				return nil, failMaterialize("root call %d originates on %d, not %d", i, c.SourceRollupID, entryRollupID)
			}
			f.roots = append(f.roots, i)
			continue
		}
		if c.Parent >= i {
			return nil, failMaterialize("call %d parent %d is not earlier", i, c.Parent)
		}
		if calls[c.Parent].TargetRollupID != c.SourceRollupID {
			return nil, failMaterialize("call %d leaves %d, its parent %d runs on %d", i, c.SourceRollupID, c.Parent, calls[c.Parent].TargetRollupID)
		}
		f.children[c.Parent] = append(f.children[c.Parent], i)
	}
	// The parent links must describe a contiguous preorder.
	next := 0
	var visit func(i int) bool
	visit = func(i int) bool {
		if i != next {
			return false
		}
		next++
		for _, child := range f.children[i] {
			if !visit(child) {
				return false
			}
		}
		return true
	}
	for _, root := range f.roots {
		if !visit(root) {
			return nil, failMaterialize("parent links are not a contiguous preorder")
		}
	}
	f.subtreeEnds = make([]int, len(calls))
	for i := len(calls) - 1; i >= 0; i-- {
		f.subtreeEnds[i] = i + 1
		if k := len(f.children[i]); k > 0 {
			f.subtreeEnds[i] = f.subtreeEnds[f.children[i][k-1]]
		}
	}
	previousEnd := 0
	for i := range calls {
		span := calls[i].RevertSpan
		if span == 0 {
			continue
		}
		end := i + span
		switch {
		case end > len(calls):
			return nil, failMaterialize("call %d revert span ends at %d, beyond %d calls", i, end, len(calls))
		case i < previousEnd:
			return nil, failMaterialize("nested or overlapping revert regions")
		}
		// A region covers whole sibling subtrees at one level.
		cursor := i
		for cursor < end {
			if calls[cursor].Parent != calls[i].Parent {
				return nil, failMaterialize("call %d revert span does not cover whole sibling subtrees", i)
			}
			cursor = f.subtreeEnds[cursor]
		}
		if cursor != end {
			return nil, failMaterialize("call %d revert span ends inside a subtree", i)
		}
		f.regions[i] = revertRegion{end: end, hostChain: calls[i].SourceRollupID, rootLevel: calls[i].Parent < 0}
		previousEnd = end
	}
	return f, nil
}

// l2RootPlan assigns the roots of an L2-origin transaction to the source
// table's cursor slots: one slot per mutable root, reused by a later retry
// after a root-level revert region restores the cursor. A static root reads
// at the cursor without taking a slot.
type l2RootPlan struct {
	assignments []l2RootAssignment
	slotRoots   []int
}

type l2RootAssignment struct {
	root int
	// slot is the root's source cursor slot, -1 for a static root.
	slot   int
	cursor int
}

func newL2RootPlan(calls []da.SemanticAction, f *callForest) (*l2RootPlan, error) {
	p := &l2RootPlan{}
	cursor := 0
	regionEnd, savedCursor := -1, 0
	for _, root := range f.roots {
		if calls[root].Static {
			p.assignments = append(p.assignments, l2RootAssignment{root: root, slot: -1, cursor: cursor})
			continue
		}
		if r, ok := f.regions[root]; ok && r.rootLevel {
			if regionEnd >= 0 {
				return nil, failMaterialize("nested root-level revert regions")
			}
			regionEnd, savedCursor = r.end, cursor
		}
		slot := cursor
		cursor++
		if slot < len(p.slotRoots) {
			prev, cur := &calls[p.slotRoots[slot]], &calls[root]
			ph, err := sourceCallHash(prev)
			if err != nil {
				return nil, err
			}
			ch, err := sourceCallHash(cur)
			if err != nil {
				return nil, err
			}
			if ph != ch || prev.Success != cur.Success || !slices.Equal(prev.ReturnData, cur.ReturnData) {
				return nil, failMaterialize("source cursor slot %d reused with another entry", slot)
			}
		} else {
			p.slotRoots = append(p.slotRoots, root)
		}
		p.assignments = append(p.assignments, l2RootAssignment{root: root, slot: slot, cursor: slot})
		if regionEnd >= 0 && f.subtreeEnds[root] == regionEnd {
			cursor, regionEnd = savedCursor, -1
		}
	}
	if regionEnd >= 0 {
		return nil, failMaterialize("source cursor ends inside a root-level revert region")
	}
	return p, nil
}

// materializeBatches materializes every chain a transaction starting on
// entryRollupID executes on.
func materializeBatches(calls []da.SemanticAction, entryRollupID uint64) (map[uint64]*materializedBatch, error) {
	f, err := newCallForest(calls, entryRollupID)
	if err != nil {
		return nil, err
	}
	m := &materializer{
		calls:       calls,
		children:    f.children,
		subtreeEnds: f.subtreeEnds,
		regions:     f.regions,
		outputs:     map[uint64]*chainOutput{},
		active:      map[uint64]*activeContext{},
	}
	var sourceEntryCount int
	if entryRollupID == MainnetRollupID {
		applied, err := l1AppliedRootMask(calls)
		if err != nil {
			return nil, err
		}
		for _, root := range f.roots {
			if !calls[root].Static {
				sourceEntryCount++
			}
		}
		for _, root := range f.roots {
			c := &calls[root]
			if c.Static {
				if err := m.pushStaticEntry(entryRollupID, root); err != nil {
					return nil, err
				}
				continue
			}
			h, err := sourceCallHash(c)
			if err != nil {
				return nil, err
			}
			entry := m.openEntry(entryRollupID, h, c.TargetRollupID, c)
			if applied[root] {
				if err := m.beginRegion(root); err != nil {
					return nil, err
				}
				if err := m.executeCall(root); err != nil {
					return nil, err
				}
				if err := m.closeRegionAtSubtree(root); err != nil {
					return nil, err
				}
			}
			m.closeEntry(entryRollupID, entry)
		}
	} else {
		// Each source cursor slot has one entry; a call back from L1 runs
		// from that entry's incoming calls while the outbound frame is open.
		// Every root of the user transaction shares one immediate L1 host.
		plan, err := newL2RootPlan(calls, f)
		if err != nil {
			return nil, err
		}
		sourceEntryCount = len(plan.slotRoots)
		for _, root := range plan.slotRoots {
			c := &calls[root]
			h, err := sourceCallHash(c)
			if err != nil {
				return nil, err
			}
			m.pushEntry(entryRollupID, h, c.TargetRollupID, c)
		}
		for _, a := range plan.assignments {
			if a.slot < 0 {
				if err := m.pushL2StaticEntry(entryRollupID, a.root, a.cursor); err != nil {
					return nil, err
				}
			}
		}
		host := m.openHostEntry(MainnetRollupID, entryRollupID)
		for _, a := range plan.assignments {
			if err := m.beginRegion(a.root); err != nil {
				return nil, err
			}
			if a.slot >= 0 {
				m.installContext(entryRollupID, a.slot)
			}
			if err := m.executeCall(a.root); err != nil {
				return nil, err
			}
			if a.slot >= 0 {
				m.closeEntry(entryRollupID, a.slot)
			}
			if err := m.closeRegionAtSubtree(a.root); err != nil {
				return nil, err
			}
		}
		m.closeEntry(MainnetRollupID, host)
	}
	return m.finish(entryRollupID, sourceEntryCount)
}

// l1AppliedRootMask marks the L1-origin roots whose L2 execution applies:
// mutable, successful and outside any root-level revert region. The others
// are lookups, their results in the source entry alone.
func l1AppliedRootMask(calls []da.SemanticAction) ([]bool, error) {
	applied := make([]bool, len(calls))
	revertedUntil := 0
	for i := range calls {
		c := &calls[i]
		if c.Parent >= 0 {
			continue
		}
		reverted := i < revertedUntil
		if c.RevertSpan != 0 {
			end := i + c.RevertSpan
			if end > len(calls) {
				return nil, failMaterialize("root %d revert span %d exceeds %d calls", i, c.RevertSpan, len(calls))
			}
			reverted = true
			revertedUntil = max(revertedUntil, end)
		}
		applied[i] = !c.Static && c.Success && !reverted
	}
	return applied, nil
}

type activeContext struct {
	entry     int
	frameRows []int
	nextRow   int
}

type chainOutput struct {
	entries         []ExecutionEntry
	staticEntries   []StaticExecutionEntry
	l2StaticEntries []L2StaticExecutionEntry
	traces          []entryTrace
	immediateCount  int
}

// callArrayLocation is one call array: an entry's flat calls or a row's.
type callArrayLocation struct {
	chain uint64
	entry int
	row   int // -1 for the flat calls
}

type activeRegion struct {
	end                int
	hostChain          uint64
	branched           bool
	savedRow           int
	savedOutboundCount int
	firstCalls         map[callArrayLocation]int
}

type materializer struct {
	calls                []da.SemanticAction
	children             [][]int
	subtreeEnds          []int
	regions              map[int]revertRegion
	outputs              map[uint64]*chainOutput
	active               map[uint64]*activeContext
	region               *activeRegion
	sourceOutboundHashes []common.Hash
}

func (m *materializer) output(chain uint64) *chainOutput {
	o, ok := m.outputs[chain]
	if !ok {
		o = &chainOutput{}
		m.outputs[chain] = o
	}
	return o
}

func (m *materializer) beginRegion(node int) error {
	r, ok := m.regions[node]
	if !ok {
		return nil
	}
	if m.region != nil {
		return failMaterialize("nested or overlapping revert regions")
	}
	_, hostActive := m.active[r.hostChain]
	branched := !r.rootLevel && hostActive
	a := &activeRegion{
		end:                r.end,
		hostChain:          r.hostChain,
		branched:           branched,
		savedOutboundCount: len(m.sourceOutboundHashes),
		firstCalls:         map[callArrayLocation]int{},
	}
	if branched {
		a.savedRow = m.active[r.hostChain].nextRow
		m.pushEvent(r.hostChain, traceEvent{kind: traceRegionBegin})
	}
	m.region = a
	return nil
}

func (m *materializer) closeRegionAtSubtree(node int) error {
	if m.region == nil {
		return nil
	}
	boundary := m.subtreeEnds[node]
	if boundary < m.region.end {
		return nil
	}
	if boundary != m.region.end {
		return failMaterialize("revert region ending at %d crosses subtree boundary %d", m.region.end, boundary)
	}
	r := m.region
	m.region = nil
	m.sourceOutboundHashes = m.sourceOutboundHashes[:r.savedOutboundCount]
	if r.branched {
		m.active[r.hostChain].nextRow = r.savedRow
		m.pushEvent(r.hostChain, traceEvent{kind: traceRegionRestore})
	}
	return nil
}

func (m *materializer) openHostEntry(chain, destination uint64) int {
	o := m.output(chain)
	i := len(o.entries)
	o.entries = append(o.entries, ExecutionEntry{DestinationRollupID: destination, Success: true, ReturnData: []byte{}})
	o.traces = append(o.traces, entryTrace{})
	o.immediateCount++
	m.installContext(chain, i)
	return i
}

func (m *materializer) openEntry(chain uint64, proxyEntryHash common.Hash, destination uint64, outer *da.SemanticAction) int {
	i := m.pushEntry(chain, proxyEntryHash, destination, outer)
	m.output(chain).immediateCount++
	m.installContext(chain, i)
	return i
}

// openDeliveryEntry opens the entry an executeIncomingCrossChainCall runs.
// The delivery commits even when its call fails: the failure is folded into
// the rolling hash, and the entry succeeds with no return data.
func (m *materializer) openDeliveryEntry(chain uint64, proxyEntryHash common.Hash, destination uint64, outer *da.SemanticAction) int {
	i := m.pushEntry(chain, proxyEntryHash, destination, outer)
	o := m.output(chain)
	o.immediateCount++
	if !outer.Success {
		o.entries[i].Success, o.entries[i].ReturnData = true, []byte{}
	}
	m.installContext(chain, i)
	return i
}

func (m *materializer) pushEntry(chain uint64, proxyEntryHash common.Hash, destination uint64, outer *da.SemanticAction) int {
	o := m.output(chain)
	i := len(o.entries)
	o.entries = append(o.entries, ExecutionEntry{
		ProxyEntryHash:      proxyEntryHash,
		DestinationRollupID: destination,
		Success:             outer.Success,
		ReturnData:          slices.Clone(outer.ReturnData),
	})
	o.traces = append(o.traces, entryTrace{})
	return i
}

func (m *materializer) pushL2StaticEntry(chain uint64, root, cursor int) error {
	c := &m.calls[root]
	callbacks, rolling := m.staticCallbacks(root)
	h, err := sourceCallHash(c)
	if err != nil {
		return err
	}
	o := m.output(chain)
	o.l2StaticEntries = append(o.l2StaticEntries, L2StaticExecutionEntry{
		ExpectedEntryIndex: big.NewInt(int64(cursor)),
		ProxyEntryHash:     h,
		IncomingCalls:      callbacks,
		RollingHash:        rolling,
		Success:            c.Success,
		ReturnData:         slices.Clone(c.ReturnData),
	})
	return nil
}

func (m *materializer) pushStaticEntry(chain uint64, root int) error {
	c := &m.calls[root]
	callbacks, rolling := m.staticCallbacks(root)
	h, err := sourceCallHash(c)
	if err != nil {
		return err
	}
	o := m.output(chain)
	o.staticEntries = append(o.staticEntries, StaticExecutionEntry{
		ProxyEntryHash:      h,
		L2ToL1Calls:         callbacks,
		RollingHash:         rolling,
		DestinationRollupID: c.TargetRollupID,
		Success:             c.Success,
		ReturnData:          slices.Clone(c.ReturnData),
	})
	return nil
}

// staticCallbacks returns the callbacks a static root makes to its own
// chain and the untagged fold of their results.
func (m *materializer) staticCallbacks(root int) ([]CrossChainCall, common.Hash) {
	var rolling common.Hash
	callbacks := make([]CrossChainCall, 0, len(m.children[root]))
	for _, child := range m.children[root] {
		c := &m.calls[child]
		rolling = StaticResult(rolling, c.Success, c.ReturnData)
		callbacks = append(callbacks, callFromAction(c))
	}
	return callbacks, rolling
}

func (m *materializer) installContext(chain uint64, entry int) {
	m.active[chain] = &activeContext{entry: entry}
}

func (m *materializer) closeEntry(chain uint64, _ int) {
	delete(m.active, chain)
}

func (m *materializer) executeCall(node int) error {
	c := &m.calls[node]
	destination := c.TargetRollupID
	if !c.Static && c.SourceRollupID != MainnetRollupID && destination == MainnetRollupID && c.Success {
		h, err := sourceCallHash(c)
		if err != nil {
			return err
		}
		m.sourceOutboundHashes = append(m.sourceOutboundHashes, h)
	}
	dh, err := destinationCallHash(c)
	if err != nil {
		return err
	}
	opened := -1
	if _, ok := m.active[destination]; !ok {
		if destination == MainnetRollupID {
			return failMaterialize("call %d reaches L1 without an active L1 host entry", node)
		}
		opened = m.openDeliveryEntry(destination, dh, destination, c)
	}
	if err := m.appendFlatCall(destination, c); err != nil {
		return err
	}
	m.pushEvent(destination, traceEvent{kind: traceCallBegin, callHash: dh})
	for _, child := range m.children[node] {
		if err := m.beginRegion(child); err != nil {
			return err
		}
		if err := m.executeReentrant(destination, child); err != nil {
			return err
		}
		if err := m.closeRegionAtSubtree(child); err != nil {
			return err
		}
	}
	m.pushEvent(destination, traceEvent{kind: traceCallEnd, success: c.Success, returnData: c.ReturnData})
	if opened >= 0 {
		m.closeEntry(destination, opened)
	}
	return nil
}

func (m *materializer) executeReentrant(source uint64, node int) error {
	c := &m.calls[node]
	if c.SourceRollupID != source {
		return failMaterialize("nested call %d leaves %d, executing on %d", node, c.SourceRollupID, source)
	}
	row, reused, err := m.pushRow(source, c)
	if err != nil {
		return err
	}
	if reused && len(m.children[node]) > 0 {
		return failMaterialize("reverted reentrant row reused by call %d with descendants", node)
	}
	h, err := sourceCallHash(c)
	if err != nil {
		return err
	}
	if c.Static {
		m.pushEvent(source, traceEvent{kind: traceStaticLookup, row: row, callHash: h})
	} else {
		m.pushEvent(source, traceEvent{kind: traceNestedBegin, row: row, callHash: h})
	}
	ctx := m.active[source]
	ctx.frameRows = append(ctx.frameRows, row)
	// A static root's callback runs from the root's static entry; here it
	// only pins the host's static row and opens no delivery.
	if !(c.Parent >= 0 && m.calls[c.Parent].Static) {
		if err := m.executeCall(node); err != nil {
			return err
		}
	}
	ctx = m.active[source]
	ctx.frameRows = ctx.frameRows[:len(ctx.frameRows)-1]
	switch {
	case c.Static:
		// A static lookup is pinned to its position but moves neither the
		// host's rolling hash nor its row cursor at run time.
	case c.Success:
		m.pushEvent(source, traceEvent{kind: traceNestedEnd})
	default:
		m.pushEvent(source, traceEvent{kind: traceNestedRevert, row: row})
	}
	return nil
}

func (m *materializer) appendFlatCall(chain uint64, c *da.SemanticAction) error {
	ctx := m.active[chain]
	frame := -1
	if k := len(ctx.frameRows); k > 0 {
		frame = ctx.frameRows[k-1]
	}
	entry := &m.outputs[chain].entries[ctx.entry]
	var index int
	if frame >= 0 {
		index = len(entry.ExpectedL1ToL2Calls[frame].Calls)
		entry.ExpectedL1ToL2Calls[frame].Calls = append(entry.ExpectedL1ToL2Calls[frame].Calls, callFromAction(c))
	} else {
		index = len(entry.L2ToL1Calls)
		entry.L2ToL1Calls = append(entry.L2ToL1Calls, callFromAction(c))
	}
	if m.region == nil || chain == m.region.hostChain {
		return nil
	}
	// Every other chain keeps its folds and rolls the region back through
	// the revertNextNCalls of the region's first call in each array.
	loc := callArrayLocation{chain: chain, entry: ctx.entry, row: frame}
	first, seen := m.region.firstCalls[loc]
	if !seen {
		m.region.firstCalls[loc] = index
		m.callAt(loc, index).RevertNextNCalls = 1
		return nil
	}
	call := m.callAt(loc, first)
	if call.RevertNextNCalls == math.MaxUint16 {
		return failMaterialize("revert region exceeds %d calls in one array", math.MaxUint16)
	}
	call.RevertNextNCalls++
	return nil
}

func (m *materializer) callAt(loc callArrayLocation, index int) *CrossChainCall {
	entry := &m.outputs[loc.chain].entries[loc.entry]
	if loc.row >= 0 {
		return &entry.ExpectedL1ToL2Calls[loc.row].Calls[index]
	}
	return &entry.L2ToL1Calls[index]
}

func (m *materializer) pushRow(chain uint64, c *da.SemanticAction) (int, bool, error) {
	ctx := m.active[chain]
	row := ctx.nextRow
	ctx.nextRow++
	entry := &m.outputs[chain].entries[ctx.entry]
	if row < len(entry.ExpectedL1ToL2Calls) {
		r := &entry.ExpectedL1ToL2Calls[row]
		if r.Success != c.Success || !slices.Equal(r.ReturnData, c.ReturnData) {
			return 0, false, failMaterialize("reverted reentrant row %d reused with another outcome", row)
		}
		return row, true, nil
	}
	if row != len(entry.ExpectedL1ToL2Calls) {
		return 0, false, failMaterialize("reentrant row cursor %d beyond %d rows", row, len(entry.ExpectedL1ToL2Calls))
	}
	entry.ExpectedL1ToL2Calls = append(entry.ExpectedL1ToL2Calls, ExpectedCall{
		Calls:      []CrossChainCall{},
		Success:    c.Success,
		ReturnData: slices.Clone(c.ReturnData),
	})
	return row, false, nil
}

func (m *materializer) pushEvent(chain uint64, e traceEvent) {
	o := m.outputs[chain]
	t := &o.traces[m.active[chain].entry]
	t.events = append(t.events, e)
}

func (m *materializer) finish(entryRollupID uint64, sourceEntryCount int) (map[uint64]*materializedBatch, error) {
	if m.region != nil {
		return nil, failMaterialize("ends inside a revert region")
	}
	out := make(map[uint64]*materializedBatch, len(m.outputs))
	for chain, o := range m.outputs {
		if chain != MainnetRollupID {
			for i := range o.entries {
				if err := finalizeTrace(&o.entries[i], &o.traces[i], L2EntrySeed(o.entries[i].ProxyEntryHash)); err != nil {
					return nil, err
				}
			}
		}
		b := &materializedBatch{
			entries:         o.entries,
			staticEntries:   o.staticEntries,
			l2StaticEntries: o.l2StaticEntries,
			traces:          o.traces,
			immediateCount:  o.immediateCount,
		}
		if chain == entryRollupID {
			b.sourceEntryCount = sourceEntryCount
			b.sourceOutboundHashes = slices.Clone(m.sourceOutboundHashes)
			if chain != MainnetRollupID {
				b.immediateCount = 0
			}
		}
		out[chain] = b
	}
	return out, nil
}

// finalizeL1EntryTraces fixes the position keys and rolling hashes of L1
// entries once settlement has supplied their root updates.
func finalizeL1EntryTraces(entries []ExecutionEntry, traces []entryTrace) error {
	if len(entries) != len(traces) {
		return failMaterialize("%d L1 entries for %d traces", len(entries), len(traces))
	}
	for i := range entries {
		e := &entries[i]
		if len(e.RollupUpdates) == 0 {
			return failMaterialize("L1 entry %d has no root updates", i)
		}
		if err := finalizeTrace(e, &traces[i], L1EntrySeed(e.RollupUpdates, e.ProxyEntryHash)); err != nil {
			return err
		}
	}
	return nil
}

func finalizeTrace(e *ExecutionEntry, t *entryTrace, rolling RollingHash) error {
	nextRow := 0
	seen := make([]bool, len(e.ExpectedL1ToL2Calls))
	type frame struct {
		row     int
		rolling RollingHash
	}
	var frames []frame
	var region *struct {
		rolling RollingHash
		row     int
	}
	keyRow := func(row int, callHash common.Hash) error {
		if row != nextRow {
			return failMaterialize("trace row %d is not at cursor %d", row, nextRow)
		}
		if row >= len(e.ExpectedL1ToL2Calls) {
			return failMaterialize("trace references missing row %d", row)
		}
		seen[row] = true
		key := positionKey(callHash, rolling.Sum())
		r := &e.ExpectedL1ToL2Calls[row]
		if r.Hash != (common.Hash{}) && r.Hash != key {
			return failMaterialize("reused row %d has different position keys", row)
		}
		r.Hash = key
		nextRow++
		return nil
	}
	for _, ev := range t.events {
		switch ev.kind {
		case traceCallBegin:
			rolling.CallBegin(ev.callHash)
		case traceCallEnd:
			rolling.CallEnd(ev.success, ev.returnData)
		case traceNestedBegin:
			if err := keyRow(ev.row, ev.callHash); err != nil {
				return err
			}
			frames = append(frames, frame{row: ev.row, rolling: rolling})
			rolling.NestedBegin(ev.callHash)
		case traceNestedEnd:
			if len(frames) == 0 {
				return failMaterialize("nested end without a frame")
			}
			frames = frames[:len(frames)-1]
			rolling.NestedEnd()
		case traceNestedRevert:
			if len(frames) == 0 {
				return failMaterialize("nested revert without a frame")
			}
			f := frames[len(frames)-1]
			frames = frames[:len(frames)-1]
			if f.row != ev.row {
				return failMaterialize("nested revert closes row %d, open row %d", ev.row, f.row)
			}
			r := &e.ExpectedL1ToL2Calls[ev.row]
			if r.Success {
				return failMaterialize("reverted row %d is successful", ev.row)
			}
			r.RevertedOrStaticRollingHash = rolling.Sum()
			rolling = f.rolling
		case traceStaticLookup:
			if err := keyRow(ev.row, ev.callHash); err != nil {
				return err
			}
		case traceRegionBegin:
			if region != nil {
				return failMaterialize("nested revert regions")
			}
			region = &struct {
				rolling RollingHash
				row     int
			}{rolling, nextRow}
		case traceRegionRestore:
			if region == nil {
				return failMaterialize("region restore without a region")
			}
			rolling, nextRow = region.rolling, region.row
			region = nil
		}
	}
	switch {
	case len(frames) > 0:
		return failMaterialize("trace ends with open nested frames")
	case region != nil:
		return failMaterialize("trace ends inside a revert region")
	case slices.Contains(seen, false):
		return failMaterialize("trace leaves reentrant rows unused")
	}
	e.RollingHash = rolling.Sum()
	return nil
}

// positionKey is keccak256(callHash ‖ rollingHash), the key of a reentrant
// row: the call it answers and the host's rolling hash when it is made.
func positionKey(callHash, rolling common.Hash) common.Hash {
	return crypto.Keccak256Hash(append(callHash[:], rolling[:]...))
}

func callFromAction(c *da.SemanticAction) CrossChainCall {
	return CrossChainCall{
		IsStatic:       c.Static,
		SourceAddress:  c.SourceAddress,
		SourceRollupID: c.SourceRollupID,
		TargetAddress:  c.TargetAddress,
		Value:          c.Value.ToBig(),
		Data:           slices.Clone(c.Data),
	}
}

// destinationCallHash is the hash a call's destination executes it under.
func destinationCallHash(c *da.SemanticAction) (common.Hash, error) {
	return CrossChainCallHash(&CallHashInput{
		IsStatic:       c.Static,
		SourceAddress:  c.SourceAddress,
		SourceRollupID: c.SourceRollupID,
		TargetAddress:  c.TargetAddress,
		TargetRollupID: c.TargetRollupID,
		Value:          c.Value.ToBig(),
		Data:           c.Data,
	})
}

// sourceCallHash is the hash a call's source chain keys it by. The L2
// managers of rollupX hash proxy calls with zero call gas, so it is the
// destination hash on every chain.
func sourceCallHash(c *da.SemanticAction) (common.Hash, error) {
	return destinationCallHash(c)
}
