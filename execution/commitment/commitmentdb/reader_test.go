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

package commitmentdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/kvmetrics"
)

// --- fakes: exercise reader routing without a real temporal DB ------------

type getVal struct {
	v    []byte
	step kv.Step
}

type getLatestCall struct {
	domain kv.Domain
	key    []byte
}

type fakeGetter struct {
	values map[string]getVal
	calls  []getLatestCall
}

func (g *fakeGetter) GetLatest(name kv.Domain, k []byte) ([]byte, kv.Step, error) {
	g.calls = append(g.calls, getLatestCall{name, append([]byte(nil), k...)})
	if gv, ok := g.values[fmt.Sprintf("%d/%x", name, k)]; ok {
		return gv.v, gv.step, nil
	}
	return nil, 0, nil
}

func (g *fakeGetter) HasPrefix(kv.Domain, []byte) ([]byte, []byte, bool, error) {
	return nil, nil, false, nil
}

func (g *fakeGetter) StepsInFiles(...kv.Domain) kv.Step { return 0 }

type fakeCommitmentSD struct {
	getter        *fakeGetter
	asGetterTx    kv.TemporalTx
	asGetterCalls int
}

func (s *fakeCommitmentSD) AsGetter(tx kv.TemporalTx) kv.TemporalGetter {
	s.asGetterCalls++
	s.asGetterTx = tx
	return s.getter
}

func (s *fakeCommitmentSD) SetTxNum(uint64)                                         {}
func (s *fakeCommitmentSD) AsPutDel(kv.TemporalTx) kv.TemporalPutDel                { return nil }
func (s *fakeCommitmentSD) MergeMetrics(kvmetrics.Source, *kvmetrics.DomainMetrics) {}
func (s *fakeCommitmentSD) StepSize() uint64                                        { return 1 }
func (s *fakeCommitmentSD) Metrics() *kvmetrics.DomainMetrics                       { return nil }

type asOfCall struct {
	domain kv.Domain
	key    []byte
	ts     uint64
}

type fakeTemporalTx struct {
	kv.TemporalTx
	values map[string][]byte
	calls  []asOfCall
}

func (tx *fakeTemporalTx) GetAsOf(d kv.Domain, k []byte, ts uint64) ([]byte, bool, error) {
	tx.calls = append(tx.calls, asOfCall{d, append([]byte(nil), k...), ts})
	if v, ok := tx.values[fmt.Sprintf("%d/%x", d, k)]; ok {
		return v, true, nil
	}
	return nil, false, nil
}

type putBranchCall struct {
	domain kv.Domain
	key    []byte
	val    []byte
	txNum  uint64
	prev   []byte
}

type fakePutDel struct {
	puts []putBranchCall
}

func (p *fakePutDel) DomainPut(d kv.Domain, k, v []byte, txNum uint64, prev []byte) error {
	p.puts = append(p.puts, putBranchCall{d, append([]byte(nil), k...), append([]byte(nil), v...), txNum, append([]byte(nil), prev...)})
	return nil
}

func (p *fakePutDel) DomainDel(kv.Domain, []byte, uint64, []byte) error { return nil }
func (p *fakePutDel) DomainDelPrefix(kv.Domain, []byte, uint64) error   { return nil }

// --- helpers --------------------------------------------------------------

func key(d kv.Domain, k []byte) string { return fmt.Sprintf("%d/%x", d, k) }

func newFakeGetter(vals map[string]getVal) *fakeGetter {
	return &fakeGetter{values: vals}
}

// --- tests ----------------------------------------------------------------

func TestHeadCaptureStateReader_Routing(t *testing.T) {
	t.Parallel()

	commKey := []byte{0xc0}
	commVal := []byte{0x01, 0x02, 0x03}
	accKey := []byte{0xa0}
	accVal := []byte{0x11}
	storKey := []byte{0x50}
	storVal := []byte{0x22}
	codeKey := []byte{0xce}
	codeVal := []byte{0x33}

	getter := newFakeGetter(map[string]getVal{
		key(kv.CommitmentDomain, commKey): {v: commVal, step: 7},
	})
	sd := &fakeCommitmentSD{getter: getter}
	pinnedTx := &fakeTemporalTx{}
	committedTx := &fakeTemporalTx{values: map[string][]byte{
		key(kv.AccountsDomain, accKey): accVal,
		key(kv.StorageDomain, storKey): storVal,
		key(kv.CodeDomain, codeKey):    codeVal,
	}}

	const plainAsOf = uint64(1000)
	reader := NewHeadCaptureStateReader(pinnedTx, sd, committedTx, plainAsOf)

	// Commitment reader is bound to the pinned parent tx's latest state.
	require.Equal(t, 1, sd.asGetterCalls)
	require.Same(t, pinnedTx, sd.asGetterTx)

	// Commitment reads resolve from the pinned tx's latest (via the getter).
	v, step, err := reader.Read(kv.CommitmentDomain, commKey, 1)
	require.NoError(t, err)
	require.Equal(t, commVal, v)
	require.Equal(t, kv.Step(7), step)
	require.Len(t, getter.calls, 1)
	require.Equal(t, kv.CommitmentDomain, getter.calls[0].domain)
	require.Equal(t, commKey, getter.calls[0].key)
	// A commitment read never touches either tx's history.
	require.Empty(t, pinnedTx.calls)
	require.Empty(t, committedTx.calls)

	// Plain reads resolve from the committed tx's history at plainAsOf.
	for _, tc := range []struct {
		domain  kv.Domain
		k, want []byte
	}{
		{kv.AccountsDomain, accKey, accVal},
		{kv.StorageDomain, storKey, storVal},
		{kv.CodeDomain, codeKey, codeVal},
	} {
		v, _, err := reader.Read(tc.domain, tc.k, 1)
		require.NoError(t, err)
		require.Equal(t, tc.want, v)
	}
	// Each plain read hit the committed tx (not the pinned tx) at plainAsOf.
	require.Len(t, committedTx.calls, 3)
	for _, c := range committedTx.calls {
		require.Equal(t, plainAsOf, c.ts)
	}
	require.Empty(t, pinnedTx.calls)
	// No extra commitment-getter calls from the plain reads.
	require.Len(t, getter.calls, 1)
}

func TestHeadCaptureStateReader_PlainStateAsOfRouting(t *testing.T) {
	t.Parallel()

	accKey := []byte{0xa0}

	mkReader := func(asOf uint64) (*CommitmentReplayStateReader, *fakeTemporalTx) {
		getter := newFakeGetter(nil)
		sd := &fakeCommitmentSD{getter: getter}
		committedTx := &fakeTemporalTx{values: map[string][]byte{
			key(kv.AccountsDomain, accKey): {byte(asOf)},
		}}
		return NewHeadCaptureStateReader(&fakeTemporalTx{}, sd, committedTx, asOf), committedTx
	}

	// Two build phases read plain state at different txNums: parent vs block-end.
	const parentTxNum = uint64(500)
	const blockEndTxNum = uint64(1000)

	parentReader, parentTx := mkReader(parentTxNum)
	_, _, err := parentReader.Read(kv.AccountsDomain, accKey, 1)
	require.NoError(t, err)
	require.Len(t, parentTx.calls, 1)
	require.Equal(t, parentTxNum, parentTx.calls[0].ts)

	endReader, endTx := mkReader(blockEndTxNum)
	_, _, err = endReader.Read(kv.AccountsDomain, accKey, 1)
	require.NoError(t, err)
	require.Len(t, endTx.calls, 1)
	require.Equal(t, blockEndTxNum, endTx.calls[0].ts)
}

func TestHeadCaptureStateReader_WithHistoryFalse(t *testing.T) {
	t.Parallel()

	reader := NewHeadCaptureStateReader(&fakeTemporalTx{}, &fakeCommitmentSD{getter: newFakeGetter(nil)}, &fakeTemporalTx{}, 1)
	require.False(t, reader.WithHistory())
}

func TestHeadCaptureStateReader_UnknownKeys(t *testing.T) {
	t.Parallel()

	getter := newFakeGetter(nil) // empty: every key misses
	sd := &fakeCommitmentSD{getter: getter}
	committedTx := &fakeTemporalTx{} // empty: GetAsOf returns not-found

	reader := NewHeadCaptureStateReader(&fakeTemporalTx{}, sd, committedTx, 1)

	v, _, err := reader.Read(kv.CommitmentDomain, []byte{0xde, 0xad}, 1)
	require.NoError(t, err)
	require.Nil(t, v)

	v, _, err = reader.Read(kv.AccountsDomain, []byte{0xbe, 0xef}, 1)
	require.NoError(t, err)
	require.Nil(t, v)
}

// TestHeadCaptureStateReader_PutBranchWritesToBatch confirms that because the
// reader reports WithHistory()==false, TrieContext.PutBranch does NOT take the
// history no-op path but writes the branch to the build's own SharedDomains
// putter (its in-memory batch, discarded on Close — never flushed to the DB).
func TestHeadCaptureStateReader_PutBranchWritesToBatch(t *testing.T) {
	t.Parallel()

	reader := NewHeadCaptureStateReader(&fakeTemporalTx{}, &fakeCommitmentSD{getter: newFakeGetter(nil)}, &fakeTemporalTx{}, 1)
	putter := &fakePutDel{}
	tc := &TrieContext{stateReader: reader, putter: putter, txNum: 42}

	prefix := []byte{0xaa}
	data := []byte{1, 2, 3}
	prev := []byte{9}
	require.NoError(t, tc.PutBranch(prefix, data, prev))

	require.Len(t, putter.puts, 1)
	require.Equal(t, kv.CommitmentDomain, putter.puts[0].domain)
	require.Equal(t, prefix, putter.puts[0].key)
	require.Equal(t, data, putter.puts[0].val)
	require.Equal(t, uint64(42), putter.puts[0].txNum)
	require.Equal(t, prev, putter.puts[0].prev)
}

// TestHeadCaptureStateReader_BranchCopiesData verifies the value read through the
// composed reader is owned by the trie-context boundary: mutating the source (or
// the returned slice) after Branch does not corrupt the other, so no live mmap
// alias is retained past the read.
func TestHeadCaptureStateReader_BranchCopiesData(t *testing.T) {
	t.Parallel()

	prefix := []byte{0xaa}
	source := []byte{1, 2, 3}
	getter := newFakeGetter(map[string]getVal{
		key(kv.CommitmentDomain, prefix): {v: source, step: 5},
	})
	reader := NewHeadCaptureStateReader(&fakeTemporalTx{}, &fakeCommitmentSD{getter: getter}, &fakeTemporalTx{}, 1)

	ctx := NewTrieContextRo(reader, 1)
	branch, step, err := ctx.Branch(prefix)
	require.NoError(t, err)
	require.Equal(t, kv.Step(5), step)
	require.Equal(t, []byte{1, 2, 3}, branch)

	source[0] = 9
	require.Equal(t, []byte{1, 2, 3}, branch)

	branch[1] = 8
	require.Equal(t, []byte{9, 2, 3}, source)
}
