package commitmentdb

import (
	"context"
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/stretchr/testify/require"
)

func Test_EncodeCommitmentState(t *testing.T) {
	t.Parallel()
	cs := commitmentState{
		txNum:     rand.Uint64(),
		trieState: make([]byte, 1024),
	}
	n, err := rand.Read(cs.trieState)
	require.NoError(t, err)
	require.Equal(t, len(cs.trieState), n)

	buf, err := cs.Encode()
	require.NoError(t, err)
	require.NotEmpty(t, buf)

	var dec commitmentState
	err = dec.Decode(buf)
	require.NoError(t, err)
	require.Equal(t, cs.txNum, dec.txNum)
	require.Equal(t, cs.trieState, dec.trieState)
}

type testStateReader struct {
	branchData   []byte
	step         kv.Step
	readDomain   kv.Domain
	readKey      []byte
	readStepSize uint64
	withHistory  bool
}

var _ StateReader = (*testStateReader)(nil)

func (r *testStateReader) WithHistory() bool { return r.withHistory }

func (r *testStateReader) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r *testStateReader) Read(d kv.Domain, key []byte, stepSize uint64) ([]byte, kv.Step, error) {
	r.readDomain = d
	r.readKey = append(r.readKey[:0], key...)
	r.readStepSize = stepSize
	if r.readDomain != kv.CommitmentDomain {
		return nil, 0, nil
	}
	return r.branchData, r.step, nil
}

func (r *testStateReader) Clone(kv.TemporalTx) StateReader { return r }

func (r *testStateReader) CloneForWorker(context.Context, kv.TemporalTx) StateReader { return r }

func Test_TrieContext_BranchCopiesData(t *testing.T) {
	t.Parallel()

	prefix := []byte{0xaa}
	expectedBranchData := []byte{1, 2, 3}
	reader := &testStateReader{
		branchData: append([]byte(nil), expectedBranchData...),
		step:       42,
	}
	ctx := NewTrieContextRo(reader, 1)

	branch, step, err := ctx.Branch(prefix)
	require.NoError(t, err)
	require.Equal(t, reader.step, step)
	require.Equal(t, expectedBranchData, branch)
	require.Equal(t, kv.CommitmentDomain, reader.readDomain)
	require.Equal(t, prefix, reader.readKey)
	require.Equal(t, uint64(1), reader.readStepSize)

	reader.branchData[0] = 9
	require.Equal(t, expectedBranchData, branch)

	branch[1] = 8
	require.Equal(t, []byte{9, 2, 3}, reader.branchData)
}

type branchMemBatch struct {
	kv.TemporalMemBatch
	value []byte
	ok    bool
	bound bool
	step  kv.Step
	calls int
	key   []byte
}

func (m *branchMemBatch) GetLatest(domain kv.Domain, key []byte) ([]byte, kv.Step, bool) {
	m.calls++
	m.key = append(m.key[:0], key...)
	if domain != kv.CommitmentDomain || !m.ok {
		if m.bound {
			return nil, m.step, false
		}
		return nil, kv.NoStepBound, false
	}
	return m.value, m.step, true
}

type branchChildCountDomains struct {
	sd
	mem *branchMemBatch
}

func (d *branchChildCountDomains) GetLatestFromMemory(domain kv.Domain, key []byte) (v []byte, step, maxStep kv.Step, ok bool) {
	v, step, ok = d.mem.GetLatest(domain, key)
	if ok {
		return v, step, kv.NoStepBound, true
	}
	return nil, 0, step, false
}

func (d *branchChildCountDomains) StepSize() uint64 { return 1 }

func TestBranchChildCountReadsPostComputeView(t *testing.T) {
	t.Parallel()

	prefix := []byte{0x0a}
	compactKey := nibbles.HexToCompact(prefix)

	t.Run("changed branch comes from memory", func(t *testing.T) {
		mem := &branchMemBatch{value: []byte{0, 0, 0, 0b0000_0111}, ok: true}
		reader := &testStateReader{branchData: []byte{0, 0, 0, 0b0000_0011}}
		sdc := &SharedDomainsCommitmentContext{
			sharedDomains: &branchChildCountDomains{mem: mem},
			stateReader:   reader,
		}

		count, err := sdc.BranchChildCount(prefix)
		require.NoError(t, err)
		require.Equal(t, 3, count)
		require.Equal(t, 1, mem.calls)
		require.Equal(t, compactKey, mem.key)
		require.Zero(t, reader.readStepSize)
	})

	t.Run("unchanged branch comes from installed reader", func(t *testing.T) {
		mem := &branchMemBatch{}
		reader := &testStateReader{branchData: []byte{0, 0, 0, 0b0000_0011}}
		sdc := &SharedDomainsCommitmentContext{
			sharedDomains: &branchChildCountDomains{mem: mem},
			stateReader:   reader,
		}

		count, err := sdc.BranchChildCount(prefix)
		require.NoError(t, err)
		require.Equal(t, 2, count)
		require.Equal(t, 1, mem.calls)
		require.Equal(t, compactKey, mem.key)
		require.Equal(t, kv.CommitmentDomain, reader.readDomain)
		require.Equal(t, compactKey, reader.readKey)
		require.Equal(t, uint64(1), reader.readStepSize)
	})
}

func TestBranchChildCountRejectsIncompleteComputedView(t *testing.T) {
	t.Parallel()

	prefix := []byte{0x0a}
	branch := []byte{0, 0, 0, 0b0000_0011}

	t.Run("missing state reader", func(t *testing.T) {
		sdc := &SharedDomainsCommitmentContext{
			sharedDomains: &branchChildCountDomains{mem: &branchMemBatch{}},
		}

		_, err := sdc.BranchChildCount(prefix)
		require.ErrorContains(t, err, "installed state reader")
	})

	t.Run("history reader suppresses branch writes", func(t *testing.T) {
		sdc := &SharedDomainsCommitmentContext{
			sharedDomains: &branchChildCountDomains{mem: &branchMemBatch{}},
			stateReader:   &testStateReader{branchData: branch, withHistory: true},
		}

		_, err := sdc.BranchChildCount(prefix)
		require.ErrorContains(t, err, "reader that permits branch writes")
	})

	t.Run("deferred branch updates are pending", func(t *testing.T) {
		sdc := &SharedDomainsCommitmentContext{
			sharedDomains: &branchChildCountDomains{mem: &branchMemBatch{}},
			stateReader:   &testStateReader{branchData: branch},
			pendingUpdate: &commitment.PendingCommitmentUpdate{},
		}

		_, err := sdc.BranchChildCount(prefix)
		require.ErrorContains(t, err, "deferred branch updates are pending")
	})

	t.Run("staged unwind bounds the fallback", func(t *testing.T) {
		sdc := &SharedDomainsCommitmentContext{
			sharedDomains: &branchChildCountDomains{mem: &branchMemBatch{bound: true, step: 1}},
			stateReader:   &testStateReader{branchData: branch},
		}

		_, err := sdc.BranchChildCount(prefix)
		require.ErrorContains(t, err, "staged unwind")
	})
}
