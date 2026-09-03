package commitmentdb

import (
	"context"
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/common/length"
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
	records      [16][]byte
	recordsFound uint16
	recordsStep  kv.Step
	recordsKey   []byte
	recordsMask  uint16
	recordsKnown bool
	readCalls    int
	withHistory  bool
}

var _ StateReader = (*testStateReader)(nil)

func (r *testStateReader) WithHistory() bool { return r.withHistory }

func (r *testStateReader) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r *testStateReader) Read(d kv.Domain, key []byte, stepSize uint64) ([]byte, kv.Step, error) {
	r.readCalls++
	r.readDomain = d
	r.readKey = append(r.readKey[:0], key...)
	r.readStepSize = stepSize
	if r.readDomain != kv.CommitmentDomain {
		return nil, 0, nil
	}
	return r.branchData, r.step, nil
}

func (r *testStateReader) ReadCommitmentRecords(nodeKey []byte, mask uint16, maskKnown bool) (records [16][]byte, present uint16, step kv.Step, err error) {
	r.recordsKey = append(r.recordsKey[:0], nodeKey...)
	r.recordsMask = mask
	r.recordsKnown = maskKnown
	return r.records, r.recordsFound, r.recordsStep, nil
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

func Test_TrieContext_BranchSynthesizesEdgeRecord(t *testing.T) {
	t.Parallel()

	accountRecord := make([]byte, 1+20)
	accountRecord[0] = 1
	for i := range accountRecord[1:] {
		accountRecord[i+1] = byte(i + 1)
	}
	reader := &testStateReader{
		records:      [16][]byte{0: accountRecord},
		recordsFound: 1,
		recordsStep:  9,
	}
	prefix := []byte{0x10}
	ctx := &TrieContext{stateReader: reader, stepSize: 1, edgeRecords: true}

	branch, step, err := ctx.Branch(prefix)
	require.NoError(t, err)
	require.Equal(t, kv.Step(9), step)
	require.Equal(t, nibbles.EncodeKeyV3(nibbles.CompactToHex(prefix)), reader.recordsKey)
	require.Equal(t, uint16(0), reader.recordsMask)
	require.False(t, reader.recordsKnown)
	require.Equal(t, append([]byte{0, 1, 0, 1, 2, 20}, accountRecord[1:]...), branch)
}

func TestBranchChildCountReadsEdgeRecords(t *testing.T) {
	t.Parallel()

	reader := &testStateReader{
		records: [16][]byte{
			1: []byte{1},
			2: []byte{},
			7: []byte{1},
		},
		recordsFound: 1<<1 | 1<<2 | 1<<7,
	}
	sdc := &SharedDomainsCommitmentContext{sharedDomains: &branchChildCountDomains{}, stateReader: reader, edgeRecords: true}

	prefix := []byte{0xa, 0xb}
	count, err := sdc.BranchChildCount(prefix)
	require.NoError(t, err)
	require.Equal(t, 2, count, "empty edge-record tombstones must not count as children")
	require.Equal(t, nibbles.EncodeKeyV3(prefix), reader.recordsKey)
	require.Zero(t, reader.readCalls)
}

func Test_TrieContext_BranchWithMaskDoesNotDecodeStateBlobAsRoot(t *testing.T) {
	t.Parallel()

	hph := commitment.NewHexPatriciaHashed(length.Addr, nil, commitment.DefaultTrieConfig())
	defer hph.Release()
	trieState, err := hph.EncodeCurrentState(nil)
	require.NoError(t, err)
	stateValue, err := (&commitmentState{txNum: 1, blockNum: 1, trieState: trieState}).Encode()
	require.NoError(t, err)

	accountRecord := make([]byte, 1+20)
	accountRecord[0] = 1
	for i := range accountRecord[1:] {
		accountRecord[i+1] = byte(i + 1)
	}
	reader := &testStateReader{
		branchData:   stateValue,
		records:      [16][]byte{0: accountRecord},
		recordsFound: 1,
		recordsStep:  9,
	}
	ctx := &TrieContext{stateReader: reader, stepSize: 1, edgeRecords: true}

	branch, step, _, _, err := ctx.BranchWithMask(commitment.KeyCommitmentState, 1, true)
	require.NoError(t, err)
	require.Equal(t, kv.Step(9), step)
	require.Equal(t, append([]byte{0, 1, 0, 1, 2, 20}, accountRecord[1:]...), branch)
}

type branchChildCountDomains struct {
	stubSharedDomains
	value   []byte
	ok      bool
	bound   bool
	maxStep kv.Step
	calls   int
	key     []byte
}

func (d *branchChildCountDomains) GetLatestFromMemory(domain kv.Domain, key []byte) ([]byte, kv.Step, bool) {
	d.calls++
	d.key = append(d.key[:0], key...)
	if domain != kv.CommitmentDomain || !d.ok {
		if d.bound {
			return nil, d.maxStep, false
		}
		return nil, kv.NoStepBound, false
	}
	return d.value, kv.NoStepBound, true
}

func TestBranchChildCountReadsPostComputeView(t *testing.T) {
	t.Parallel()

	prefix := []byte{0x0a}
	compactKey := nibbles.HexToCompact(prefix)

	t.Run("changed branch comes from memory", func(t *testing.T) {
		domains := &branchChildCountDomains{value: []byte{0, 0, 0, 0b0000_0111}, ok: true}
		reader := &testStateReader{branchData: []byte{0, 0, 0, 0b0000_0011}}
		sdc := &SharedDomainsCommitmentContext{
			sharedDomains: domains,
			stateReader:   reader,
		}

		count, err := sdc.BranchChildCount(prefix)
		require.NoError(t, err)
		require.Equal(t, 3, count)
		require.Equal(t, 1, domains.calls)
		require.Equal(t, compactKey, domains.key)
		require.Zero(t, reader.readCalls)
	})

	t.Run("unchanged branch comes from installed reader", func(t *testing.T) {
		domains := &branchChildCountDomains{}
		reader := &testStateReader{branchData: []byte{0, 0, 0, 0b0000_0011}}
		sdc := &SharedDomainsCommitmentContext{
			sharedDomains: domains,
			stateReader:   reader,
		}

		count, err := sdc.BranchChildCount(prefix)
		require.NoError(t, err)
		require.Equal(t, 2, count)
		require.Equal(t, 1, domains.calls)
		require.Equal(t, compactKey, domains.key)
		require.Equal(t, 1, reader.readCalls)
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
			sharedDomains: &branchChildCountDomains{},
		}

		_, err := sdc.BranchChildCount(prefix)
		require.ErrorContains(t, err, "installed state reader")
	})

	t.Run("history reader suppresses branch writes", func(t *testing.T) {
		reader := &testStateReader{branchData: branch, withHistory: true}
		sdc := &SharedDomainsCommitmentContext{
			sharedDomains: &branchChildCountDomains{},
			stateReader:   reader,
		}

		_, err := sdc.BranchChildCount(prefix)
		require.ErrorContains(t, err, "reader that permits branch writes")
	})

	t.Run("deferred branch updates are pending", func(t *testing.T) {
		reader := &testStateReader{branchData: branch}
		sdc := &SharedDomainsCommitmentContext{
			sharedDomains: &branchChildCountDomains{},
			stateReader:   reader,
			pendingUpdate: &commitment.PendingCommitmentUpdate{},
		}

		_, err := sdc.BranchChildCount(prefix)
		require.ErrorContains(t, err, "deferred branch updates are pending")
	})

	t.Run("staged unwind bounds the fallback", func(t *testing.T) {
		reader := &testStateReader{branchData: branch}
		sdc := &SharedDomainsCommitmentContext{
			sharedDomains: &branchChildCountDomains{bound: true, maxStep: 1},
			stateReader:   reader,
		}

		_, err := sdc.BranchChildCount(prefix)
		require.ErrorContains(t, err, "staged unwind")
	})
}

func Test_TrieContext_BranchReusesBufferAcrossReads(t *testing.T) {
	t.Parallel()

	reader := &testStateReader{branchData: []byte{1, 2, 3}, step: 7}
	ctx := NewTrieContextRo(reader, 1)

	got1, _, err := ctx.Branch([]byte{0xaa})
	require.NoError(t, err)
	require.Equal(t, []byte{1, 2, 3}, got1)

	reader.branchData = []byte{4, 5, 6}
	got2, _, err := ctx.Branch([]byte{0xbb})
	require.NoError(t, err)
	require.Equal(t, []byte{4, 5, 6}, got2)

	// The contract Branch's callers rely on: the returned bytes live only until the
	// next read on this context. A caller that keeps them sees the newer branch.
	require.Equal(t, []byte{4, 5, 6}, got1, "second read must land in the same buffer")
	require.Equal(t, &got1[0], &got2[0], "second read must not allocate a new buffer")
}

func Test_TrieContext_BranchKeepsNilAndEmptyDistinct(t *testing.T) {
	t.Parallel()

	// A nil branch means "absent"; callers test it with == nil, so reusing a buffer
	// must not turn it into an empty non-nil slice.
	reader := &testStateReader{}
	ctx := NewTrieContextRo(reader, 1)

	got, _, err := ctx.Branch([]byte{0xaa})
	require.NoError(t, err)
	require.Nil(t, got, "absent branch must stay nil")

	reader.branchData = []byte{1, 2, 3}
	if _, _, err = ctx.Branch([]byte{0xbb}); err != nil {
		t.Fatal(err)
	}

	reader.branchData = []byte{}
	got, _, err = ctx.Branch([]byte{0xcc})
	require.NoError(t, err)
	require.NotNil(t, got, "present-but-empty branch must stay non-nil after the buffer is warm")
	require.Empty(t, got)

	reader.branchData = nil
	got, _, err = ctx.Branch([]byte{0xdd})
	require.NoError(t, err)
	require.Nil(t, got, "absent branch must stay nil after the buffer is warm")
}
