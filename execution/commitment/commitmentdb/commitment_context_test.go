package commitmentdb

import (
	"context"
	"math/rand"
	"testing"

	"github.com/erigontech/erigon/db/kv"
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
}

var _ StateReader = (*testStateReader)(nil)

func (r *testStateReader) WithHistory() bool { return false }

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
