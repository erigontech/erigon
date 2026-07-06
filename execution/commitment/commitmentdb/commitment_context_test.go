package commitmentdb

import (
	"context"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
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

// TestTouchKeyWrite_DomainLengthMismatch pins that a write touch whose key length
// disagrees with the fold's length-based classification (accounts length.Addr,
// storage length.Addr+length.Hash) is never carried: the fold would land the
// update in a differently-typed cell. Mismatches keep the nil/re-read delivery.
func TestTouchKeyWrite_DomainLengthMismatch(t *testing.T) {
	t.Parallel()

	acc := accounts.Account{Nonce: 1, Balance: *uint256.NewInt(7), CodeHash: accounts.EmptyCodeHash}
	accBlob := accounts.SerialiseV3(&acc)
	accountKey := string(make([]byte, length.Addr))
	storageKey := string(make([]byte, length.Addr+length.Hash))

	delivered := func(sdc *SharedDomainsCommitmentContext) map[string]*commitment.Update {
		t.Helper()
		got := map[string]*commitment.Update{}
		err := sdc.updates.HashSort(context.Background(), nil, func(hk, pk []byte, upd *commitment.Update) error {
			var cp *commitment.Update
			if upd != nil {
				cp = new(commitment.Update)
				*cp = *upd
			}
			got[string(pk)] = cp
			return nil
		})
		require.NoError(t, err)
		return got
	}

	t.Run("matched lengths carry", func(t *testing.T) {
		sdc := NewSharedDomainsCommitmentContext(nil, commitment.ModeDirect, t.TempDir(), commitment.DefaultTrieConfig())
		sdc.TouchKeyWrite(kv.AccountsDomain, accountKey, accBlob)
		sdc.TouchKeyWrite(kv.StorageDomain, storageKey, []byte{1})
		got := delivered(sdc)
		require.NotNil(t, got[accountKey])
		require.NotNil(t, got[storageKey])
	})

	t.Run("mismatched lengths degrade to re-read", func(t *testing.T) {
		sdc := NewSharedDomainsCommitmentContext(nil, commitment.ModeDirect, t.TempDir(), commitment.DefaultTrieConfig())
		sdc.TouchKeyWrite(kv.AccountsDomain, storageKey, accBlob)
		sdc.TouchKeyWrite(kv.StorageDomain, accountKey, []byte{1})
		got := delivered(sdc)
		require.Len(t, got, 2)
		require.Nil(t, got[storageKey], "account write under a storage-length key must not carry")
		require.Nil(t, got[accountKey], "storage write under an account-length key must not carry")
	})
}
