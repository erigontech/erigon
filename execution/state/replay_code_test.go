package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type mapCodeByHash struct {
	code  map[common.Hash][]byte
	txNum map[common.Hash]uint64
}

func newMapCodeByHash() *mapCodeByHash {
	return &mapCodeByHash{code: map[common.Hash][]byte{}, txNum: map[common.Hash]uint64{}}
}

func (m *mapCodeByHash) GetCodeByHash(codeHash []byte) ([]byte, bool) {
	c, ok := m.code[common.BytesToHash(codeHash)]
	return c, ok
}

func (m *mapCodeByHash) PutCodeByHash(codeHash, code []byte, txNum uint64) {
	m.code[common.BytesToHash(codeHash)] = code
	m.txNum[common.BytesToHash(codeHash)] = txNum
}

type historicalCodeReader struct {
	codeReader
	txNum                    uint64
	codeReads, codeSizeReads int
}

func (r *historicalCodeReader) GetTxNum() uint64 { return r.txNum }

func (r *historicalCodeReader) ReadAccountCode(addr accounts.Address) ([]byte, error) {
	r.codeReads++
	return r.codeReader.ReadAccountCode(addr)
}

func (r *historicalCodeReader) ReadAccountCodeSize(addr accounts.Address) (int, error) {
	r.codeSizeReads++
	return r.codeReader.ReadAccountCodeSize(addr)
}

var replayCode = []byte{0x60, 0x01, 0x60, 0x02, 0x01, 0x00}

// newReplayState is one RPC replay over a historical reader that shares code through store.
func newReplayState(store CodeByHash) (*IntraBlockState, *historicalCodeReader, accounts.Address) {
	addr := accounts.InternAddress([20]byte{0xC0, 0xDE})
	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(replayCode))
	reader := &historicalCodeReader{codeReader: codeReader{addr: addr, account: &acc, code: replayCode}, txNum: 777}
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetNoConflictDetection()
	ibs.SetCodeByHash(store)
	ibs.SetTxContext(100, 0)
	return ibs, reader, addr
}

// endReplayTx ends a tx the way a multi-tx RPC replay does: the tx's writes go to the
// version map and the next tx starts with an empty read set.
func endReplayTx(t *testing.T, ibs *IntraBlockState) {
	t.Helper()
	writes, err := ibs.FinalizedWrites(nil)
	require.NoError(t, err)
	ibs.FlushWritesToVersionMap(writes)
	ibs.SoftFinalise()
	ibs.ResetVersionedIO()
	ibs.SetTxContext(100, ibs.TxnIndex()+1)
}

func TestReplayReadsCommittedCodeOnceAcrossTxs(t *testing.T) {
	ibs, reader, addr := newReplayState(newMapCodeByHash())

	for range 3 {
		got, err := ibs.GetCode(addr)
		require.NoError(t, err)
		require.Equal(t, replayCode, got)
		endReplayTx(t, ibs)
	}
	require.Equal(t, 1, reader.codeReads)
}

func TestReplayCodeSizeReusesSharedCode(t *testing.T) {
	ibs, reader, addr := newReplayState(newMapCodeByHash())

	_, err := ibs.GetCode(addr)
	require.NoError(t, err)
	endReplayTx(t, ibs)

	size, err := ibs.GetCodeSize(addr)
	require.NoError(t, err)
	require.Equal(t, len(replayCode), size)
	require.Zero(t, reader.codeSizeReads)
}

func TestReplaySharesCommittedCodeAcrossRequests(t *testing.T) {
	store := newMapCodeByHash()

	reads := 0
	for range 2 {
		ibs, reader, addr := newReplayState(store)
		got, err := ibs.GetCode(addr)
		require.NoError(t, err)
		require.Equal(t, replayCode, got)
		reads += reader.codeReads
	}
	require.Equal(t, 1, reads)
	require.Equal(t, uint64(777), store.txNum[crypto.Keccak256Hash(replayCode)])
}
