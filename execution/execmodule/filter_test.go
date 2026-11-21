package execmodule

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// nonceReaderStub is a minimal state.StateReader returning settable accounts by address (only ReadAccountData
// is meaningful — filterCandidatesByNonce reads nothing else).
type nonceReaderStub struct {
	accts map[accounts.Address]*accounts.Account
}

func (m *nonceReaderStub) ReadAccountData(a accounts.Address) (*accounts.Account, error) {
	return m.accts[a], nil
}
func (m *nonceReaderStub) ReadAccountDataForDebug(a accounts.Address) (*accounts.Account, error) {
	return m.accts[a], nil
}
func (m *nonceReaderStub) ReadAccountStorage(accounts.Address, accounts.StorageKey) (uint256.Int, bool, error) {
	return uint256.Int{}, false, nil
}
func (m *nonceReaderStub) ReadAccountByPlainAddr(a common.Address) (*accounts.Account, error) {
	return m.ReadAccountData(accounts.InternAddress(a))
}
func (m *nonceReaderStub) HasAccount(a common.Address) (bool, error) {
	acc, err := m.ReadAccountData(accounts.InternAddress(a))
	return acc != nil, err
}
func (m *nonceReaderStub) HasStorage(accounts.Address) (bool, error)               { return false, nil }
func (m *nonceReaderStub) ReadAccountCode(accounts.Address) ([]byte, error)        { return nil, nil }
func (m *nonceReaderStub) ReadAccountCodeSize(accounts.Address) (int, error)       { return 0, nil }
func (m *nonceReaderStub) ReadAccountIncarnation(accounts.Address) (uint64, error) { return 0, nil }
func (m *nonceReaderStub) SetTrace(bool, string)                                   {}
func (m *nonceReaderStub) Trace() bool                                             { return false }
func (m *nonceReaderStub) TracePrefix() string                                     { return "" }

// ISOLATED (candidate filter feature): filterCandidatesByNonce — run at the start of the pre-exec cycle — must
// DROP a stale (nonce-too-low) candidate and NOT include a future/gapped one, keeping the applicable ones in
// order, so an invalid candidate is filtered rather than breaking block execution.
func TestFilterCandidatesByNonce_DropsStaleKeepsApplicableRequeuesFuture(t *testing.T) {
	signer := types.LatestSignerForChainID(chain.AllProtocolChanges.ChainID)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	mk := func(nonce uint64) types.Transaction {
		tx := types.NewTransaction(nonce, common.Address{0xaa}, uint256.NewInt(0), 21000, uint256.NewInt(0), nil)
		signed, serr := types.SignTx(tx, *signer, key)
		require.NoError(t, serr)
		s, serr := signer.Sender(signed)
		require.NoError(t, serr)
		signed.SetSender(s)
		return signed
	}
	txs := []types.Transaction{mk(3), mk(5), mk(6), mk(9)} // 3=stale, 5+6=applicable, 9=future(gap)
	sender, ok := txs[0].GetSender()
	require.True(t, ok)
	acct := &accounts.Account{Nonce: 5, CodeHash: accounts.EmptyCodeHash}
	acct.Balance.SetUint64(1_000_000_000)
	r := &nonceReaderStub{accts: map[accounts.Address]*accounts.Account{sender: acct}}

	out := filterCandidatesByNonce(r, signer, txs)
	require.Len(t, out, 2, "only applicable nonces 5 and 6 survive")
	require.Equal(t, uint64(5), out[0].GetNonce())
	require.Equal(t, uint64(6), out[1].GetNonce())
}

func signedRLP(t *testing.T, signer *types.Signer, k *ecdsa.PrivateKey, nonce uint64) []byte {
	t.Helper()
	tx := types.NewTransaction(nonce, common.Address{0xaa}, uint256.NewInt(0), 21000, uint256.NewInt(0), nil)
	signed, err := types.SignTx(tx, *signer, k)
	require.NoError(t, err)
	var buf bytes.Buffer
	require.NoError(t, signed.MarshalBinary(&buf))
	return buf.Bytes()
}

func TestJudgeInCommitOrder_AGappedNonceIsDroppedWhereTheDAGPutIt(t *testing.T) {
	signer := types.LatestSignerForChainID(chain.AllProtocolChanges.ChainID)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	cands, bad := decodeCandidates([][]byte{signedRLP(t, signer, key, 9), signedRLP(t, signer, key, 7), signedRLP(t, signer, key, 8)}, signer)
	require.Zero(t, bad)

	v := judgeInCommitOrder(cands, func(accounts.Address) uint64 { return 7 }, map[snKeyExec]bool{}, nil)

	require.Len(t, v.kept, 2)
	require.Equal(t, []uint64{7, 8}, []uint64{v.kept[0].nonce, v.kept[1].nonce})
	require.Empty(t, v.dropped)
	require.Equal(t, []common.Hash{cands[0].hash}, v.deferred, "nonce 9 came before 7 in commit order")
	require.Equal(t, []common.Hash{cands[0].hash}, v.decidedAgainst(true), "commit-order execution decides a gapped nonce")
	require.Empty(t, v.decidedAgainst(false), "otherwise a gapped nonce waits for its predecessor")
	require.Equal(t, 1, v.future)
}

func TestJudgeInCommitOrder_KeepsTheOfferedOrderAcrossSenders(t *testing.T) {
	signer := types.LatestSignerForChainID(chain.AllProtocolChanges.ChainID)
	keyA, err := crypto.GenerateKey()
	require.NoError(t, err)
	keyB, err := crypto.GenerateKey()
	require.NoError(t, err)
	cands, bad := decodeCandidates([][]byte{signedRLP(t, signer, keyB, 4), signedRLP(t, signer, keyA, 1), signedRLP(t, signer, keyB, 5)}, signer)
	require.Zero(t, bad)
	expected := map[accounts.Address]uint64{cands[0].sender: 4, cands[1].sender: 1}

	v := judgeInCommitOrder(cands, func(a accounts.Address) uint64 { return expected[a] }, map[snKeyExec]bool{}, nil)

	require.Len(t, v.kept, 3)
	for i := range cands {
		require.Equal(t, cands[i].hash, v.kept[i].hash)
	}
	require.Empty(t, v.dropped)
}

func TestJudgeInCommitOrder_ADecidedPairIsNotJudgedAgain(t *testing.T) {
	signer := types.LatestSignerForChainID(chain.AllProtocolChanges.ChainID)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	cands, _ := decodeCandidates([][]byte{signedRLP(t, signer, key, 3), signedRLP(t, signer, key, 2)}, signer)
	seen := map[snKeyExec]bool{}

	first := judgeInCommitOrder(cands, func(accounts.Address) uint64 { return 2 }, seen, nil)
	require.Len(t, first.kept, 1)
	require.False(t, seen[snKeyExec{addr: cands[0].sender, nonce: 3}], "a gapped nonce is not decided, so it may be offered again")

	again := judgeInCommitOrder(cands[1:], func(accounts.Address) uint64 { return 3 }, seen, nil)
	require.Empty(t, again.kept)
	require.Equal(t, 1, again.seen)
}

func TestOrderBySenderNonce_RestoresEachSendersSequenceAndKeepsSendersInArrivalOrder(t *testing.T) {
	signer := types.LatestSignerForChainID(chain.AllProtocolChanges.ChainID)
	keyA, err := crypto.GenerateKey()
	require.NoError(t, err)
	keyB, err := crypto.GenerateKey()
	require.NoError(t, err)
	cands, _ := decodeCandidates([][]byte{signedRLP(t, signer, keyB, 5), signedRLP(t, signer, keyA, 1), signedRLP(t, signer, keyB, 4)}, signer)

	got := orderBySenderNonce(cands)

	require.Equal(t, []common.Hash{cands[2].hash, cands[0].hash, cands[1].hash}, []common.Hash{got[0].hash, got[1].hash, got[2].hash})
}

func TestJudge_SenderNonceOrderKeepsARoundWhoseNoncesArriveOutOfOrder(t *testing.T) {
	signer := types.LatestSignerForChainID(chain.AllProtocolChanges.ChainID)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	cands, _ := decodeCandidates([][]byte{signedRLP(t, signer, key, 9), signedRLP(t, signer, key, 7), signedRLP(t, signer, key, 8)}, signer)

	v := judgeInCommitOrder(orderBySenderNonce(cands), func(accounts.Address) uint64 { return 7 }, map[snKeyExec]bool{}, nil)

	require.Len(t, v.kept, 3)
	require.Empty(t, v.deferred)
}

type plainAssembler struct{}

func (plainAssembler) NewPayloadAttrs(context.Context, *builder.Parameters) error   { return nil }
func (plainAssembler) AssembleBlock(context.Context, *builder.Parameters) error     { return nil }
func (plainAssembler) GetAssembledBlock(context.Context, *builder.Parameters) error { return nil }

type commitOrderAssembler struct{ plainAssembler }

func (commitOrderAssembler) CommitOrderExecution() bool { return true }

func TestAnAssemblerChoosesCommitOrderExecution(t *testing.T) {
	e := &ExecModule{}
	e.SetBlockAssembler(plainAssembler{})
	require.False(t, e.flash.commitOrder, "an assembler that does not ask keeps sender-nonce order")
	e.SetBlockAssembler(commitOrderAssembler{})
	require.True(t, e.flash.commitOrder)
}
