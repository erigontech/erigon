package execmodule

import (
	"bytes"
	"crypto/ecdsa"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
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

// A SENDER'S CONSECUTIVE NONCES MUST SURVIVE ARRIVING OUT OF ORDER.
//
// The round's batch comes in DAG COMMIT order, which says nothing about nonces, so a sender that
// submits N and N+1 together can be offered N+1 first. Judged in that order, N+1 is `future` against
// a state still expecting N, and it is refused — then it goes back through the pool and the DAG and
// lands a block later. Measured on live225, one round of a four-order burst:
//
//	[TRACE-filter] round  block=262  in=2  kept=1  future=1
//
// Two consecutive nonces from one sender, one kept and one refused. Placement costs 40ms when
// nothing is queued behind it, so each refusal cost roughly fifty times the work it was waiting on.
func TestOrderBySenderNonce_RestoresOneSendersSequence(t *testing.T) {
	signer := types.LatestSignerForChainID(chain.AllProtocolChanges.ChainID)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	rlpOf := func(nonce uint64) []byte {
		tx := types.NewTransaction(nonce, common.Address{0xaa}, uint256.NewInt(0), 21000, uint256.NewInt(0), nil)
		signed, serr := types.SignTx(tx, *signer, key)
		require.NoError(t, serr)
		var buf bytes.Buffer
		require.NoError(t, signed.MarshalBinary(&buf))
		return buf.Bytes()
	}

	// Offered 9, 7, 8 — the shape the DAG actually produces for a burst from one sponsor.
	cands, bad := orderBySenderNonce([][]byte{rlpOf(9), rlpOf(7), rlpOf(8)}, signer)
	require.Zero(t, bad)
	require.Len(t, cands, 3)
	require.Equal(t, []uint64{7, 8, 9}, []uint64{cands[0].nonce, cands[1].nonce, cands[2].nonce},
		"a sender's own nonces have exactly one valid order and the filter must see them in it")
}

// ⚠ AND SENDERS ARE NOT REORDERED AGAINST EACH OTHER. Only a sender's own sequence is restored;
// whose transaction came first in the round is the DAG's decision, not this function's.
func TestOrderBySenderNonce_KeepsSendersInArrivalOrder(t *testing.T) {
	signer := types.LatestSignerForChainID(chain.AllProtocolChanges.ChainID)
	keyA, err := crypto.GenerateKey()
	require.NoError(t, err)
	keyB, err := crypto.GenerateKey()
	require.NoError(t, err)
	rlpOf := func(k *ecdsa.PrivateKey, nonce uint64) []byte {
		tx := types.NewTransaction(nonce, common.Address{0xaa}, uint256.NewInt(0), 21000, uint256.NewInt(0), nil)
		signed, serr := types.SignTx(tx, *signer, k)
		require.NoError(t, serr)
		var buf bytes.Buffer
		require.NoError(t, signed.MarshalBinary(&buf))
		return buf.Bytes()
	}

	// B arrives first with a HIGH nonce; A follows with a low one. B must stay first.
	cands, bad := orderBySenderNonce([][]byte{rlpOf(keyB, 5), rlpOf(keyA, 1), rlpOf(keyB, 4)}, signer)
	require.Zero(t, bad)
	require.Len(t, cands, 3)
	addrB, aerr := signer.Sender(func() types.Transaction {
		tx := types.NewTransaction(5, common.Address{0xaa}, uint256.NewInt(0), 21000, uint256.NewInt(0), nil)
		signed, serr := types.SignTx(tx, *signer, keyB)
		require.NoError(t, serr)
		return signed
	}())
	require.NoError(t, aerr)

	require.Equal(t, addrB, cands[0].sender, "the sender that arrived first stays first")
	require.Equal(t, uint64(4), cands[0].nonce, "but its own nonces are in order")
	require.Equal(t, addrB, cands[1].sender)
	require.Equal(t, uint64(5), cands[1].nonce)
	require.NotEqual(t, addrB, cands[2].sender, "the later sender stays later")
}
