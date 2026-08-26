package execmodule

import (
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
type nonceReaderStub struct{ accts map[accounts.Address]*accounts.Account }

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
