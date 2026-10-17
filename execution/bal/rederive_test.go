package bal

import (
	"context"
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// replayTestReader serves a few accounts and fails every storage read.
type replayTestReader struct {
	*state.NoopReader
	accounts   map[accounts.Address]accounts.Account
	code       map[accounts.Address][]byte
	storageErr error
}

func (r *replayTestReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	if acc, ok := r.accounts[addr]; ok {
		return &acc, nil
	}
	return nil, nil
}

func (r *replayTestReader) ReadAccountByPlainAddr(addr common.Address) (*accounts.Account, error) {
	return r.ReadAccountData(accounts.InternAddress(addr))
}

func (r *replayTestReader) HasAccount(addr common.Address) (bool, error) {
	_, ok := r.accounts[accounts.InternAddress(addr)]
	return ok, nil
}

func (r *replayTestReader) ReadAccountCode(addr accounts.Address) ([]byte, error) {
	return r.code[addr], nil
}

func (r *replayTestReader) ReadAccountCodeSize(addr accounts.Address) (int, error) {
	return len(r.code[addr]), nil
}

func (r *replayTestReader) ReadAccountStorage(accounts.Address, accounts.StorageKey) (uint256.Int, bool, error) {
	return uint256.Int{}, false, r.storageErr
}

// TestReplayBlockPrefixSurfacesStateReadError pins that a failed state read
// inside a replayed transaction is reported, instead of being folded into the
// I/O as a failed transaction that would then misrepresent the block.
func TestReplayBlockPrefixSurfacesStateReadError(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := accounts.InternAddress(crypto.PubkeyToAddress(key.PublicKey))
	contractAddr := common.HexToAddress("0x000000000000000000000000000000000000c0de")
	contract := accounts.InternAddress(contractAddr)
	code := []byte{byte(vm.PUSH1), 0, byte(vm.SLOAD), byte(vm.STOP)}

	senderAcc := accounts.NewAccount()
	senderAcc.Balance = *uint256.NewInt(1_000_000_000_000_000_000)
	contractAcc := accounts.NewAccount()
	contractAcc.Nonce = 1
	contractAcc.Incarnation = state.FirstContractIncarnation
	contractAcc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	readErr := errors.New("storage read failed")
	reader := &replayTestReader{
		NoopReader: state.NewNoopReader(),
		accounts:   map[accounts.Address]accounts.Account{sender: senderAcc, contract: contractAcc},
		code:       map[accounts.Address][]byte{contract: code},
		storageErr: readErr,
	}
	ibs := state.New(reader)
	defer ibs.Close()
	ibs.SetVersionMap(state.NewVersionMap(nil))

	config := chain.TestChainBerlinConfig
	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 10_000_000, Difficulty: *uint256.NewInt(1)}
	txn, err := types.SignTx(&types.LegacyTx{
		CommonTx: types.CommonTx{Nonce: 0, To: &contractAddr, GasLimit: 100_000},
		GasPrice: *uint256.NewInt(1),
	}, *types.MakeSigner(config, 1, 0), key)
	require.NoError(t, err)

	_, err = ReplayBlockPrefix(context.Background(), config, ethash.NewFaker(), nil, nil, header, types.Transactions{txn}, ibs, &state.VersionedIO{}, log.New())
	require.ErrorIs(t, err, readErr)
}
