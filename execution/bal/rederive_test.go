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
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// rederiveTestReader serves a funded sender and one contract, and fails every
// storage read with storageErr.
type rederiveTestReader struct {
	*state.NoopReader
	accounts   map[accounts.Address]accounts.Account
	code       map[accounts.Address][]byte
	storageErr error
}

func (r *rederiveTestReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	if acc, ok := r.accounts[addr]; ok {
		return &acc, nil
	}
	return nil, nil
}

func (r *rederiveTestReader) ReadAccountCode(addr accounts.Address) ([]byte, error) {
	return r.code[addr], nil
}

func (r *rederiveTestReader) ReadAccountCodeSize(addr accounts.Address) (int, error) {
	return len(r.code[addr]), nil
}

func (r *rederiveTestReader) ReadAccountStorage(accounts.Address, accounts.StorageKey) (uint256.Int, bool, error) {
	return uint256.Int{}, false, r.storageErr
}

// noHeader is a header lookup that finds nothing.
func noHeader(common.Hash, uint64) (*types.Header, error) { return nil, nil }

// rederiveCall re-derives a block whose only transaction calls a contract
// running code.
func rederiveCall(t *testing.T, code []byte, storageErr error, getHeader GetHeaderFunc) error {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := accounts.InternAddress(crypto.PubkeyToAddress(key.PublicKey))
	contractAddr := common.HexToAddress("0x000000000000000000000000000000000000c0de")
	contract := accounts.InternAddress(contractAddr)

	senderAcc := accounts.NewAccount()
	senderAcc.Balance = *uint256.NewInt(1_000_000_000_000_000_000)
	contractAcc := accounts.NewAccount()
	contractAcc.Nonce = 1
	contractAcc.Incarnation = state.FirstContractIncarnation
	contractAcc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	ibs := state.New(&rederiveTestReader{
		NoopReader: state.NewNoopReader(),
		accounts:   map[accounts.Address]accounts.Account{sender: senderAcc, contract: contractAcc},
		code:       map[accounts.Address][]byte{contract: code},
		storageErr: storageErr,
	})
	defer ibs.Close()
	ibs.SetVersionMap(state.NewVersionMap(nil))

	config := chain.TestChainBerlinConfig
	header := &types.Header{Number: *uint256.NewInt(5), GasLimit: 10_000_000, Difficulty: *uint256.NewInt(1)}
	txn, err := types.SignTx(&types.LegacyTx{
		CommonTx: types.CommonTx{Nonce: 0, To: &contractAddr, GasLimit: 100_000},
		GasPrice: *uint256.NewInt(1),
	}, *types.MakeSigner(config, 5, 0), key)
	require.NoError(t, err)

	_, err = RederiveBlockAccessList(context.Background(), config, ethash.NewFaker(), nil, getHeader, header, types.Transactions{txn}, nil, nil, ibs, log.New())
	return err
}

// TestRederiveBlockAccessListReportsStateReadError pins that a failed state
// read is reported as the node's failure: the EVM folds it into a failed call,
// which would otherwise misrepresent the block.
func TestRederiveBlockAccessListReportsStateReadError(t *testing.T) {
	t.Parallel()
	readErr := errors.New("storage read failed")
	err := rederiveCall(t, []byte{byte(vm.PUSH1), 0, byte(vm.SLOAD), byte(vm.STOP)}, readErr, noHeader)
	require.ErrorIs(t, err, readErr)
	require.NotErrorIs(t, err, rules.ErrInvalidBlock)
}

// TestRederiveBlockAccessListReportsHeaderLookupError pins that a failed
// ancestor header lookup is reported as the node's failure: BLOCKHASH folds it
// into a failed call, which would otherwise misrepresent the block.
func TestRederiveBlockAccessListReportsHeaderLookupError(t *testing.T) {
	t.Parallel()
	lookupErr := errors.New("header read failed")
	getHeader := func(common.Hash, uint64) (*types.Header, error) { return nil, lookupErr }
	err := rederiveCall(t, []byte{byte(vm.PUSH1), 1, byte(vm.BLOCKHASH), byte(vm.STOP)}, nil, getHeader)
	require.ErrorIs(t, err, lookupErr)
	require.NotErrorIs(t, err, rules.ErrInvalidBlock)
}

// TestRederiveBlockAccessListReportsRecoveredPanic pins that a panic recovered
// from a replayed transaction is reported as the node's failure, as the live
// executor reports it.
func TestRederiveBlockAccessListReportsRecoveredPanic(t *testing.T) {
	t.Parallel()
	getHeader := func(common.Hash, uint64) (*types.Header, error) { panic("header read panicked") }
	err := rederiveCall(t, []byte{byte(vm.PUSH1), 1, byte(vm.BLOCKHASH), byte(vm.STOP)}, nil, getHeader)
	require.ErrorIs(t, err, protocol.ErrExecPanic)
	require.NotErrorIs(t, err, rules.ErrInvalidBlock)
}

// TestRederiveBlockAccessListReportsMissingHeader pins that an ancestor header
// lookup that finds nothing is reported as the node's failure: BLOCKHASH would
// otherwise read a zero hash.
func TestRederiveBlockAccessListReportsMissingHeader(t *testing.T) {
	t.Parallel()
	err := rederiveCall(t, []byte{byte(vm.PUSH1), 1, byte(vm.BLOCKHASH), byte(vm.STOP)}, nil, noHeader)
	require.ErrorContains(t, err, "not found")
	require.NotErrorIs(t, err, rules.ErrInvalidBlock)
}

func TestRederiveSystemCallBlockHash(t *testing.T) {
	code := []byte{byte(vm.PUSH1), 2, byte(vm.NUMBER), byte(vm.SUB), byte(vm.BLOCKHASH), byte(vm.PUSH0), byte(vm.SSTORE), byte(vm.STOP)}
	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Incarnation = state.FirstContractIncarnation
	acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	for _, contract := range []accounts.Address{params.BeaconRootsAddress, params.WithdrawalRequestAddress} {
		t.Run(contract.Value().String(), func(t *testing.T) {
			emptyCode := []byte{byte(vm.STOP)}
			emptyAcc := acc
			emptyAcc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(emptyCode))
			reader := &rederiveTestReader{NoopReader: state.NewNoopReader(), accounts: map[accounts.Address]accounts.Account{
				params.WithdrawalRequestAddress: emptyAcc, params.ConsolidationRequestAddress: emptyAcc, contract: acc,
			}, code: map[accounts.Address][]byte{
				params.WithdrawalRequestAddress: emptyCode, params.ConsolidationRequestAddress: emptyCode, contract: code,
			}}
			ibs := state.New(reader)
			defer ibs.Close()
			ibs.SetVersionMap(state.NewVersionMap(nil))
			ancestor := &types.Header{Number: *uint256.NewInt(1)}
			parent := &types.Header{Number: *uint256.NewInt(2), ParentHash: ancestor.Hash()}
			header := &types.Header{Number: *uint256.NewInt(3), ParentHash: parent.Hash(), GasLimit: 30_000_000, ParentBeaconBlockRoot: new(common.Hash)}
			getHeader := func(hash common.Hash, number uint64) (*types.Header, error) {
				require.Equal(t, uint64(2), number)
				require.Equal(t, parent.Hash(), hash)
				return parent, nil
			}
			vio, err := RederiveBlockAccessList(t.Context(), chain.TestChainOsakaConfig, merge.New(ethash.NewFaker()), nil, getHeader, header, nil, nil, nil, ibs, log.New())
			require.NoError(t, err)
			var got *uint256.Int
			for _, account := range vio.AsBlockAccessList() {
				if account.Address == contract.Value() {
					for _, slot := range account.StorageChanges {
						if slot.Slot == accounts.ZeroKey && len(slot.Changes) > 0 {
							got = &slot.Changes[0].Value
						}
					}
				}
			}
			require.NotNil(t, got)
			require.Equal(t, ancestor.Hash().U256(), *got)
		})
	}
}
