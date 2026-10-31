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
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
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

// TestRederiveBlockAccessListRefusesAccountAbstractionTx pins that a RIP-7560
// transaction, which the live executor runs on its own path, is reported as the
// node's failure: the replay cannot judge it.
func TestRederiveBlockAccessListRefusesAccountAbstractionTx(t *testing.T) {
	t.Parallel()
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	ibs.SetVersionMap(state.NewVersionMap(nil))
	header := &types.Header{Number: *uint256.NewInt(5), GasLimit: 10_000_000, Difficulty: *uint256.NewInt(1)}
	txns := types.Transactions{&types.AccountAbstractionTransaction{ChainID: uint256.NewInt(1), GasLimit: 100_000, Tip: new(uint256.Int), FeeCap: new(uint256.Int)}}
	_, err := RederiveBlockAccessList(context.Background(), chain.TestChainBerlinConfig, ethash.NewFaker(), nil, noHeader, header, txns, nil, nil, ibs, log.New())
	require.Error(t, err)
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
