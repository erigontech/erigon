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

package exec

import (
	"context"
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

type assemblerStateReader struct {
	accounts map[accounts.Address]accounts.Account
	code     map[accounts.Address][]byte
}

func (r *assemblerStateReader) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	account, ok := r.accounts[address]
	if !ok {
		return nil, nil
	}
	return &account, nil
}

func (r *assemblerStateReader) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return r.ReadAccountData(address)
}

func (*assemblerStateReader) ReadAccountStorage(accounts.Address, accounts.StorageKey) (uint256.Int, bool, error) {
	return uint256.Int{}, false, nil
}

func (r *assemblerStateReader) ReadAccountCode(address accounts.Address) ([]byte, error) {
	return append([]byte(nil), r.code[address]...), nil
}

func (r *assemblerStateReader) ReadAccountCodeSize(address accounts.Address) (int, error) {
	return len(r.code[address]), nil
}

func (*assemblerStateReader) ReadAccountIncarnation(accounts.Address) (uint64, error) { return 0, nil }
func (*assemblerStateReader) SetTrace(bool, string)                                   {}
func (*assemblerStateReader) Trace() bool                                             { return false }
func (*assemblerStateReader) TracePrefix() string                                     { return "" }

func TestRequiredSuccessTransactionIsDiscardedOnlyWhenItsReceiptFails(t *testing.T) {
	hash := common.Hash{0x42}
	requiresSuccess := func(candidate common.Hash) bool { return candidate == hash }

	require.False(t, shouldDiscardFailedTransaction(hash, &types.Receipt{Status: types.ReceiptStatusSuccessful}, requiresSuccess))
	require.True(t, shouldDiscardFailedTransaction(hash, &types.Receipt{Status: types.ReceiptStatusFailed}, requiresSuccess))
	require.False(t, shouldDiscardFailedTransaction(common.Hash{0x43}, &types.Receipt{Status: types.ReceiptStatusFailed}, requiresSuccess))
	require.False(t, shouldDiscardFailedTransaction(hash, &types.Receipt{Status: types.ReceiptStatusFailed}, nil))
}

func TestTransactionDependencyRequiresTargetToBeIncluded(t *testing.T) {
	targetHash := common.Hash{0x41}
	privateHash := common.Hash{0x42}
	dependency := func(hash common.Hash) (common.Hash, bool) {
		return targetHash, hash == privateHash
	}
	included := map[common.Hash]struct{}{targetHash: {}}

	require.True(t, transactionDependencySatisfied(privateHash, included, dependency))
	delete(included, targetHash)
	require.False(t, transactionDependencySatisfied(privateHash, included, dependency))
	require.True(t, transactionDependencySatisfied(common.Hash{0x43}, included, dependency))
	require.True(t, transactionDependencySatisfied(privateHash, included, nil))
}

func TestAddTransactionsRollsBackRevertingRequiredSuccessTransaction(t *testing.T) {
	config := chain.AllProtocolChanges.Copy()
	config.AmsterdamTime = nil
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	revertingContract := common.Address{0x41}
	recipient := common.Address{0x42}
	revertingCode := []byte{0xfe}

	senderAccount := accounts.NewAccount()
	senderAccount.Balance.SetUint64(1_000_000_000)
	contractAccount := accounts.NewAccount()
	contractAccount.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(revertingCode))
	contractAccount.Incarnation = 1
	reader := &assemblerStateReader{
		accounts: map[accounts.Address]accounts.Account{
			accounts.InternAddress(sender):            senderAccount,
			accounts.InternAddress(revertingContract): contractAccount,
		},
		code: map[accounts.Address][]byte{
			accounts.InternAddress(revertingContract): revertingCode,
		},
	}
	ibs := state.New(reader)
	header := types.NewEmptyHeaderForAssembling()
	header.Number.SetUint64(1)
	header.GasLimit = 1_000_000
	header.Time = 1
	header.BaseFee = uint256.NewInt(1)
	block := &AssembledBlock{Header: header}
	assembler := NewBlockAssembler(AssemblerCfg{ChainConfig: config, Engine: ethash.NewFaker()}, block)
	signer := types.LatestSigner(config)
	revertingTxn, err := types.SignTx(types.NewTransaction(0, revertingContract, nil, 50_000, uint256.NewInt(2), nil), *signer, key)
	require.NoError(t, err)
	succeedingTxn, err := types.SignTx(types.NewTransaction(0, recipient, uint256.NewInt(1), 50_000, uint256.NewInt(2), nil), *signer, key)
	require.NoError(t, err)

	require.NotPanics(t, func() {
		_, _, err = assembler.AddTransactions(
			context.Background(),
			func(common.Hash, uint64) (*types.Header, error) { return nil, nil },
			[]types.Transaction{revertingTxn, succeedingTxn},
			accounts.NilAddress,
			&vm.Config{},
			ibs,
			func(hash common.Hash) bool { return hash == revertingTxn.Hash() },
			nil,
			nil,
			"test",
			log.Root(),
		)
	})
	require.NoError(t, err)
	require.Equal(t, types.Transactions{succeedingTxn}, block.Txns)
	require.Len(t, block.Receipts, 1)
	require.Equal(t, uint64(types.ReceiptStatusSuccessful), block.Receipts[0].Status)
	require.Equal(t, block.Receipts[0].CumulativeGasUsed, header.GasUsed)
	nonce, err := ibs.GetNonce(accounts.InternAddress(sender))
	require.NoError(t, err)
	require.Equal(t, uint64(1), nonce)
}

func TestAddTransactionsDoesNotSatisfyDependencyWithRevertedTarget(t *testing.T) {
	config := chain.AllProtocolChanges.Copy()
	config.AmsterdamTime = nil
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	revertingContract := common.Address{0x41}
	recipient := common.Address{0x42}
	revertingCode := []byte{0xfe}

	senderAccount := accounts.NewAccount()
	senderAccount.Balance.SetUint64(1_000_000_000)
	contractAccount := accounts.NewAccount()
	contractAccount.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(revertingCode))
	contractAccount.Incarnation = 1
	ibs := state.New(&assemblerStateReader{
		accounts: map[accounts.Address]accounts.Account{
			accounts.InternAddress(sender):            senderAccount,
			accounts.InternAddress(revertingContract): contractAccount,
		},
		code: map[accounts.Address][]byte{
			accounts.InternAddress(revertingContract): revertingCode,
		},
	})
	header := types.NewEmptyHeaderForAssembling()
	header.Number.SetUint64(1)
	header.GasLimit = 1_000_000
	header.Time = 1
	header.BaseFee = uint256.NewInt(1)
	block := &AssembledBlock{Header: header}
	assembler := NewBlockAssembler(AssemblerCfg{ChainConfig: config, Engine: ethash.NewFaker()}, block)
	signer := types.LatestSigner(config)
	targetTxn, err := types.SignTx(types.NewTransaction(0, revertingContract, nil, 50_000, uint256.NewInt(2), nil), *signer, key)
	require.NoError(t, err)
	privateTxn, err := types.SignTx(types.NewTransaction(1, recipient, uint256.NewInt(1), 50_000, uint256.NewInt(2), nil), *signer, key)
	require.NoError(t, err)

	_, _, err = assembler.AddTransactions(
		context.Background(),
		func(common.Hash, uint64) (*types.Header, error) { return nil, nil },
		[]types.Transaction{targetTxn, privateTxn},
		accounts.NilAddress,
		&vm.Config{},
		ibs,
		nil,
		func(hash common.Hash) (common.Hash, bool) { return targetTxn.Hash(), hash == privateTxn.Hash() },
		nil,
		"test",
		log.Root(),
	)
	require.NoError(t, err)
	require.Equal(t, types.Transactions{targetTxn}, block.Txns)
	require.Len(t, block.Receipts, 1)
	require.Equal(t, uint64(types.ReceiptStatusFailed), block.Receipts[0].Status)
}
