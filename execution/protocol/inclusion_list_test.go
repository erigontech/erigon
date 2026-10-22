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

package protocol

import (
	"crypto/ecdsa"
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

const (
	ilBlockGasLimit = 30_000_000
	ilTxnGasLimit   = 100_000
	ilChainID       = 1337
)

var (
	ilSenderKey, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	ilOtherKey, _   = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
	ilSenderCommon  = crypto.PubkeyToAddress(ilSenderKey.PublicKey)
	ilOtherCommon   = crypto.PubkeyToAddress(ilOtherKey.PublicKey)
	ilSenderAddress = accounts.InternAddress(ilSenderCommon)
	ilOtherAddress  = accounts.InternAddress(ilOtherCommon)
	ilRecipient     = common.HexToAddress("0x3333333333333333333333333333333333333333")
)

func ilSigner() types.Signer {
	return *types.LatestSignerForChainID(uint256.NewInt(ilChainID))
}

func ilSign(t *testing.T, key *ecdsa.PrivateKey, txn types.Transaction) types.Transaction {
	t.Helper()
	signed, err := types.SignTx(txn, ilSigner(), key)
	require.NoError(t, err)
	return signed
}

func ilLegacyTxn(t *testing.T, key *ecdsa.PrivateKey, nonce, gasLimit, gasPrice, value uint64, data []byte) types.Transaction {
	t.Helper()
	return ilSign(t, key, types.NewTransaction(nonce, ilRecipient, uint256.NewInt(value), gasLimit, uint256.NewInt(gasPrice), data))
}

func ilValidTxn(t *testing.T, nonce uint64) types.Transaction {
	t.Helper()
	return ilLegacyTxn(t, ilSenderKey, nonce, ilTxnGasLimit, 0, 0, nil)
}

func ilUnsignedTxn() types.Transaction {
	return types.NewTransaction(0, ilRecipient, uint256.NewInt(0), ilTxnGasLimit, uint256.NewInt(0), nil)
}

type ilStateReader struct {
	state.StateReader
	accounts map[accounts.Address]*accounts.Account
}

func newILStateReader() *ilStateReader {
	return &ilStateReader{
		StateReader: state.NewNoopReader(),
		accounts:    map[accounts.Address]*accounts.Account{},
	}
}

func (r *ilStateReader) account(addr accounts.Address) *accounts.Account {
	acc, ok := r.accounts[addr]
	if !ok {
		newAcc := accounts.NewAccount()
		acc = &newAcc
		r.accounts[addr] = acc
	}
	return acc
}

func (r *ilStateReader) setNonce(addr accounts.Address, nonce uint64) {
	r.account(addr).Nonce = nonce
}

func (r *ilStateReader) setBalance(addr accounts.Address, balance uint256.Int) {
	r.account(addr).Balance = balance
}

func (r *ilStateReader) ReadAccountData(addr accounts.Address) (*accounts.Account, error) {
	return r.accounts[addr], nil
}

func ilCheck(t *testing.T, reader state.StateReader, cfg *chain.Config, gp *GasPool, blockTxns, inclusionListTxns types.Transactions) bool {
	t.Helper()
	satisfied, err := ilCheckResult(reader, cfg, gp, blockTxns, inclusionListTxns)
	require.NoError(t, err)
	return satisfied
}

func ilCheckResult(reader state.StateReader, cfg *chain.Config, gp *GasPool, blockTxns, inclusionListTxns types.Transactions) (bool, error) {
	blockCtx := evmtypes.BlockContext{
		CanTransfer: CanTransfer,
		Transfer:    misc.Transfer,
		GasLimit:    ilBlockGasLimit,
	}
	return CheckInclusionListTransactions(gp, blockTxns, inclusionListTxns, reader, &blockCtx, cfg, &vm.Config{NoBaseFee: true})
}

func ilGasPool() *GasPool {
	return NewGasPool(ilBlockGasLimit, params.GasPerBlob*params.MaxBlobsPerTxn)
}

func TestCheckInclusionListTransactions_EmptyInclusionList(t *testing.T) {
	t.Parallel()

	blockTxns := types.Transactions{ilValidTxn(t, 0), ilValidTxn(t, 1)}
	require.True(t, ilCheck(t, newILStateReader(), chain.AllProtocolChanges, ilGasPool(), blockTxns, types.Transactions{}))
}

func TestCheckInclusionListTransactions_AllInclusionListTxnsInBlock(t *testing.T) {
	t.Parallel()

	first, second, third := ilValidTxn(t, 0), ilValidTxn(t, 1), ilValidTxn(t, 2)
	blockTxns := types.Transactions{third, first, second}
	inclusionListTxns := types.Transactions{second, first}
	require.True(t, ilCheck(t, newILStateReader(), chain.AllProtocolChanges, ilGasPool(), blockTxns, inclusionListTxns))
}

func TestCheckInclusionListTransactions_IncludedTxnIsNotValidated(t *testing.T) {
	t.Parallel()

	reader := newILStateReader()
	reader.setNonce(ilSenderAddress, 9)

	nonceTooLow := ilValidTxn(t, 0)
	belowIntrinsicGas := ilLegacyTxn(t, ilSenderKey, 9, 1_000, 0, 0, nil)

	blockTxns := types.Transactions{nonceTooLow, belowIntrinsicGas}
	inclusionListTxns := types.Transactions{nonceTooLow, belowIntrinsicGas}

	require.True(t, ilCheck(t, reader, chain.AllProtocolChanges, ilGasPool(), blockTxns, inclusionListTxns))
}

func TestCheckInclusionListTransactions_MissingValidTxn(t *testing.T) {
	t.Parallel()

	reader := newILStateReader()
	cfg := chain.AllProtocolChanges

	missing := ilValidTxn(t, 0)

	t.Run("empty block", func(t *testing.T) {
		require.False(t, ilCheck(t, reader, cfg, ilGasPool(), types.Transactions{}, types.Transactions{missing}))
	})

	t.Run("same sender and nonce but different txn", func(t *testing.T) {
		sameNonceOtherPayload := ilLegacyTxn(t, ilSenderKey, 0, ilTxnGasLimit, 0, 0, []byte{0x01})
		require.NotEqual(t, missing.Hash(), sameNonceOtherPayload.Hash())
		blockTxns := types.Transactions{sameNonceOtherPayload}
		require.False(t, ilCheck(t, reader, cfg, ilGasPool(), blockTxns, types.Transactions{missing}))
	})

	t.Run("one of several inclusion list txns missing", func(t *testing.T) {
		included := ilValidTxn(t, 1)
		blockTxns := types.Transactions{included}
		inclusionListTxns := types.Transactions{included, missing}
		require.False(t, ilCheck(t, reader, cfg, ilGasPool(), blockTxns, inclusionListTxns))
	})
}

func TestCheckInclusionListTransactions_MissingBlobTxnIsSkipped(t *testing.T) {
	t.Parallel()

	reader := newILStateReader()
	reader.setBalance(ilSenderAddress, *uint256.NewInt(1_000_000_000_000_000_000))
	blobTxn := ilSign(t, ilSenderKey, &types.BlobTx{
		DynamicFeeTransaction: types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: 0, GasLimit: ilTxnGasLimit, To: &ilRecipient},
			ChainID:  *uint256.NewInt(ilChainID),
			FeeCap:   *uint256.NewInt(1_000_000_000),
		},
		MaxFeePerBlobGas:    *uint256.NewInt(1_000_000_000),
		BlobVersionedHashes: []common.Hash{{0x01}},
	})

	require.True(t, ilCheck(t, reader, chain.AllProtocolChanges, ilGasPool(), nil, types.Transactions{blobTxn}))
}

func TestCheckInclusionListTransactions_MissingInvalidTxn(t *testing.T) {
	t.Parallel()

	cfg := chain.AllProtocolChanges

	t.Run("unsigned txn", func(t *testing.T) {
		require.True(t, ilCheck(t, newILStateReader(), cfg, ilGasPool(), nil, types.Transactions{ilUnsignedTxn()}))
	})

	t.Run("nonce too low", func(t *testing.T) {
		reader := newILStateReader()
		reader.setNonce(ilSenderAddress, 5)
		require.True(t, ilCheck(t, reader, cfg, ilGasPool(), nil, types.Transactions{ilValidTxn(t, 4)}))
	})

	t.Run("insufficient funds for gas", func(t *testing.T) {
		reader := newILStateReader()
		reader.setBalance(ilSenderAddress, *uint256.NewInt(1))
		txn := ilLegacyTxn(t, ilSenderKey, 0, ilTxnGasLimit, 1_000_000_000, 0, nil)
		require.True(t, ilCheck(t, reader, cfg, ilGasPool(), nil, types.Transactions{txn}))
	})

	t.Run("sufficient funds for gas", func(t *testing.T) {
		reader := newILStateReader()
		reader.setBalance(ilSenderAddress, *uint256.NewInt(ilTxnGasLimit * 1_000_000_000))
		txn := ilLegacyTxn(t, ilSenderKey, 0, ilTxnGasLimit, 1_000_000_000, 0, nil)
		require.False(t, ilCheck(t, reader, cfg, ilGasPool(), nil, types.Transactions{txn}))
	})

	t.Run("state read failure", func(t *testing.T) {
		readErr := errors.New("nonce read failed")
		reader := &accountErrorReader{StateReader: state.NewNoopReader(), err: readErr}
		satisfied, err := ilCheckResult(reader, cfg, ilGasPool(), nil, types.Transactions{ilValidTxn(t, 0)})
		require.ErrorIs(t, err, ErrTxnExecutionFailed)
		require.ErrorIs(t, err, readErr)
		require.False(t, satisfied)
	})
}

func TestCheckInclusionListTransactions_BlockGasExhausted(t *testing.T) {
	t.Parallel()

	reader := newILStateReader()
	cfg := chain.AllProtocolChanges
	txn := ilValidTxn(t, 0)

	t.Run("execution gas exhausted", func(t *testing.T) {
		gp := NewBlockGasPool(ilTxnGasLimit-1, ilBlockGasLimit, params.GasPerBlob)
		require.True(t, ilCheck(t, reader, cfg, gp, nil, types.Transactions{txn}))
	})

	t.Run("both dimensions exactly sufficient", func(t *testing.T) {
		gp := NewBlockGasPool(ilTxnGasLimit, ilTxnGasLimit, params.GasPerBlob)
		require.False(t, ilCheck(t, reader, cfg, gp, nil, types.Transactions{txn}))
	})
}

func TestCheckInclusionListTransactions_ValidTxnAfterInvalidOnes(t *testing.T) {
	t.Parallel()

	reader := newILStateReader()
	reader.setNonce(ilOtherAddress, 5)

	nonceTooLow := ilLegacyTxn(t, ilOtherKey, 0, ilTxnGasLimit, 0, 0, nil)
	belowIntrinsicGas := ilLegacyTxn(t, ilOtherKey, 5, 1_000, 0, 0, nil)
	inclusionListTxns := types.Transactions{nonceTooLow, belowIntrinsicGas, ilUnsignedTxn(), ilValidTxn(t, 0)}
	require.False(t, ilCheck(t, reader, chain.AllProtocolChanges, ilGasPool(), nil, inclusionListTxns))
}

func TestCheckInclusionListTransactions_LeavesGasPoolUntouched(t *testing.T) {
	t.Parallel()

	gp := ilGasPool()
	require.False(t, ilCheck(t, newILStateReader(), chain.AllProtocolChanges, gp, nil, types.Transactions{ilValidTxn(t, 0)}))
	require.Equal(t, uint64(ilBlockGasLimit), gp.ExecutionGasAvailable())
	require.Equal(t, uint64(ilBlockGasLimit), gp.StateGasAvailable())
	require.Equal(t, params.GasPerBlob*params.MaxBlobsPerTxn, gp.BlobGas())
}
