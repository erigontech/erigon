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

package execution_client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type newPayloadModuleStub struct {
	execmodule.ExecutionModule
	inserted        []*types.Block
	validationCalls int
	recoveries      map[common.Hash]*exec.SendersRecovery
	validation      execmodule.ValidationResult
}

func (s *newPayloadModuleStub) AddSendersRecovery(blockHash common.Hash, r *exec.SendersRecovery) {
	if s.recoveries == nil {
		s.recoveries = map[common.Hash]*exec.SendersRecovery{}
	}
	s.recoveries[blockHash] = r
}

func (s *newPayloadModuleStub) InsertBlocks(_ context.Context, blocks []*types.Block) (execmodule.ExecutionStatus, error) {
	s.inserted = append(s.inserted, blocks...)
	return execmodule.ExecutionStatusSuccess, nil
}

func (s *newPayloadModuleStub) CurrentHeader(context.Context) (*types.Header, error) {
	return &types.Header{}, nil
}

func (s *newPayloadModuleStub) ValidateChain(context.Context, *types.Block) (execmodule.ValidationResult, error) {
	s.validationCalls++
	return s.validation, nil
}

func TestDirectNewPayloadTransactionEncoding(t *testing.T) {
	transactions, err := types.MarshalTransactionsBinary(types.Transactions{
		&types.LegacyTx{},
		&types.DynamicFeeTransaction{},
		&types.DynamicFeeTransaction{CommonTx: types.CommonTx{Data: make([]byte, 64)}},
	})
	require.NoError(t, err)
	wrappedShort, err := rlp.EncodeToBytes(transactions[1])
	require.NoError(t, err)
	wrappedLong, err := rlp.EncodeToBytes(transactions[2])
	require.NoError(t, err)

	for _, tc := range []struct {
		name         string
		transactions [][]byte
		invalid      bool
	}{
		{name: "empty"},
		{name: "legacy", transactions: transactions[:1]},
		{name: "typed", transactions: transactions[1:]},
		{name: "mixed", transactions: transactions},
		{name: "wrapped short", transactions: [][]byte{wrappedShort}, invalid: true},
		{name: "wrapped long", transactions: [][]byte{wrappedLong}, invalid: true},
		{name: "wrapped after canonical", transactions: [][]byte{transactions[0], transactions[1], wrappedLong}, invalid: true},
		{name: "malformed legacy", transactions: [][]byte{{0xc1, 0x80}}, invalid: true},
		{name: "malformed access list", transactions: [][]byte{{types.AccessListTxType, 0xc0}}, invalid: true},
		{name: "malformed dynamic fee", transactions: [][]byte{{types.DynamicFeeTxType, 0xc0}}, invalid: true},
		{name: "malformed set code", transactions: [][]byte{{types.SetCodeTxType, 0xc0}}, invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, module := newDirectClientForNewPayload(t)

			payload := cltypes.NewEth1Block(clparams.FuluVersion, &clparams.MainnetBeaconConfig)
			payload.BlockNumber = 1
			payload.Extra = solid.NewExtraData()
			payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(clparams.MainnetBeaconConfig.MaxWithdrawalsPerPayload), 44)
			payload.Transactions = solid.NewTransactionsSSZFromTransactions(tc.transactions)
			parentRoot := common.Hash{}
			payloadHash, err := payload.ComputeBlockHash(&parentRoot, cltypes.ComputeExecutionRequestHash(nil), nil)
			require.NoError(t, err)
			payload.BlockHash = payloadHash

			status, err := client.NewPayload(t.Context(), payload, &parentRoot, []common.Hash{}, nil)
			if tc.invalid {
				require.EqualValues(t, PayloadStatusInvalidated, status)
				require.Error(t, err)
				require.Empty(t, module.inserted)
				require.Zero(t, module.validationCalls)
				return
			}
			require.NoError(t, err)
			require.EqualValues(t, PayloadStatusValidated, status)
			require.Len(t, module.inserted, 1)
			require.Len(t, module.inserted[0].Transactions(), len(tc.transactions))
			require.Equal(t, 1, module.validationCalls)
		})
	}
}

func TestDirectNewPayloadRegistersSendersRecovery(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	payload := cltypes.NewEth1Block(clparams.FuluVersion, &clparams.MainnetBeaconConfig)
	payload.BlockNumber = 1
	payload.Extra = solid.NewExtraData()
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(clparams.MainnetBeaconConfig.MaxWithdrawalsPerPayload), 44)
	txn, err := types.SignTx(&types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{GasLimit: 21_000},
		ChainID:  *chain.AllProtocolChanges.ChainID,
	}, *types.MakeSigner(chain.AllProtocolChanges, payload.BlockNumber, payload.Time), key)
	require.NoError(t, err)
	raw, err := types.MarshalTransactionsBinary(types.Transactions{txn})
	require.NoError(t, err)
	payload.Transactions = solid.NewTransactionsSSZFromTransactions(raw)
	parentRoot := common.Hash{}
	payload.BlockHash, err = payload.ComputeBlockHash(&parentRoot, cltypes.ComputeExecutionRequestHash(nil), nil)
	require.NoError(t, err)

	module := &newPayloadModuleStub{}
	client, err := NewExecutionClientDirect(chainreader.NewChainReaderEth1(chain.AllProtocolChanges, module, time.Second), nil)
	require.NoError(t, err)
	_, err = client.NewPayload(t.Context(), payload, &parentRoot, []common.Hash{}, nil)
	require.NoError(t, err)

	r, ok := module.recoveries[payload.BlockHash]
	require.True(t, ok, "Caplin must start the recovery before validation, as Engine API newPayload does")
	senders, err := r.Wait(t.Context())
	require.NoError(t, err)
	require.Equal(t, []accounts.Address{accounts.InternAddress(crypto.PubkeyToAddress(key.PublicKey))}, senders)
}

func TestDirectNewPayloadCarriesLatestValidHash(t *testing.T) {
	lvh := common.Hash{0x42}
	for _, tc := range []struct {
		name    string
		status  execmodule.ExecutionStatus
		lvh     common.Hash
		wantLVH common.Hash
	}{
		{name: "bad block with lvh", status: execmodule.ExecutionStatusBadBlock, lvh: lvh, wantLVH: lvh},
		{name: "bad block without lvh", status: execmodule.ExecutionStatusBadBlock},
		{name: "invalid forkchoice with lvh", status: execmodule.ExecutionStatusInvalidForkchoice, lvh: lvh},
	} {
		t.Run(tc.name, func(t *testing.T) {
			module := &newPayloadModuleStub{validation: execmodule.ValidationResult{ValidationStatus: tc.status, LatestValidHash: tc.lvh}}
			client, err := NewExecutionClientDirect(chainreader.NewChainReaderEth1(chain.AllProtocolChanges, module, time.Second), nil)
			require.NoError(t, err)

			payload := cltypes.NewEth1Block(clparams.FuluVersion, &clparams.MainnetBeaconConfig)
			payload.BlockNumber = 1
			payload.Extra = solid.NewExtraData()
			payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(clparams.MainnetBeaconConfig.MaxWithdrawalsPerPayload), 44)
			payload.Transactions = solid.NewTransactionsSSZFromTransactions(nil)
			parentRoot := common.Hash{}
			payload.BlockHash, err = payload.ComputeBlockHash(&parentRoot, cltypes.ComputeExecutionRequestHash(nil), nil)
			require.NoError(t, err)

			status, err := client.NewPayload(t.Context(), payload, &parentRoot, []common.Hash{}, nil)
			require.EqualValues(t, PayloadStatusInvalidated, status)
			require.Error(t, err)
			var invalid *InvalidPayloadError
			if tc.wantLVH == (common.Hash{}) {
				require.NotErrorAs(t, err, &invalid)
				return
			}
			require.ErrorAs(t, err, &invalid)
			require.Equal(t, tc.wantLVH, invalid.LatestValidHash)
		})
	}
}

func TestRetryAssembleBlockReturnsFirstSuccess(t *testing.T) {
	calls := 0
	id, err := retryAssembleBlock(t.Context(), 3, time.Millisecond, time.Millisecond, func(context.Context) (uint64, error) {
		calls++
		if calls < 3 {
			return 0, chainreader.ErrExecutionBusy
		}
		return 7, nil
	})

	require.NoError(t, err)
	require.Equal(t, uint64(7), id)
	require.Equal(t, 3, calls)
}

func TestRetryAssembleBlockStopsOnRejection(t *testing.T) {
	rejected := errors.New("withdrawals before shanghai")
	calls := 0
	_, err := retryAssembleBlock(t.Context(), 30, time.Hour, time.Hour, func(context.Context) (uint64, error) {
		calls++
		return 0, rejected
	})

	// Only contention settles by waiting; a rejection answers the same way however often it is
	// asked, so retrying it just burns the slot.
	require.ErrorIs(t, err, rejected)
	require.Equal(t, 1, calls)
}

func TestRetryAssembleBlockGivesUpAfterAttempts(t *testing.T) {
	calls := 0
	_, err := retryAssembleBlock(t.Context(), 2, time.Millisecond, time.Millisecond, func(context.Context) (uint64, error) {
		calls++
		return 0, chainreader.ErrExecutionBusy
	})

	require.ErrorIs(t, err, chainreader.ErrExecutionBusy)
	require.Equal(t, 2, calls)
}

func TestRetryAssembleBlockStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	_, err := retryAssembleBlock(ctx, 30, time.Hour, time.Hour, func(context.Context) (uint64, error) {
		calls++
		cancel()
		return 0, chainreader.ErrExecutionBusy
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
}

func TestRetryAssembleBlockDoesNotStartWithCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	calls := 0
	_, err := retryAssembleBlock(ctx, 30, time.Hour, time.Hour, func(context.Context) (uint64, error) {
		calls++
		return 0, chainreader.ErrExecutionBusy
	})

	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, calls)
}

func TestRetryAssembleBlockRejectsNoAttempts(t *testing.T) {
	_, err := retryAssembleBlock(t.Context(), 0, time.Millisecond, time.Millisecond, func(context.Context) (uint64, error) {
		return 1, nil
	})
	require.EqualError(t, err, "assemble block requires at least one attempt")
}

func newDirectClientForNewPayload(t *testing.T) (*ExecutionClientDirect, *newPayloadModuleStub) {
	t.Helper()
	module := &newPayloadModuleStub{}
	client, err := NewExecutionClientDirect(chainreader.NewChainReaderEth1(chain.AllProtocolChanges, module, time.Second), nil)
	require.NoError(t, err)
	return client, module
}

func gloasPayloadWithBlobTx(t *testing.T, blobHashes ...common.Hash) *cltypes.Eth1Block {
	t.Helper()
	txn := &types.BlobTx{
		DynamicFeeTransaction: types.DynamicFeeTransaction{CommonTx: types.CommonTx{GasLimit: 21_000, To: &common.Address{1}}},
		BlobVersionedHashes:   blobHashes,
	}
	raw, err := types.MarshalTransactionsBinary(types.Transactions{txn})
	require.NoError(t, err)
	payload := gloas(&clparams.MainnetBeaconConfig, []byte{0xc0})
	payload.Transactions = solid.NewProgressiveTransactionsSSZFromTransactions(raw)
	payload.BlobGasUsed = misc.GetBlobGasUsed(len(blobHashes))
	bal, err := DecodeAndValidateBlockAccessList(payload)
	require.NoError(t, err)
	payload.BlockHash, err = payload.ComputeBlockHash(&common.Hash{}, cltypes.ComputeExecutionRequestHash(nil), bal)
	require.NoError(t, err)
	return payload
}

func TestDirectNewPayloadRejectsBlobHashesTheBidDidNotCommitTo(t *testing.T) {
	v, w := common.Hash{0x01, 0x0a}, common.Hash{0x01, 0x0b}
	for _, tc := range []struct {
		name      string
		inPayload []common.Hash
		expected  []common.Hash
	}{
		{"missing", []common.Hash{v}, []common.Hash{}},
		{"different", []common.Hash{v}, []common.Hash{w}},
		{"reordered", []common.Hash{v, w}, []common.Hash{w, v}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, module := newDirectClientForNewPayload(t)

			status, err := client.NewPayload(t.Context(), gloasPayloadWithBlobTx(t, tc.inPayload...), &common.Hash{}, tc.expected, nil)

			require.ErrorIs(t, err, misc.ErrMismatchBlobHashes)
			require.EqualValues(t, PayloadStatusInvalidated, status)
			require.Empty(t, module.inserted)
		})
	}
}

func TestDirectNewPayloadAcceptsMatchingBlobHashes(t *testing.T) {
	v := common.Hash{0x01, 0x0a}
	client, module := newDirectClientForNewPayload(t)

	status, err := client.NewPayload(t.Context(), gloasPayloadWithBlobTx(t, v), &common.Hash{}, []common.Hash{v}, nil)

	require.NoError(t, err)
	require.EqualValues(t, PayloadStatusValidated, status)
	require.Len(t, module.inserted, 1)
}

func TestDirectNewPayloadTreatsNilBlobHashesAsACallerError(t *testing.T) {
	client, module := newDirectClientForNewPayload(t)

	status, err := client.NewPayload(t.Context(), gloasPayloadWithBlobTx(t, common.Hash{0x01}), &common.Hash{}, nil, nil)

	require.ErrorIs(t, err, misc.ErrNilBlobHashes)
	require.EqualValues(t, PayloadStatusNone, status)
	require.Empty(t, module.inserted)
}

// The usual contention is the forkchoice update's own commit, which ends within tens of
// milliseconds, so the first retries come quickly; longer contention backs off to the cap.
func TestAssembleRetryDelayBacksOffToTheCap(t *testing.T) {
	var delays []time.Duration
	for retry := range 7 {
		delays = append(delays, assembleRetryDelay(retry, 10*time.Millisecond, 200*time.Millisecond))
	}
	require.Equal(t, []time.Duration{
		10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond, 80 * time.Millisecond,
		160 * time.Millisecond, 200 * time.Millisecond, 200 * time.Millisecond,
	}, delays)
}

// One busy answer costs the first, short delay, not the cap.
func TestRetryAssembleBlockRetriesOneBusyAnswerQuickly(t *testing.T) {
	calls := 0
	start := time.Now()
	id, err := retryAssembleBlock(t.Context(), 30, 10*time.Millisecond, 5*time.Second, func(context.Context) (uint64, error) {
		calls++
		if calls == 1 {
			return 0, chainreader.ErrExecutionBusy
		}
		return 7, nil
	})

	require.NoError(t, err)
	require.Equal(t, uint64(7), id)
	require.Less(t, time.Since(start), time.Second)
}
