package protocol

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func TestSlotExceeded_ExtraDataRoundTrip(t *testing.T) {
	on := &chain.Config{SlotExceededTxs: true}

	extra, err := EncodeSlotExceeded([]int{0, 3, 300})
	require.NoError(t, err)
	require.Equal(t, []uint16{0, 3, 300}, SlotExceededIndices(on, extra))

	require.Nil(t, SlotExceededIndices(&chain.Config{}, extra), "a chain that does not enable verdicts reads none")
	require.Nil(t, SlotExceededIndices(nil, extra))
	require.Nil(t, SlotExceededIndices(on, []byte("erigon-v3.0")), "arbitrary extra-data is not a verdict")
	require.Nil(t, SlotExceededIndices(on, []byte{slotExceededTag, 0, 3, 0, 1}), "indices out of order are not a verdict")
	require.Nil(t, SlotExceededIndices(on, []byte{slotExceededTag, 0}), "a truncated index is not a verdict")

	all := make([]int, MaxSlotExceeded)
	for i := range all {
		all[i] = i
	}
	full, err := EncodeSlotExceeded(all)
	require.NoError(t, err)
	require.LessOrEqual(t, uint64(len(full)), params.MaximumExtraDataSize, "the most verdicts one header carries still fit extra-data")
	_, err = EncodeSlotExceeded(append(all, MaxSlotExceeded))
	require.Error(t, err)
	_, err = EncodeSlotExceeded([]int{2, 1})
	require.Error(t, err)
}

// A slot-exceeded transaction is included the way an out-of-gas one is: nonce consumed, the whole gas limit
// charged and paid to the coinbase, the value NOT moved, and a failed result. The same transaction at a body
// index the header does not name executes normally.
func TestSlotExceeded_IncludedLikeOutOfGas(t *testing.T) {
	sender := accounts.InternAddress(common.HexToAddress("0x1111111111111111111111111111111111111111"))
	recipient := accounts.InternAddress(common.HexToAddress("0x2222222222222222222222222222222222222222"))
	coinbase := accounts.InternAddress(common.HexToAddress("0x3333333333333333333333333333333333333333"))
	cfg := chain.TestChainOsakaConfig

	const (
		gasLimit      = 100_000
		gasPrice      = 2
		blockGasLimit = 30_000_000
	)
	startBalance := *uint256.NewInt(1_000_000_000)

	run := func(txIndex int) (*evmtypes.ExecutionResult, *state.IntraBlockState, *GasPool) {
		ibs := state.New(state.NewNoopReader())
		require.NoError(t, ibs.AddBalance(sender, startBalance, tracing.BalanceChangeUnspecified))
		blockCtx := evmtypes.BlockContext{
			CanTransfer:  CanTransfer,
			Transfer:     misc.Transfer,
			GasLimit:     blockGasLimit,
			Coinbase:     coinbase,
			SlotExceeded: []uint16{0},
		}
		evm := vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs, cfg, vm.Config{})
		ibs.SetTxContext(1, txIndex)
		msg := types.NewMessage(sender, recipient, 0, uint256.NewInt(5), gasLimit,
			uint256.NewInt(gasPrice), uint256.NewInt(gasPrice), uint256.NewInt(gasPrice),
			nil, nil,
			true,  // checkNonce
			false, // checkTransaction
			false, // checkGas
			false, // isFree
			nil,
		)
		gp := new(GasPool).AddGas(blockGasLimit)
		res, err := ApplyBlockMessage(evm, msg, gp, true, false, nil)
		require.NoError(t, err)
		return res, ibs, gp
	}

	res, ibs, gp := run(0)
	require.True(t, res.Failed(), "a slot-exceeded transaction fails")
	require.ErrorIs(t, res.Err, vm.ErrOutOfGas)
	require.Equal(t, uint64(gasLimit), res.ReceiptGasUsed, "the whole gas limit is used")
	require.Equal(t, uint64(blockGasLimit-gasLimit), gp.Gas(), "and taken from the block")

	nonce, err := ibs.GetNonce(sender)
	require.NoError(t, err)
	require.Equal(t, uint64(1), nonce, "the nonce is consumed")
	got, err := ibs.GetBalance(recipient)
	require.NoError(t, err)
	require.True(t, got.IsZero(), "the value does not move")
	got, err = ibs.GetBalance(sender)
	require.NoError(t, err)
	require.Equal(t, uint64(1_000_000_000-gasLimit*gasPrice), got.Uint64(), "the sender pays for all of the gas")
	got, err = ibs.GetBalance(coinbase)
	require.NoError(t, err)
	require.Equal(t, uint64(gasLimit*gasPrice), got.Uint64(), "the producer is paid for it")

	res, ibs, _ = run(1)
	require.False(t, res.Failed(), "an index the header does not name executes normally")
	got, err = ibs.GetBalance(recipient)
	require.NoError(t, err)
	require.Equal(t, uint64(5), got.Uint64())
}
