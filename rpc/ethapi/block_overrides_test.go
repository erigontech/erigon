package ethapi

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// helpers

func bigHex(n uint64) *hexutil.Big    { return (*hexutil.Big)(new(big.Int).SetUint64(n)) }
func u64Hex(n uint64) *hexutil.Uint64 { u := hexutil.Uint64(n); return &u }
func hash(b byte) *common.Hash        { h := common.Hash{b}; return &h }

// ─────────────────────────────────────────────────────────────────────────────
// Override — EVM block context (eth_call / eth_estimateGas)
// ─────────────────────────────────────────────────────────────────────────────

func TestOverride_NilReceiver(t *testing.T) {
	var o *BlockOverrides
	ctx := evmtypes.BlockContext{GasLimit: 1}
	require.NoError(t, o.Override(&ctx))
	assert.Equal(t, uint64(1), ctx.GasLimit, "nil receiver must be a no-op")
}

func TestOverride_AllFields(t *testing.T) {
	prevRandao := common.Hash{0xab}
	coinbase := common.Address{0x01}

	o := BlockOverrides{
		Number:        bigHex(100),
		Difficulty:    bigHex(200),
		Time:          u64Hex(300),
		GasLimit:      u64Hex(400),
		FeeRecipient:  &coinbase,
		PrevRandao:    &prevRandao,
		BaseFeePerGas: bigHex(500),
		BlobBaseFee:   bigHex(600),
	}

	ctx := evmtypes.BlockContext{}
	require.NoError(t, o.Override(&ctx))

	assert.Equal(t, uint64(100), ctx.BlockNumber)
	assert.Equal(t, *uint256.NewInt(200), ctx.Difficulty)
	assert.Equal(t, uint64(300), ctx.Time)
	assert.Equal(t, uint64(400), ctx.GasLimit)
	assert.Equal(t, accounts.InternAddress(coinbase), ctx.Coinbase)
	assert.Equal(t, &prevRandao, ctx.PrevRanDao)
	assert.Equal(t, *uint256.NewInt(500), ctx.BaseFee)
	assert.Equal(t, *uint256.NewInt(600), ctx.BlobBaseFee)
}

func TestOverride_NilFieldsAreNoOp(t *testing.T) {
	ctx := evmtypes.BlockContext{
		BlockNumber: 99,
		Time:        42,
		GasLimit:    1_000_000,
	}
	require.NoError(t, (&BlockOverrides{}).Override(&ctx))
	assert.Equal(t, uint64(99), ctx.BlockNumber)
	assert.Equal(t, uint64(42), ctx.Time)
	assert.Equal(t, uint64(1_000_000), ctx.GasLimit)
}

func TestOverride_RejectsBeaconRoot(t *testing.T) {
	o := BlockOverrides{BeaconRoot: hash(0x01)}
	err := o.Override(&evmtypes.BlockContext{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "beaconRoot")
}

func TestOverride_RejectsWithdrawals(t *testing.T) {
	ws := types.Withdrawals{}
	o := BlockOverrides{Withdrawals: &ws}
	err := o.Override(&evmtypes.BlockContext{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "withdrawals")
}

func TestOverride_BaseFeeOverflow(t *testing.T) {
	// value larger than 2^256-1 → overflow
	tooBig := new(big.Int).Lsh(big.NewInt(1), 256) // 2^256
	o := BlockOverrides{BaseFeePerGas: (*hexutil.Big)(tooBig)}
	err := o.Override(&evmtypes.BlockContext{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "BaseFee")
}

func TestOverride_BlobBaseFeeOverflow(t *testing.T) {
	tooBig := new(big.Int).Lsh(big.NewInt(1), 256)
	o := BlockOverrides{BlobBaseFee: (*hexutil.Big)(tooBig)}
	err := o.Override(&evmtypes.BlockContext{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "BlobBaseFee")
}

func TestOverride_PartialOverrideDoesNotTouchOtherFields(t *testing.T) {
	ctx := evmtypes.BlockContext{
		BlockNumber: 10,
		Time:        99,
		GasLimit:    5_000,
	}
	// only override GasLimit
	o := BlockOverrides{GasLimit: u64Hex(30_000_000)}
	require.NoError(t, o.Override(&ctx))
	assert.Equal(t, uint64(10), ctx.BlockNumber, "BlockNumber must be unchanged")
	assert.Equal(t, uint64(99), ctx.Time, "Time must be unchanged")
	assert.Equal(t, uint64(30_000_000), ctx.GasLimit)
}

// ─────────────────────────────────────────────────────────────────────────────
// OverrideHeader — block header copy (eth_simulateV1 block assembly)
// ─────────────────────────────────────────────────────────────────────────────

func baseHeader() *types.Header {
	h := types.NewEmptyHeaderForAssembling()
	h.Number.SetUint64(1)
	h.Difficulty.SetUint64(1)
	h.Time = 1
	h.GasLimit = 1
	h.Coinbase = common.Address{0xFF}
	h.BaseFee = uint256.NewInt(1)
	h.MixDigest = common.Hash{0xFF}
	return h
}

func TestOverrideHeader_NilReceiver(t *testing.T) {
	var o *BlockOverrides
	orig := baseHeader()
	result := o.OverrideHeader(orig)
	assert.Equal(t, orig, result, "nil receiver must return the original header unchanged")
}

func TestOverrideHeader_ReturnsACopy(t *testing.T) {
	orig := baseHeader()
	o := BlockOverrides{Number: bigHex(999)}
	result := o.OverrideHeader(orig)
	assert.NotSame(t, orig, result, "must return a new copy, not the same pointer")
	assert.Equal(t, uint64(1), orig.Number.Uint64(), "original must be untouched")
	assert.Equal(t, uint64(999), result.Number.Uint64())
}

func TestOverrideHeader_AllFields(t *testing.T) {
	coinbase := common.Address{0x02}
	prevRandao := common.Hash{0xCD}

	o := BlockOverrides{
		Number:        bigHex(1000),
		Difficulty:    bigHex(2000),
		Time:          u64Hex(3000),
		GasLimit:      u64Hex(4000),
		FeeRecipient:  &coinbase,
		BaseFeePerGas: bigHex(5000),
		PrevRandao:    &prevRandao,
	}

	result := o.OverrideHeader(baseHeader())

	assert.Equal(t, uint64(1000), result.Number.Uint64())
	assert.Equal(t, uint64(2000), result.Difficulty.Uint64())
	assert.Equal(t, uint64(3000), result.Time)
	assert.Equal(t, uint64(4000), result.GasLimit)
	assert.Equal(t, coinbase, result.Coinbase)
	assert.Equal(t, uint256.NewInt(5000), result.BaseFee)
	assert.Equal(t, prevRandao, result.MixDigest)
}

func TestOverrideHeader_NilFieldsAreNoOp(t *testing.T) {
	orig := baseHeader()
	result := (&BlockOverrides{}).OverrideHeader(orig)
	assert.Equal(t, orig.Number.Uint64(), result.Number.Uint64())
	assert.Equal(t, orig.Time, result.Time)
	assert.Equal(t, orig.GasLimit, result.GasLimit)
	assert.Equal(t, orig.Coinbase, result.Coinbase)
}

func TestOverrideHeader_BeaconRootAndWithdrawalsIgnored(t *testing.T) {
	// BeaconRoot and Withdrawals are not header fields — must not panic
	ws := types.Withdrawals{}
	o := BlockOverrides{
		BeaconRoot:  hash(0x01),
		Withdrawals: &ws,
	}
	require.NotPanics(t, func() {
		_ = o.OverrideHeader(baseHeader())
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// OverrideBlockContext — EVM context for simulation (eth_simulateV1 / eth_callMany)
// ─────────────────────────────────────────────────────────────────────────────

func TestOverrideBlockContext_NilReceiver(t *testing.T) {
	var o *BlockOverrides
	ctx := evmtypes.BlockContext{GasLimit: 1}
	overrides := BlockHashOverrides{}
	require.NotPanics(t, func() { o.OverrideBlockContext(&ctx, overrides) })
	assert.Equal(t, uint64(1), ctx.GasLimit, "nil receiver must be a no-op")
}

func TestOverrideBlockContext_AllFields(t *testing.T) {
	coinbase := common.Address{0x03}
	prevRandao := common.Hash{0xEF}

	o := BlockOverrides{
		Number:        bigHex(10),
		Difficulty:    bigHex(20),
		Time:          u64Hex(30),
		GasLimit:      u64Hex(40),
		FeeRecipient:  &coinbase,
		PrevRandao:    &prevRandao,
		BaseFeePerGas: bigHex(50),
		BlobBaseFee:   bigHex(60),
	}

	ctx := evmtypes.BlockContext{}
	o.OverrideBlockContext(&ctx, BlockHashOverrides{})

	assert.Equal(t, uint64(10), ctx.BlockNumber)
	assert.Equal(t, *uint256.NewInt(20), ctx.Difficulty)
	assert.Equal(t, uint64(30), ctx.Time)
	assert.Equal(t, uint64(40), ctx.GasLimit)
	assert.Equal(t, accounts.InternAddress(coinbase), ctx.Coinbase)
	assert.Equal(t, &prevRandao, ctx.PrevRanDao)
	assert.Equal(t, *uint256.NewInt(50), ctx.BaseFee)
	assert.Equal(t, *uint256.NewInt(60), ctx.BlobBaseFee)
}

func TestOverrideBlockContext_NilFieldsAreNoOp(t *testing.T) {
	ctx := evmtypes.BlockContext{
		BlockNumber: 77,
		Time:        88,
		GasLimit:    99,
	}
	(&BlockOverrides{}).OverrideBlockContext(&ctx, BlockHashOverrides{})
	assert.Equal(t, uint64(77), ctx.BlockNumber)
	assert.Equal(t, uint64(88), ctx.Time)
	assert.Equal(t, uint64(99), ctx.GasLimit)
}

func TestOverrideBlockContext_BlockHash(t *testing.T) {
	h1 := common.Hash{0x01}
	h2 := common.Hash{0x02}
	blockHashes := map[uint64]common.Hash{100: h1}
	o := BlockOverrides{BlockHash: &map[uint64]common.Hash{101: h2}}

	o.OverrideBlockContext(&evmtypes.BlockContext{}, BlockHashOverrides(blockHashes))

	assert.Equal(t, h1, blockHashes[100], "existing entry must be preserved")
	assert.Equal(t, h2, blockHashes[101], "new entry from override must be present")
}

func TestOverrideBlockContext_BlockHashOverwritesExisting(t *testing.T) {
	orig := common.Hash{0xAA}
	replacement := common.Hash{0xBB}
	blockHashes := map[uint64]common.Hash{200: orig}
	o := BlockOverrides{BlockHash: &map[uint64]common.Hash{200: replacement}}

	o.OverrideBlockContext(&evmtypes.BlockContext{}, BlockHashOverrides(blockHashes))

	assert.Equal(t, replacement, blockHashes[200], "override must replace existing entry")
}

func TestOverrideBlockContext_BeaconRootAndWithdrawalsDoNotPanic(t *testing.T) {
	// BeaconRoot and Withdrawals are not applied here — caller handles them.
	// Verify no panic occurs when they are set.
	ws := types.Withdrawals{}
	o := BlockOverrides{
		BeaconRoot:  hash(0x01),
		Withdrawals: &ws,
	}
	require.NotPanics(t, func() {
		o.OverrideBlockContext(&evmtypes.BlockContext{}, BlockHashOverrides{})
	})
}

func TestOverrideBlockContext_PartialOverrideDoesNotTouchOtherFields(t *testing.T) {
	ctx := evmtypes.BlockContext{
		BlockNumber: 5,
		Time:        55,
		GasLimit:    555,
	}
	o := BlockOverrides{Time: u64Hex(999)}
	o.OverrideBlockContext(&ctx, BlockHashOverrides{})
	assert.Equal(t, uint64(5), ctx.BlockNumber, "BlockNumber must be unchanged")
	assert.Equal(t, uint64(999), ctx.Time)
	assert.Equal(t, uint64(555), ctx.GasLimit, "GasLimit must be unchanged")
}
