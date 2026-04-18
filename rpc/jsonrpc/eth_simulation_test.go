package jsonrpc

import (
	"context"
	"math"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

// ─── sanitizeSimulatedBlocks tests ────────────────────────────────────────────

func TestSimulateSanitizeBlockOrder(t *testing.T) {
	type result struct {
		number    uint64
		timestamp uint64
	}
	for i, tc := range []struct {
		baseNumber    uint64
		baseTimestamp uint64
		blocks        []SimulatedBlock
		expected      []result
		err           string
	}{
		{
			baseNumber:    10,
			baseTimestamp: 50,
			blocks:        []SimulatedBlock{{}, {}, {}},
			expected:      []result{{number: 11, timestamp: 62}, {number: 12, timestamp: 74}, {number: 13, timestamp: 86}},
		},
		{
			baseNumber:    10,
			baseTimestamp: 50,
			blocks:        []SimulatedBlock{{BlockOverrides: &ethapi.BlockOverrides{Number: newBig(13), Time: newUint64(80)}}, {}},
			expected:      []result{{number: 11, timestamp: 62}, {number: 12, timestamp: 74}, {number: 13, timestamp: 80}, {number: 14, timestamp: 92}},
		},
		{
			baseNumber:    10,
			baseTimestamp: 50,
			blocks:        []SimulatedBlock{{BlockOverrides: &ethapi.BlockOverrides{Number: newBig(11)}}, {BlockOverrides: &ethapi.BlockOverrides{Number: newBig(14)}}, {}},
			expected:      []result{{number: 11, timestamp: 62}, {number: 12, timestamp: 74}, {number: 13, timestamp: 86}, {number: 14, timestamp: 98}, {number: 15, timestamp: 110}},
		},
		{
			baseNumber:    10,
			baseTimestamp: 50,
			blocks:        []SimulatedBlock{{BlockOverrides: &ethapi.BlockOverrides{Number: newBig(13)}}, {BlockOverrides: &ethapi.BlockOverrides{Number: newBig(12)}}},
			err:           "block numbers must be in order: 12 <= 13",
		},
		{
			baseNumber:    10,
			baseTimestamp: 50,
			blocks:        []SimulatedBlock{{BlockOverrides: &ethapi.BlockOverrides{Number: newBig(13), Time: newUint64(74)}}},
			err:           "block timestamps must be in order: 74 <= 74",
		},
		{
			baseNumber:    10,
			baseTimestamp: 50,
			blocks:        []SimulatedBlock{{BlockOverrides: &ethapi.BlockOverrides{Number: newBig(11), Time: newUint64(60)}}, {BlockOverrides: &ethapi.BlockOverrides{Number: newBig(12), Time: newUint64(55)}}},
			err:           "block timestamps must be in order: 55 <= 60",
		},
		{
			baseNumber:    10,
			baseTimestamp: 50,
			blocks:        []SimulatedBlock{{BlockOverrides: &ethapi.BlockOverrides{Number: newBig(11), Time: newUint64(60)}}, {BlockOverrides: &ethapi.BlockOverrides{Number: newBig(13), Time: newUint64(72)}}},
			err:           "block timestamps must be in order: 72 <= 72",
		},
	} {
		sim := &simulator{base: &types.Header{Number: *uint256.NewInt(tc.baseNumber), Time: tc.baseTimestamp}}
		res, err := sim.sanitizeSimulatedBlocks(tc.blocks)
		if err != nil {
			if err.Error() == tc.err {
				continue
			} else {
				t.Fatalf("testcase %d: error mismatch. Want '%s', have '%s'", i, tc.err, err.Error())
			}
		}
		if tc.err != "" {
			t.Fatalf("testcase %d: expected err", i)
		}
		if len(res) != len(tc.expected) {
			t.Errorf("testcase %d: mismatch number of blocks. Want %d, have %d", i, len(tc.expected), len(res))
		}
		for bi, b := range res {
			if b.BlockOverrides == nil {
				t.Fatalf("testcase %d: block overrides nil", i)
			}
			if b.BlockOverrides.Number == nil {
				t.Fatalf("testcase %d: block number not set", i)
			}
			if b.BlockOverrides.Time == nil {
				t.Fatalf("testcase %d: block time not set", i)
			}
			if uint64(*b.BlockOverrides.Time) != tc.expected[bi].timestamp {
				t.Errorf("testcase %d: block timestamp mismatch. Want %d, have %d", i, tc.expected[bi].timestamp, uint64(*b.BlockOverrides.Time))
			}
			have := b.BlockOverrides.Number.Uint64()
			if have != tc.expected[bi].number {
				t.Errorf("testcase %d: block number mismatch. Want %d, have %d", i, tc.expected[bi].number, have)
			}
		}
	}
}

// TestSanitizeBlocksTooMany verifies the maxSimulateBlocks limit.
func TestSanitizeBlocksTooMany(t *testing.T) {
	sim := &simulator{base: &types.Header{Number: *uint256.NewInt(0), Time: 0}}
	// Requesting block 257 (gap from 0) exceeds the 256-block limit.
	blocks := []SimulatedBlock{{BlockOverrides: &ethapi.BlockOverrides{Number: newBig(maxSimulateBlocks + 1)}}}
	_, err := sim.sanitizeSimulatedBlocks(blocks)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too many blocks")

	// Exactly 256 should succeed.
	blocks = []SimulatedBlock{{BlockOverrides: &ethapi.BlockOverrides{Number: newBig(maxSimulateBlocks)}}}
	_, err = sim.sanitizeSimulatedBlocks(blocks)
	require.NoError(t, err)
}

// TestSanitizeBlocksSameNumber verifies that duplicate block numbers are rejected.
func TestSanitizeBlocksSameNumber(t *testing.T) {
	sim := &simulator{base: &types.Header{Number: *uint256.NewInt(5), Time: 100}}
	blocks := []SimulatedBlock{
		{BlockOverrides: &ethapi.BlockOverrides{Number: newBig(7)}},
		{BlockOverrides: &ethapi.BlockOverrides{Number: newBig(7)}},
	}
	_, err := sim.sanitizeSimulatedBlocks(blocks)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "block numbers must be in order")
}

// TestSanitizeBlocksEqualToBase verifies that a block number equal to base is rejected.
func TestSanitizeBlocksEqualToBase(t *testing.T) {
	sim := &simulator{base: &types.Header{Number: *uint256.NewInt(10), Time: 100}}
	blocks := []SimulatedBlock{
		{BlockOverrides: &ethapi.BlockOverrides{Number: newBig(10)}},
	}
	_, err := sim.sanitizeSimulatedBlocks(blocks)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "block numbers must be in order")
}

// ─── sanitizeCall tests ──────────────────────────────────────────────────────

// newTestSimulator creates a simulator with just the chain config needed for sanitizeCall tests.
func newTestSimulator(chainID *big.Int) *simulator {
	return &simulator{
		chainConfig: &chain.Config{ChainID: chainID},
	}
}

// callArgs creates a CallArgs with a nonce pre-set (to avoid needing IntraBlockState).
func callArgs() ethapi.CallArgs {
	nonce := hexutil.Uint64(0)
	return ethapi.CallArgs{Nonce: &nonce}
}

// blockCtx creates a BlockContext with the given gas limit.
func blockCtx(gasLimit uint64) evmtypes.BlockContext {
	return evmtypes.BlockContext{GasLimit: gasLimit}
}

// TestSanitizeCallGasDefaultsToRemainingBlock verifies that when Gas is nil,
// it defaults to remaining block gas (blockGasLimit - gasUsed), capped by globalGasCap.
func TestSanitizeCallGasDefaultsToRemainingBlock(t *testing.T) {
	sim := newTestSimulator(big.NewInt(1))
	args := callArgs()
	bc := blockCtx(30_000_000)

	err := sim.sanitizeCall(&args, nil, &bc, nil, 10_000_000, 50_000_000)
	require.NoError(t, err)
	// remaining = 30M - 10M = 20M, which is < globalGasCap of 50M, so gas = 20M
	assert.Equal(t, uint64(20_000_000), uint64(*args.Gas))
}

// TestSanitizeCallGasDefaultsCappedByGasCap verifies that when Gas is nil and
// remaining block gas exceeds globalGasCap, the gas is capped to globalGasCap.
func TestSanitizeCallGasDefaultsCappedByGasCap(t *testing.T) {
	sim := newTestSimulator(big.NewInt(1))
	args := callArgs()
	bc := blockCtx(100_000_000)

	err := sim.sanitizeCall(&args, nil, &bc, nil, 0, 25_000_000)
	require.NoError(t, err)
	// remaining = 100M, globalGasCap = 25M → capped to 25M
	assert.Equal(t, uint64(25_000_000), uint64(*args.Gas))
}

// TestSanitizeCallGasDefaultsNoCap verifies that when globalGasCap is 0 (unlimited),
// remaining block gas is capped by MaxUint64/2.
func TestSanitizeCallGasDefaultsNoCap(t *testing.T) {
	sim := newTestSimulator(big.NewInt(1))
	args := callArgs()
	bc := blockCtx(30_000_000)

	err := sim.sanitizeCall(&args, nil, &bc, nil, 0, 0)
	require.NoError(t, err)
	// remaining = 30M, effectiveCap = MaxUint64/2 → gas = 30M (remaining is less)
	assert.Equal(t, uint64(30_000_000), uint64(*args.Gas))
}

// TestSanitizeCallUserGasRespected verifies that user-specified gas is used when within limits.
func TestSanitizeCallUserGasRespected(t *testing.T) {
	sim := newTestSimulator(big.NewInt(1))
	args := callArgs()
	userGas := hexutil.Uint64(5_000_000)
	args.Gas = &userGas
	bc := blockCtx(30_000_000)

	err := sim.sanitizeCall(&args, nil, &bc, nil, 0, 50_000_000)
	require.NoError(t, err)
	assert.Equal(t, uint64(5_000_000), uint64(*args.Gas))
}

// TestSanitizeCallUserGasCappedByGlobalCap verifies that user-specified gas exceeding
// globalGasCap is capped down.
func TestSanitizeCallUserGasCappedByGlobalCap(t *testing.T) {
	sim := newTestSimulator(big.NewInt(1))
	args := callArgs()
	userGas := hexutil.Uint64(100_000_000)
	args.Gas = &userGas
	bc := blockCtx(200_000_000)

	err := sim.sanitizeCall(&args, nil, &bc, nil, 0, 50_000_000)
	require.NoError(t, err)
	assert.Equal(t, uint64(50_000_000), uint64(*args.Gas))
}

// TestSanitizeCallUserGasNoCap verifies that when globalGasCap is 0, user gas is not capped.
func TestSanitizeCallUserGasNoCap(t *testing.T) {
	sim := newTestSimulator(big.NewInt(1))
	args := callArgs()
	userGas := hexutil.Uint64(100_000_000)
	args.Gas = &userGas
	bc := blockCtx(200_000_000)

	err := sim.sanitizeCall(&args, nil, &bc, nil, 0, 0)
	require.NoError(t, err)
	// No globalGasCap, so user gas is not capped
	assert.Equal(t, uint64(100_000_000), uint64(*args.Gas))
}

// TestSanitizeCallBlockGasLimitExceeded verifies that gas + gasUsed exceeding block gas limit is rejected.
func TestSanitizeCallBlockGasLimitExceeded(t *testing.T) {
	sim := newTestSimulator(big.NewInt(1))
	args := callArgs()
	userGas := hexutil.Uint64(20_000_001)
	args.Gas = &userGas
	bc := blockCtx(30_000_000)

	err := sim.sanitizeCall(&args, nil, &bc, nil, 10_000_000, 50_000_000)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "block gas limit reached")
	var customErr *rpc.CustomError
	require.ErrorAs(t, err, &customErr)
	assert.Equal(t, rpc.ErrCodeBlockGasLimitReached, customErr.Code)
}

// TestSanitizeCallBlockGasLimitExact verifies that gas + gasUsed exactly at block gas limit succeeds.
func TestSanitizeCallBlockGasLimitExact(t *testing.T) {
	sim := newTestSimulator(big.NewInt(1))
	args := callArgs()
	userGas := hexutil.Uint64(20_000_000)
	args.Gas = &userGas
	bc := blockCtx(30_000_000)

	err := sim.sanitizeCall(&args, nil, &bc, nil, 10_000_000, 50_000_000)
	require.NoError(t, err)
}

// TestSanitizeCallChainIDDefaultCopied verifies that the default chain ID is a copy,
// not a reference to the live chainConfig (preventing aliasing bugs).
func TestSanitizeCallChainIDDefaultCopied(t *testing.T) {
	originalID := big.NewInt(1)
	sim := newTestSimulator(originalID)
	args := callArgs()
	bc := blockCtx(30_000_000)

	err := sim.sanitizeCall(&args, nil, &bc, nil, 0, 50_000_000)
	require.NoError(t, err)
	require.NotNil(t, args.ChainID)
	// The returned chain ID should equal the original.
	assert.Equal(t, 0, (*big.Int)(args.ChainID).Cmp(originalID))
	// But it must be a different pointer — mutating args.ChainID must not affect chainConfig.
	(*big.Int)(args.ChainID).SetInt64(999)
	assert.Equal(t, int64(1), originalID.Int64(), "mutating args.ChainID must not affect chainConfig.ChainID")
}

// TestSanitizeCallChainIDMismatch verifies that a mismatched chain ID in args is rejected.
func TestSanitizeCallChainIDMismatch(t *testing.T) {
	sim := newTestSimulator(big.NewInt(1))
	args := callArgs()
	args.ChainID = (*hexutil.Big)(big.NewInt(999))
	bc := blockCtx(30_000_000)

	err := sim.sanitizeCall(&args, nil, &bc, nil, 0, 50_000_000)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "chainId does not match")
}

// TestSanitizeCallChainIDMatch verifies that a matching chain ID in args is accepted.
func TestSanitizeCallChainIDMatch(t *testing.T) {
	sim := newTestSimulator(big.NewInt(42))
	args := callArgs()
	args.ChainID = (*hexutil.Big)(big.NewInt(42))
	bc := blockCtx(30_000_000)

	err := sim.sanitizeCall(&args, nil, &bc, nil, 0, 50_000_000)
	require.NoError(t, err)
}

// TestSanitizeCallBaseFeeNil verifies that when baseFee is nil (pre-London),
// GasPrice is set if not provided.
func TestSanitizeCallBaseFeeNil(t *testing.T) {
	sim := newTestSimulator(big.NewInt(1))
	args := callArgs()
	bc := blockCtx(30_000_000)

	err := sim.sanitizeCall(&args, nil, &bc, nil, 0, 50_000_000)
	require.NoError(t, err)
	assert.NotNil(t, args.GasPrice, "GasPrice should be set when baseFee is nil")
	assert.Nil(t, args.MaxFeePerGas, "MaxFeePerGas should not be set when baseFee is nil")
}

// TestSanitizeCallBaseFeeSet verifies that when baseFee is provided (post-London),
// MaxFeePerGas and MaxPriorityFeePerGas are set if not provided.
func TestSanitizeCallBaseFeeSet(t *testing.T) {
	sim := newTestSimulator(big.NewInt(1))
	args := callArgs()
	bc := blockCtx(30_000_000)
	baseFee := uint256.NewInt(1_000_000_000) // 1 gwei

	err := sim.sanitizeCall(&args, nil, &bc, baseFee, 0, 50_000_000)
	require.NoError(t, err)
	assert.NotNil(t, args.MaxFeePerGas, "MaxFeePerGas should be set when baseFee is provided")
	assert.NotNil(t, args.MaxPriorityFeePerGas, "MaxPriorityFeePerGas should be set when baseFee is provided")
	assert.Nil(t, args.GasPrice, "GasPrice should not be set when baseFee is provided")
}

// TestSanitizeCallBlobGas verifies that MaxFeePerBlobGas is set when BlobVersionedHashes is provided.
func TestSanitizeCallBlobGas(t *testing.T) {
	sim := newTestSimulator(big.NewInt(1))
	args := callArgs()
	args.BlobVersionedHashes = []common.Hash{{1}}
	bc := blockCtx(30_000_000)

	err := sim.sanitizeCall(&args, nil, &bc, nil, 0, 50_000_000)
	require.NoError(t, err)
	assert.NotNil(t, args.MaxFeePerBlobGas, "MaxFeePerBlobGas should be set when BlobVersionedHashes is present")
}

// TestSanitizeCallBlobGasNotSetWithoutHashes verifies that MaxFeePerBlobGas is not auto-set
// when BlobVersionedHashes is nil.
func TestSanitizeCallBlobGasNotSetWithoutHashes(t *testing.T) {
	sim := newTestSimulator(big.NewInt(1))
	args := callArgs()
	bc := blockCtx(30_000_000)

	err := sim.sanitizeCall(&args, nil, &bc, nil, 0, 50_000_000)
	require.NoError(t, err)
	assert.Nil(t, args.MaxFeePerBlobGas, "MaxFeePerBlobGas should not be set without BlobVersionedHashes")
}

// TestSanitizeCallGasDefaultCappedAtEffectiveCapWhenNoGlobalCap tests that with globalGasCap=0
// and a very large block gas limit, the gas is capped at MaxUint64/2.
func TestSanitizeCallGasDefaultCappedAtEffectiveCapWhenNoGlobalCap(t *testing.T) {
	sim := newTestSimulator(big.NewInt(1))
	args := callArgs()
	// Use a block gas limit larger than MaxUint64/2 to trigger the cap.
	hugeGasLimit := uint64(math.MaxUint64/2) + 1000
	bc := blockCtx(hugeGasLimit)

	err := sim.sanitizeCall(&args, nil, &bc, nil, 0, 0)
	require.NoError(t, err)
	// Gas should be capped to MaxUint64/2, not the full block remaining.
	assert.Equal(t, uint64(math.MaxUint64/2), uint64(*args.Gas))
}

// ─── txValidationError tests ─────────────────────────────────────────────────

func TestTxValidationErrorNil(t *testing.T) {
	assert.Nil(t, txValidationError(nil))
}

// ─── error helper tests ──────────────────────────────────────────────────────

func TestErrorHelpers(t *testing.T) {
	tests := []struct {
		name string
		fn   func(string) error
		code int
	}{
		{"invalidBlockNumber", invalidBlockNumberError, rpc.ErrCodeBlockNumberInvalid},
		{"invalidBlockTimestamp", invalidBlockTimestampError, rpc.ErrCodeBlockTimestampInvalid},
		{"blockGasLimitReached", blockGasLimitReachedError, rpc.ErrCodeBlockGasLimitReached},
		{"clientLimitExceeded", clientLimitExceededError, rpc.ErrCodeClientLimitExceeded},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn("test message")
			var customErr *rpc.CustomError
			require.ErrorAs(t, err, &customErr)
			assert.Equal(t, tc.code, customErr.Code)
			assert.Equal(t, "test message", customErr.Message)
		})
	}
}

// ─── repairLogs tests ────────────────────────────────────────────────────────

func TestRepairLogs(t *testing.T) {
	hash := common.HexToHash("0xdeadbeef")
	calls := []CallResult{
		{Logs: []*types.RPCLog{{}, {}}},
		{Logs: []*types.RPCLog{{}}},
		{Logs: nil},
	}
	repairLogs(calls, hash)
	assert.Equal(t, hash, calls[0].Logs[0].BlockHash)
	assert.Equal(t, hash, calls[0].Logs[1].BlockHash)
	assert.Equal(t, hash, calls[1].Logs[0].BlockHash)
}

func TestRepairLogsEmpty(t *testing.T) {
	// Should not panic with empty input.
	repairLogs(nil, common.Hash{})
	repairLogs([]CallResult{}, common.Hash{})
}

// ─── newSimulator tests ──────────────────────────────────────────────────────

func TestNewSimulatorGasPool(t *testing.T) {
	req := &SimulationRequest{TraceTransfers: true, Validation: true, ReturnFullTransactions: true}
	header := &types.Header{Number: *uint256.NewInt(1)}
	cfg := &chain.Config{ChainID: big.NewInt(1)}

	sim := newSimulator(req, header, cfg, datadir.Dirs{}, nil, rawdbv3.TxNumsReader{}, nil, nil, 50_000_000, 1024, 0, false)
	assert.Equal(t, uint64(50_000_000), sim.gasPool.Gas())
	assert.True(t, sim.traceTransfers)
	assert.True(t, sim.validation)
	assert.True(t, sim.fullTransactions)
}

func TestNewSimulatorZeroGasCap(t *testing.T) {
	req := &SimulationRequest{}
	header := &types.Header{Number: *uint256.NewInt(1)}
	cfg := &chain.Config{ChainID: big.NewInt(1)}

	sim := newSimulator(req, header, cfg, datadir.Dirs{}, nil, rawdbv3.TxNumsReader{}, nil, nil, 0, 1024, 0, false)
	assert.Equal(t, uint64(0), sim.gasPool.Gas())
}

// ─── SimulationRequest validation tests ──────────────────────────────────────

func TestSimulationRequestTypes(t *testing.T) {
	// Verify the SimulationRequest and related types are properly structured.
	req := SimulationRequest{
		BlockStateCalls:        []SimulatedBlock{{Calls: []ethapi.CallArgs{{}}}},
		TraceTransfers:         true,
		Validation:             false,
		ReturnFullTransactions: true,
	}
	assert.Len(t, req.BlockStateCalls, 1)
	assert.Len(t, req.BlockStateCalls[0].Calls, 1)
	assert.True(t, req.TraceTransfers)
	assert.True(t, req.ReturnFullTransactions)
}

// ─── simulatedCanonicalReader tests ──────────────────────────────────────────

func TestSimulatedCanonicalReaderIsCanonical(t *testing.T) {
	reader := &simulatedCanonicalReader{}
	ok, err := reader.IsCanonical(context.TODO(), nil, common.Hash{}, 0)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestSimulatedCanonicalReaderBadHeaderNumber(t *testing.T) {
	reader := &simulatedCanonicalReader{}
	blockHeight, err := reader.BadHeaderNumber(context.TODO(), nil, common.Hash{})
	assert.Nil(t, blockHeight)
	require.Error(t, err)
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func newUint64(n uint64) *hexutil.Uint64 {
	u := hexutil.Uint64(n)
	return &u
}

func newBig(n uint64) *hexutil.Big {
	return (*hexutil.Big)(new(big.Int).SetUint64(n))
}
