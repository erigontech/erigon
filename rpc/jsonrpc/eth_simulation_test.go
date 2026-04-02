package jsonrpc

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/rpc/ethapi"
)

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

func TestSimulateSanitizeCallDefaultGas(t *testing.T) {
	const gasUsed = uint64(100)
	const blockTime = uint64(1)
	blockGasLimit := params.MaxTxnGasLimit + 1000 + gasUsed
	remaining := blockGasLimit - gasUsed

	tests := []struct {
		name      string
		sim       *simulator
		wantGas   uint64
		globalCap uint64
	}{
		{
			name:    "validation mode caps default gas at Osaka tx limit",
			sim:     &simulator{chainConfig: chain.TestChainOsakaConfig, validation: true},
			wantGas: params.MaxTxnGasLimit,
		},
		{
			name:    "non-validation mode keeps block remaining gas",
			sim:     &simulator{chainConfig: chain.TestChainOsakaConfig, validation: false},
			wantGas: remaining,
		},
		{
			name:    "amsterdam keeps full block remaining gas in validation mode",
			sim:     &simulator{chainConfig: chain.AllProtocolChanges, validation: true},
			wantGas: remaining,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nonce := hexutil.Uint64(0)
			args := ethapi.CallArgs{
				Nonce: &nonce, // Keep nonce explicit so sanitizeCall does not need state access.
			}
			blockCtx := evmtypes.BlockContext{
				GasLimit: blockGasLimit,
				Time:     blockTime,
			}

			err := tc.sim.sanitizeCall(&args, nil, &blockCtx, nil, gasUsed, tc.globalCap)
			if err != nil {
				t.Fatalf("sanitizeCall failed: %v", err)
			}
			if args.Gas == nil {
				t.Fatal("expected gas to be populated")
			}
			if got := uint64(*args.Gas); got != tc.wantGas {
				t.Fatalf("unexpected default gas: got %d, want %d", got, tc.wantGas)
			}
		})
	}
}

func newUint64(n uint64) *hexutil.Uint64 {
	u := hexutil.Uint64(n)
	return &u
}

func newBig(n uint64) *hexutil.Big {
	return (*hexutil.Big)(new(big.Int).SetUint64(n))
}
