package jsonrpc

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
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
	nonce := hexutil.Uint64(0)
	remainingGas := params.MaxTxnGasLimit + 100

	for _, tc := range []struct {
		name       string
		validation bool
		osakaTime  *uint64
		wantGas    uint64
	}{
		{
			name:       "caps default gas at Osaka limit in validation mode",
			validation: true,
			osakaTime:  common.NewUint64(0),
			wantGas:    params.MaxTxnGasLimit,
		},
		{
			name:       "keeps block remaining gas without validation",
			validation: false,
			osakaTime:  common.NewUint64(0),
			wantGas:    remainingGas,
		},
		{
			name:       "keeps block remaining gas before Osaka",
			validation: true,
			osakaTime:  common.NewUint64(remainingGas + 1),
			wantGas:    remainingGas,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := &ethapi.CallArgs{Nonce: &nonce}
			sim := &simulator{
				validation: tc.validation,
				chainConfig: &chain.Config{
					ChainID:   big.NewInt(1),
					OsakaTime: tc.osakaTime,
				},
			}
			blockCtx := &evmtypes.BlockContext{
				GasLimit: remainingGas,
				Time:     remainingGas,
			}

			if err := sim.sanitizeCall(args, nil, blockCtx, nil, 0, 0); err != nil {
				t.Fatalf("sanitizeCall returned error: %v", err)
			}
			if args.Gas == nil {
				t.Fatal("gas was not set")
			}
			if have := uint64(*args.Gas); have != tc.wantGas {
				t.Fatalf("unexpected gas: have %d want %d", have, tc.wantGas)
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
