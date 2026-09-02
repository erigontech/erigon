package jsonrpc

import (
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
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

func newUint64(n uint64) *hexutil.Uint64 {
	u := hexutil.Uint64(n)
	return &u
}

func newBig(n uint64) *hexutil.Big {
	return (*hexutil.Big)(new(big.Int).SetUint64(n))
}

// TestSimulateV1BlockHashOfEarlierSimulatedBlock pins that BLOCKHASH inside a
// simulated block resolves an earlier simulated block to that block's own hash,
// not to the hash of the real canonical block sitting at the same number. The
// base is historical on purpose: only then does the canonical chain hold a
// competing block at a simulated block number.
func TestSimulateV1BlockHashOfEarlierSimulatedBlock(t *testing.T) {
	gspec := &types.Genesis{Config: chain.TestChainConfig, Difficulty: params.GenesisDifficulty}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec))
	canonicalChain, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 4, func(int, *blockgen.BlockGen) {})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(canonicalChain))

	ctx := context.Background()
	ff := rpchelper.New(ctx, rpchelper.DefaultFiltersConfig, nil, nil, nil, func() {}, m.Log)
	api := newEthApiForTest(newBaseApiWithFiltersForTest(ff, kvcache.New(kvcache.DefaultCoherentConfig), m), m.DB, nil, nil)

	const baseNumber = uint64(1)
	canonical := canonicalChain.Blocks[baseNumber] // canonical block at baseNumber+1
	require.Equal(t, baseNumber+1, canonical.NumberU64())

	// runtime code returning blockhash(calldataload(0))
	probe := common.Address{0xbb}
	code := hexutil.Bytes(hexutil.MustDecodeHex("0x6000354060005260206000f3"))
	arg := hexutil.Bytes(common.BigToHash(new(big.Int).SetUint64(baseNumber + 1)).Bytes())

	res, err := api.SimulateV1(ctx, SimulationRequest{BlockStateCalls: []SimulatedBlock{
		{},
		{
			StateOverrides: &ethapi.StateOverrides{accounts.InternAddress(probe): ethapi.Account{Code: &code}},
			Calls:          []ethapi.CallArgs{{To: &probe, Data: &arg}},
		},
	}}, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(baseNumber)))
	require.NoError(t, err)
	require.Len(t, res, 2)

	simulated, ok := res[0]["hash"].(common.Hash)
	require.True(t, ok, "simulated block hash missing: %v", res[0]["hash"])
	require.NotEqual(t, canonical.Hash(), simulated, "test is only meaningful if the two hashes differ")

	calls, ok := res[1]["calls"].([]CallResult)
	require.True(t, ok)
	require.Len(t, calls, 1)
	require.Equal(t, hexutil.Uint64(1), calls[0].Status, "call failed: %v", calls[0].Error)
	require.Equal(t, simulated.Hex(), calls[0].ReturnData)
}
