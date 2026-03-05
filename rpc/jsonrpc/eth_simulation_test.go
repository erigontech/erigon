package jsonrpc

import (
	"math/big"
	"testing"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/transactions"
)

func TestSimulateSanitizeBlockOrder(t *testing.T) {
	type result struct {
		number    uint64
		timestamp uint64
	}
	for i, tc := range []struct {
		baseNumber    int
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
			blocks:        []SimulatedBlock{{BlockOverrides: &transactions.BlockOverrides{BlockNumber: newUint64(13), Timestamp: newUint64(80)}}, {}},
			expected:      []result{{number: 11, timestamp: 62}, {number: 12, timestamp: 74}, {number: 13, timestamp: 80}, {number: 14, timestamp: 92}},
		},
		{
			baseNumber:    10,
			baseTimestamp: 50,
			blocks:        []SimulatedBlock{{BlockOverrides: &transactions.BlockOverrides{BlockNumber: newUint64(11)}}, {BlockOverrides: &transactions.BlockOverrides{BlockNumber: newUint64(14)}}, {}},
			expected:      []result{{number: 11, timestamp: 62}, {number: 12, timestamp: 74}, {number: 13, timestamp: 86}, {number: 14, timestamp: 98}, {number: 15, timestamp: 110}},
		},
		{
			baseNumber:    10,
			baseTimestamp: 50,
			blocks:        []SimulatedBlock{{BlockOverrides: &transactions.BlockOverrides{BlockNumber: newUint64(13)}}, {BlockOverrides: &transactions.BlockOverrides{BlockNumber: newUint64(12)}}},
			err:           "block numbers must be in order: 12 <= 13",
		},
		{
			baseNumber:    10,
			baseTimestamp: 50,
			blocks:        []SimulatedBlock{{BlockOverrides: &transactions.BlockOverrides{BlockNumber: newUint64(13), Timestamp: newUint64(74)}}},
			err:           "block timestamps must be in order: 74 <= 74",
		},
		{
			baseNumber:    10,
			baseTimestamp: 50,
			blocks:        []SimulatedBlock{{BlockOverrides: &transactions.BlockOverrides{BlockNumber: newUint64(11), Timestamp: newUint64(60)}}, {BlockOverrides: &transactions.BlockOverrides{BlockNumber: newUint64(12), Timestamp: newUint64(55)}}},
			err:           "block timestamps must be in order: 55 <= 60",
		},
		{
			baseNumber:    10,
			baseTimestamp: 50,
			blocks:        []SimulatedBlock{{BlockOverrides: &transactions.BlockOverrides{BlockNumber: newUint64(11), Timestamp: newUint64(60)}}, {BlockOverrides: &transactions.BlockOverrides{BlockNumber: newUint64(13), Timestamp: newUint64(72)}}},
			err:           "block timestamps must be in order: 72 <= 72",
		},
	} {
		sim := &simulator{base: &types.Header{Number: big.NewInt(int64(tc.baseNumber)), Time: tc.baseTimestamp}}
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
			if b.BlockOverrides.BlockNumber == nil {
				t.Fatalf("testcase %d: block number not set", i)
			}
			if b.BlockOverrides.Timestamp == nil {
				t.Fatalf("testcase %d: block time not set", i)
			}
			if uint64(*b.BlockOverrides.Timestamp) != tc.expected[bi].timestamp {
				t.Errorf("testcase %d: block timestamp mismatch. Want %d, have %d", i, tc.expected[bi].timestamp, uint64(*b.BlockOverrides.Timestamp))
			}
			have := b.BlockOverrides.BlockNumber.Uint64()
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
