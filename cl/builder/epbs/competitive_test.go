// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package epbs

import (
	"math"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
)

func TestCompetitiveBid(t *testing.T) {
	blockValue := new(big.Int).Mul(big.NewInt(100), big.NewInt(weiPerGwei))
	overflowingBlockValue := new(big.Int).Lsh(big.NewInt(1), 100)
	best := func(value, builderIndex uint64) *cltypes.SignedExecutionPayloadBid {
		return &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
			Value: value, BuilderIndex: builderIndex,
		}}
	}

	for _, test := range []struct {
		name           string
		blockValue     *big.Int
		baseBid        uint64
		minProfit      uint64
		best           *cltypes.SignedExecutionPayloadBid
		wantBid        uint64
		wantMax        uint64
		wantHighest    uint64
		wantBuilder    uint64
		wantCompetitor bool
		wantReason     string
		wantError      string
	}{
		{name: "no competing bid", blockValue: blockValue, baseBid: 85, wantBid: 85},
		{name: "margin bid below minimum profit", blockValue: blockValue, baseBid: 85, minProfit: 16, wantMax: 84, wantReason: bidSkipBelowMinProfit},
		{name: "margin bid at minimum profit", blockValue: blockValue, baseBid: 85, minProfit: 15, wantBid: 85, wantMax: 85},
		{name: "minimum profit equals block value", blockValue: blockValue, baseBid: 85, minProfit: 100, wantReason: bidSkipBelowMinProfit},
		{name: "minimum profit above block value", blockValue: blockValue, baseBid: 85, minProfit: 1000, wantReason: bidSkipBelowMinProfit},
		{name: "profit cap floors wei", blockValue: big.NewInt(99_900_000_000), baseBid: 85, minProfit: 15, wantMax: 84, wantReason: bidSkipBelowMinProfit},
		{name: "best is our builder", blockValue: blockValue, baseBid: 85, best: best(96, 7), wantBid: 85, wantHighest: 96, wantBuilder: 7},
		{name: "own bid is ignored with profit limit", blockValue: blockValue, baseBid: 85, minProfit: 5, best: best(96, 7), wantBid: 85, wantMax: 95, wantHighest: 96, wantBuilder: 7},
		{name: "above highest", blockValue: blockValue, baseBid: 85, best: best(90, 8), wantBid: 91, wantMax: 97, wantHighest: 90, wantBuilder: 8, wantCompetitor: true},
		{name: "above highest within profit limit", blockValue: blockValue, baseBid: 85, minProfit: 5, best: best(89, 8), wantBid: 90, wantMax: 95, wantHighest: 89, wantBuilder: 8, wantCompetitor: true},
		{name: "competitor crosses profit limit", blockValue: blockValue, baseBid: 85, minProfit: 10, best: best(90, 8), wantMax: 90, wantHighest: 90, wantBuilder: 8, wantCompetitor: true, wantReason: bidSkipBelowMinProfit},
		{name: "above highest crosses profit limit", blockValue: blockValue, baseBid: 85, minProfit: 16, best: best(84, 8), wantMax: 84, wantHighest: 84, wantBuilder: 8, wantCompetitor: true, wantReason: bidSkipBelowMinProfit},
		{name: "best plus one equals cap", blockValue: blockValue, baseBid: 85, best: best(96, 8), wantBid: 97, wantMax: 97, wantHighest: 96, wantBuilder: 8, wantCompetitor: true},
		{name: "best plus one exceeds cap", blockValue: blockValue, baseBid: 85, best: best(97, 8), wantMax: 97, wantHighest: 97, wantBuilder: 8, wantCompetitor: true, wantReason: bidSkipAboveMax},
		{name: "margin limit binds before profit limit", blockValue: blockValue, baseBid: 85, minProfit: 1, best: best(97, 8), wantMax: 97, wantHighest: 97, wantBuilder: 8, wantCompetitor: true, wantReason: bidSkipAboveMax},
		{name: "equal limits use margin reason", blockValue: blockValue, baseBid: 85, minProfit: 3, best: best(97, 8), wantMax: 97, wantHighest: 97, wantBuilder: 8, wantCompetitor: true, wantReason: bidSkipAboveMax},
		{name: "best plus one overflows", blockValue: blockValue, baseBid: 85, best: best(math.MaxUint64, 8), wantMax: 97, wantHighest: math.MaxUint64, wantBuilder: 8, wantCompetitor: true, wantReason: bidSkipAboveMax},
		{name: "wei to gwei floors", blockValue: big.NewInt(10_999_999_999), baseBid: 9, best: best(9, 8), wantBid: 10, wantMax: 10, wantHighest: 9, wantBuilder: 8, wantCompetitor: true},
		{name: "competing cap overflows gwei", blockValue: overflowingBlockValue, baseBid: 1, best: best(1, 8), wantBid: 1, wantHighest: 1, wantBuilder: 8, wantCompetitor: true, wantError: "epbs/coordinator: maximum bid exceeds uint64 gwei"},
		{name: "absent bid ignores unused cap overflow", blockValue: overflowingBlockValue, baseBid: 1, wantBid: 1},
		{name: "own bid ignores unused cap overflow", blockValue: overflowingBlockValue, baseBid: 1, best: best(1, 7), wantBid: 1, wantHighest: 1, wantBuilder: 7},
	} {
		t.Run(test.name, func(t *testing.T) {
			decision, err := competitiveBid(test.baseBid, test.blockValue, 0.97, test.minProfit, test.best, 7)
			require.Equal(t, test.wantBid, decision.bid)
			require.Equal(t, test.wantMax, decision.maxBid)
			require.Equal(t, test.wantHighest, decision.highestSeen)
			require.Equal(t, test.wantBuilder, decision.highestBuilderIndex)
			require.Equal(t, test.wantCompetitor, decision.hasCompetitor)
			require.Equal(t, test.wantReason, decision.reason)
			if test.wantError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, test.wantError)
			}
		})
	}
}
