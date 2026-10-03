// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package epbs

import (
	"fmt"
	"math/big"

	"github.com/erigontech/erigon/cl/cltypes"
)

const (
	bidSkipAboveMax               = "above_max_bid"
	bidSkipBelowMinProfit         = "below_min_profit"
	bidSkipInsufficientCollateral = "insufficient_collateral"
)

type competitiveBidDecision struct {
	bid                 uint64
	maxBid              uint64
	highestSeen         uint64
	highestBuilderIndex uint64
	hasCompetitor       bool
	reason              string
}

func competitiveBid(
	base uint64,
	blockValue *big.Int,
	maxBidMargin float64,
	minProfitGwei uint64,
	highest *cltypes.SignedExecutionPayloadBid,
	builderIndex uint64,
) (competitiveBidDecision, error) {
	decision := competitiveBidDecision{bid: base}
	if highest != nil && highest.Message != nil {
		decision.highestSeen = highest.Message.Value
		decision.highestBuilderIndex = highest.Message.BuilderIndex
		decision.hasCompetitor = highest.Message.BuilderIndex != builderIndex
	}
	if !decision.hasCompetitor && minProfitGwei == 0 {
		return decision, nil
	}
	maxBid, _, err := bidValueGwei(FixedMarginStrategy{Margin: maxBidMargin}.Decide(0, blockValue))
	if err != nil {
		return decision, fmt.Errorf("epbs/coordinator: maximum %w", err)
	}
	skipReason := bidSkipAboveMax
	if minProfitGwei > 0 {
		profitCapGwei := new(big.Int).Quo(new(big.Int).Set(blockValue), big.NewInt(weiPerGwei))
		minProfit := new(big.Int).SetUint64(minProfitGwei)
		if profitCapGwei.Cmp(minProfit) < 0 {
			profitCapGwei.SetUint64(0)
		} else {
			profitCapGwei.Sub(profitCapGwei, minProfit)
		}
		if profitCapGwei.Cmp(new(big.Int).SetUint64(maxBid)) < 0 {
			maxBid = profitCapGwei.Uint64()
			skipReason = bidSkipBelowMinProfit
		}
	}
	decision.maxBid = maxBid
	if base > maxBid || decision.hasCompetitor && decision.highestSeen >= maxBid {
		decision.bid = 0
		decision.reason = skipReason
		return decision, nil
	}
	if !decision.hasCompetitor {
		return decision, nil
	}
	decision.bid = max(base, decision.highestSeen+1)
	return decision, nil
}
