package epbs

import (
	"math"
	"math/big"
)

// FixedMarginStrategy bids a fixed fraction of the block value.
// Margin is the fraction of block value to bid (e.g., 0.85 means the builder
// keeps 15% profit). MinProfit is the minimum profit required to submit a bid;
// if profit < MinProfit the slot is skipped.
type FixedMarginStrategy struct {
	Margin    float64  // e.g. 0.85 = bid 85% of block value
	MinProfit *big.Int // minimum profit in wei; nil means no minimum
}

const marginPrecision = 1_000_000_000 // 1e9 — 9 decimal places

func (s *FixedMarginStrategy) Decide(_ uint64, blockValue *big.Int) *big.Int {
	if blockValue == nil || blockValue.Sign() <= 0 {
		return nil
	}

	// Reject invalid margins: must be finite and in [0, 1].
	if math.IsNaN(s.Margin) || math.IsInf(s.Margin, 0) || s.Margin < 0 || s.Margin > 1 {
		return nil
	}

	// bid = floor(blockValue * margin)
	scaledMargin := big.NewInt(int64(math.Round(s.Margin * marginPrecision)))
	bid := new(big.Int).Mul(blockValue, scaledMargin)
	bid.Div(bid, big.NewInt(marginPrecision))

	// profit = blockValue - bid
	profit := new(big.Int).Sub(blockValue, bid)

	if s.MinProfit != nil && profit.Cmp(s.MinProfit) < 0 {
		return nil
	}

	return bid
}
