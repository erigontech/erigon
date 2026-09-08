package epbs

import (
	"math"
	"math/big"
)

const marginPrecision = 1_000_000_000

// FixedMarginStrategy bids a fixed fraction of the block value subject to a profit floor.
type FixedMarginStrategy struct {
	Margin    float64
	MinProfit *big.Int
}

func (s FixedMarginStrategy) Decide(_ uint64, blockValue *big.Int) *big.Int {
	if blockValue == nil || blockValue.Sign() <= 0 {
		return nil
	}
	if math.IsNaN(s.Margin) || math.IsInf(s.Margin, 0) || s.Margin < 0 || s.Margin > 1 {
		return nil
	}

	scaledMargin := big.NewInt(int64(math.Floor(s.Margin * marginPrecision)))
	bid := new(big.Int).Mul(blockValue, scaledMargin)
	bid.Div(bid, big.NewInt(marginPrecision))
	if s.MinProfit != nil && new(big.Int).Sub(blockValue, bid).Cmp(s.MinProfit) < 0 {
		return nil
	}
	return bid
}
