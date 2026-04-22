package epbscfg

import (
	"math/big"

	"github.com/erigontech/erigon/common"
)

// Config holds the configuration for the ePBS builder.
type Config struct {
	Enabled      bool
	KeyPath      string         // path to BLS key file
	FeeRecipient common.Address // fee recipient address
	BidMargin    float64        // fraction of block value to bid (default 0.85)
	MinProfit    *big.Int       // minimum profit in wei to submit a bid
}

// DefaultConfig returns a Config with sane defaults.
func DefaultConfig() Config {
	return Config{
		BidMargin: 0.85,
		MinProfit: new(big.Int),
	}
}
