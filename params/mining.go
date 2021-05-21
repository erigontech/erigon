package params

import (
	"crypto/ecdsa"
	"math/big"
	"time"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
)

// MiningConfig is the configuration parameters of mining.
type MiningConfig struct {
	Enabled   bool
	Noverify  bool              // Disable remote mining solution verification(only useful in ethash).
	Etherbase common.Address    `toml:",omitempty"` // Public address for block mining rewards
	SigKey    *ecdsa.PrivateKey // ECDSA private key for signing blocks
	Notify    []string          `toml:",omitempty"` // HTTP URL list to be notified of new work packages(only useful in ethash).
	ExtraData hexutil.Bytes     `toml:",omitempty"` // Block extra data set by the miner
	GasFloor  uint64            // Target gas floor for mined blocks.
	GasCeil   uint64            // Target gas ceiling for mined blocks.
	GasPrice  *big.Int          // Minimum gas price for mining a transaction
	Recommit  time.Duration     // The time interval for miner to re-create mining work.
}
