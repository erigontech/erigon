package params

import (
	"crypto/ecdsa"
	"math/big"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
)

// MiningConfig is the configuration parameters of mining.
type MiningConfig struct {
	Enabled    bool
	EnabledPOS bool
	Noverify   bool              // Disable remote mining solution verification(only useful in ethash).
	Etherbase  libcommon.Address `toml:",omitempty"` // Public address for block mining rewards
	SigKey     *ecdsa.PrivateKey // ECDSA private key for signing blocks
	Notify     []string          `toml:",omitempty"` // HTTP URL list to be notified of new work packages(only useful in ethash).
	ExtraData  hexutility.Bytes  `toml:",omitempty"` // Block extra data set by the miner
	GasLimit   uint64            // Target gas limit for mined blocks.
	GasPrice   *big.Int          // Minimum gas price for mining a transaction
	Recommit   time.Duration     // The time interval for miner to re-create mining work.
}
