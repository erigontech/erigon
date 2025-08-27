// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package buildercfg

import (
	"crypto/ecdsa"
	"math/big"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
)

// MiningConfig is the configuration parameters of mining.
type MiningConfig struct {
	Enabled    bool
	EnabledPOS bool
	Noverify   bool              // Disable remote mining solution verification(only useful in ethash).
	Etherbase  common.Address    `toml:",omitempty"` // Public address for block mining rewards
	SigKey     *ecdsa.PrivateKey // ECDSA private key for signing blocks
	Notify     []string          `toml:",omitempty"` // HTTP URL list to be notified of new work packages(only useful in ethash).
	ExtraData  hexutil.Bytes     `toml:",omitempty"` // Block extra data set by the miner
	GasLimit   *uint64           // Target gas limit for mined blocks.
	GasPrice   *big.Int          // Minimum gas price for mining a transaction
	Recommit   time.Duration     // The time interval for miner to re-create mining work.
}
