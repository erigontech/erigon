// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package ethconfig

import (
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/txpool/txpoolcfg"
)

// DeprecatedTxPoolConfig are the configuration parameters of the transaction pool.
type DeprecatedTxPoolConfig struct {
	Disable  bool
	Locals   []common.Address // Addresses that should be treated by default as local
	NoLocals bool             // Whether local transaction handling should be disabled

	PriceLimit uint64 // Minimum gas price to enforce for acceptance into the pool
	PriceBump  uint64 // Minimum price bump percentage to replace an already existing transaction (nonce)

	AccountSlots uint64 // Number of executable transaction slots guaranteed per account
	GlobalSlots  uint64 // Maximum number of executable transaction slots for all accounts
	AccountQueue uint64 // Maximum number of non-executable transaction slots permitted per account
	GlobalQueue  uint64 // Maximum number of non-executable transaction slots for all accounts

	GlobalBaseFeeQueue uint64 // Maximum number of non-executable transaction slots for all accounts

	Lifetime      time.Duration // Maximum amount of time non-executable transaction are queued
	StartOnInit   bool
	TracedSenders []string // List of senders for which tx pool should print out debugging info
	CommitEvery   time.Duration
}

// DeprecatedDefaultTxPoolConfig contains the default configurations for the transaction
// pool.
var DeprecatedDefaultTxPoolConfig = DeprecatedTxPoolConfig{
	PriceLimit: 1,
	PriceBump:  10,

	AccountSlots:       16,
	GlobalSlots:        10_000,
	GlobalBaseFeeQueue: 30_000,
	AccountQueue:       64,
	GlobalQueue:        30_000,

	Lifetime: 3 * time.Hour,
}

var DefaultTxPool2Config = func(fullCfg *Config) txpoolcfg.Config {
	pool1Cfg := &fullCfg.DeprecatedTxPool
	cfg := txpoolcfg.DefaultConfig
	cfg.PendingSubPoolLimit = int(pool1Cfg.GlobalSlots)
	cfg.BaseFeeSubPoolLimit = int(pool1Cfg.GlobalBaseFeeQueue)
	cfg.QueuedSubPoolLimit = int(pool1Cfg.GlobalQueue)
	cfg.PriceBump = pool1Cfg.PriceBump
	cfg.BlobPriceBump = fullCfg.TxPool.BlobPriceBump
	cfg.MinFeeCap = pool1Cfg.PriceLimit
	cfg.AccountSlots = pool1Cfg.AccountSlots
	cfg.BlobSlots = fullCfg.TxPool.BlobSlots
	cfg.LogEvery = 1 * time.Minute
	cfg.CommitEvery = 5 * time.Minute
	cfg.TracedSenders = pool1Cfg.TracedSenders
	cfg.CommitEvery = pool1Cfg.CommitEvery

	return cfg
}
