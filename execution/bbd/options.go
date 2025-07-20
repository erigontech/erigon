// Copyright 2025 The Erigon Authors
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

package bbd

import (
	"math"
	"time"

	"github.com/erigontech/erigon/polygon/p2p"
)

type Option func(requestConfig) requestConfig

func WithPeerId(peerId *p2p.PeerId) Option {
	return func(config requestConfig) requestConfig {
		config.peerId = peerId
		return config
	}
}

func WithBlocksBatchSize(blocksBatchSize uint64) Option {
	return func(config requestConfig) requestConfig {
		config.blocksBatchSize = blocksBatchSize
		return config
	}
}

func WithChainLengthLimit(limit uint64) Option {
	return func(config requestConfig) requestConfig {
		config.chainLengthLimit = limit
		return config
	}
}

func WithChainLengthCurrentHead(head uint64) Option {
	return func(config requestConfig) requestConfig {
		config.chainLengthCurrentHead = &head
		return config
	}
}

func applyOptions(opts ...Option) requestConfig {
	config := defaultRequestConfig
	for _, opt := range opts {
		config = opt(config)
	}
	return config
}

type requestConfig struct {
	peerId                       *p2p.PeerId
	blocksBatchSize              uint64
	chainLengthLimit             uint64
	chainLengthCurrentHead       *uint64
	maxParallelBodyDownloads     int
	initialHeaderFetchTimeout    time.Duration
	initialHeaderFetchRetries    uint64
	headerChainBatchFetchTimeout time.Duration
	headerChainBatchFetchRetries uint64
	bodiesBatchFetchTimeout      time.Duration
	bodiesBatchFetchRetries      uint64
}

var defaultRequestConfig = requestConfig{
	peerId:                       nil,
	blocksBatchSize:              500,
	chainLengthLimit:             math.MaxUint64,
	maxParallelBodyDownloads:     10,
	initialHeaderFetchTimeout:    time.Second, // note we want initial header fetch to be quick, so we terminate quickly
	initialHeaderFetchRetries:    0,           // in case of chainLengthLimit+chainLengthCurrentHead early termination
	headerChainBatchFetchTimeout: 10 * time.Second,
	headerChainBatchFetchRetries: 1,
	bodiesBatchFetchTimeout:      30 * time.Second,
	bodiesBatchFetchRetries:      1,
}
