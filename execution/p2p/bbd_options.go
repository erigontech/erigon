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

package p2p

import (
	"math"

	"github.com/erigontech/erigon/execution/p2p/p2pcfg"
)

type BbdOption func(bbdRequestConfig) bbdRequestConfig

func WithPeerId(peerId *PeerId) BbdOption {
	return func(config bbdRequestConfig) bbdRequestConfig {
		config.peerId = peerId
		return config
	}
}

func WithBlocksBatchSize(blocksBatchSize uint64) BbdOption {
	return func(config bbdRequestConfig) bbdRequestConfig {
		config.BlocksBatchSize = blocksBatchSize
		return config
	}
}

func WithChainLengthLimit(limit uint64) BbdOption {
	return func(config bbdRequestConfig) bbdRequestConfig {
		config.chainLengthLimit = limit
		return config
	}
}

func WithChainLengthCurrentHead(head uint64) BbdOption {
	return func(config bbdRequestConfig) bbdRequestConfig {
		config.chainLengthCurrentHead = &head
		return config
	}
}

func applyBbdOptions(blockDownload p2pcfg.BlockDownload, opts ...BbdOption) bbdRequestConfig {
	config := defaultBbdRequestConfig
	config.BlockDownload = blockDownload
	for _, opt := range opts {
		config = opt(config)
	}
	return config
}

type bbdRequestConfig struct {
	p2pcfg.BlockDownload
	peerId                 *PeerId
	chainLengthLimit       uint64
	chainLengthCurrentHead *uint64
}

var defaultBbdRequestConfig = bbdRequestConfig{
	BlockDownload:    p2pcfg.DefaultBlockDownload,
	peerId:           nil,
	chainLengthLimit: math.MaxUint64,
}
