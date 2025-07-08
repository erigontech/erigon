package bbd

import (
	"math"

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

func applyOptions(opts ...Option) requestConfig {
	config := defaultRequestConfig
	for _, opt := range opts {
		config = opt(config)
	}
	return config
}

type requestConfig struct {
	peerId           *p2p.PeerId
	blocksBatchSize  uint64
	chainLengthLimit uint64
}

var defaultRequestConfig = requestConfig{
	peerId:           nil,
	blocksBatchSize:  500,
	chainLengthLimit: math.MaxUint64,
}
