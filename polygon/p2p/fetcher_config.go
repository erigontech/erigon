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

package p2p

import (
	"math/rand"
	"time"
)

var defaultFetcherConfig = FetcherConfig{
	responseTimeout:    5 * time.Second,
	retryBackOff:       time.Second,
	maxRetries:         1,
	requestIdGenerator: rand.Uint64,
}

type RequestIdGenerator func() uint64

type FetcherConfig struct {
	responseTimeout    time.Duration
	retryBackOff       time.Duration
	maxRetries         uint64
	requestIdGenerator RequestIdGenerator
}

func (fc FetcherConfig) CopyWithOptions(opts ...FetcherOption) FetcherConfig {
	res := fc
	for _, opt := range opts {
		res = opt(res)
	}
	return res
}

type FetcherOption func(FetcherConfig) FetcherConfig

func WithResponseTimeout(responseTimeout time.Duration) FetcherOption {
	return func(config FetcherConfig) FetcherConfig {
		config.responseTimeout = responseTimeout
		return config
	}
}

func WithRetryBackOff(retryBackOff time.Duration) FetcherOption {
	return func(config FetcherConfig) FetcherConfig {
		config.retryBackOff = retryBackOff
		return config
	}
}

func WithMaxRetries(maxRetries uint64) FetcherOption {
	return func(config FetcherConfig) FetcherConfig {
		config.maxRetries = maxRetries
		return config
	}
}

func WithRequestIdGenerator(requestIdGenerator RequestIdGenerator) FetcherOption {
	return func(config FetcherConfig) FetcherConfig {
		config.requestIdGenerator = requestIdGenerator
		return config
	}
}
