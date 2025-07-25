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

package txpool

import (
	"sync"

	"github.com/erigontech/erigon/execution/consensus/misc"
)

type Option func(*options)

func WithFeeCalculator(f FeeCalculator) Option {
	return func(o *options) {
		o.feeCalculator = f
	}
}

func WithPoolDBInitializer(init poolDBInitializer) Option {
	return func(o *options) {
		o.poolDBInitializer = init
	}
}

func WithP2PFetcherWg(wg *sync.WaitGroup) Option {
	return func(o *options) {
		o.p2pFetcherWg = wg
	}
}

func WithP2PSenderWg(wg *sync.WaitGroup) Option {
	return func(o *options) {
		o.p2pSenderWg = wg
	}
}

type options struct {
	feeCalculator     FeeCalculator
	poolDBInitializer poolDBInitializer
	p2pSenderWg       *sync.WaitGroup
	p2pFetcherWg      *sync.WaitGroup
}

func applyOpts(opts ...Option) options {
	overriddenDefaults := defaultOptions
	for _, opt := range opts {
		opt(&overriddenDefaults)
	}
	return overriddenDefaults
}

var defaultOptions = options{
	poolDBInitializer: defaultPoolDBInitializer,
	feeCalculator:     misc.Eip1559FeeCalculator,
}
