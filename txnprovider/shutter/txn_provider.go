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

package shutter

import (
	"context"

	"github.com/erigontech/erigon/txnprovider"
)

const shutterProviderPriority = 110

var _ txnprovider.TxnProvider = TxnProvider{}

type TxnProvider struct {
	pool *Pool
}

func (p TxnProvider) Priority() int {
	return shutterProviderPriority
}

func (p TxnProvider) Yield(_ context.Context, _ ...txnprovider.YieldOption) (txnprovider.YieldResult, error) {
	return p.pool.Yield()
}
