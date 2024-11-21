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

package txnprovider

import (
	"context"

	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/txnprovider/shutter"
)

const shutterProviderPriority = 110

var _ Provider = ShutterProvider{}

type ShutterProvider struct {
	shutterPool *shutter.Pool
}

func (s ShutterProvider) Priority() uint64 {
	return shutterProviderPriority
}

func (s ShutterProvider) Yield(_ context.Context, _ ...YieldOption) ([]types.Transaction, error) {
	return s.shutterPool.Yield()
}
