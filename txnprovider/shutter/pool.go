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

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/txnprovider"
)

var _ txnprovider.TxnProvider = (*Pool)(nil)

type Pool struct {
	logger               log.Logger
	config               Config
	secondaryTxnProvider txnprovider.TxnProvider
}

func NewPool(logger log.Logger, config Config, secondaryTxnProvider txnprovider.TxnProvider) *Pool {
	return &Pool{
		logger:               logger.New("component", "shutter"),
		config:               config,
		secondaryTxnProvider: secondaryTxnProvider,
	}
}

func (p Pool) Run(ctx context.Context) error {
	p.logger.Info("running pool")
	//
	// TODO - start pool, sentinel listeners for keyper decryption keys and other necessary background goroutines
	//        blocks until all sub-components have shutdown or have error-ed
	//
	return nil
}

func (p Pool) ProvideTxns(ctx context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	//
	// TODO - implement shutter spec
	//        1) fetch corresponding txns for current slot and fill the remaining gas
	//           with the secondary txn provider (devp2p)
	//        2) if no decryption keys arrive for current slot then return empty transactions
	//
	return p.secondaryTxnProvider.ProvideTxns(ctx, opts...)
}
