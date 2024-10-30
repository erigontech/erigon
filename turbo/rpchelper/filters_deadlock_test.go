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

package rpchelper_test

import (
	"context"
	"testing"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	types2 "github.com/erigontech/erigon-lib/gointerfaces/typesproto"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/v3/core/types"
	"github.com/erigontech/erigon/v3/eth/filters"
	"github.com/erigontech/erigon/v3/turbo/rpchelper"
)

func TestFiltersDeadlock_Test(t *testing.T) {
	t.Parallel()
	logger := log.New()
	config := rpchelper.FiltersConfig{}
	f := rpchelper.New(context.TODO(), config, nil, nil, nil, func() {}, logger)
	crit := filters.FilterCriteria{
		Addresses: nil,
		Topics:    [][]libcommon.Hash{},
	}
	subCount := 20
	logCount := 100
	type sub struct {
		id rpchelper.LogsSubID
		ch <-chan *types.Log
	}
	ctx, cancel := context.WithCancel(context.TODO())
	for i := 0; i < subCount; i++ {
		n := &sub{}
		n.ch, n.id = f.SubscribeLogs(128, crit)
		// start a loop similar to an rpcdaemon subscription, that calls unsubscribe on return
		go func() {
			defer f.UnsubscribeLogs(n.id)
			for {
				select {
				case l := <-n.ch:
					_ = l
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	// cancel the subs at the same time
	cancel()
	// try to send logs
	for i := 0; i < logCount; i++ {
		log := createLog()
		// this will deadlock
		f.OnNewLogs(log)
	}
}

func createLog() *remote.SubscribeLogsReply {
	return &remote.SubscribeLogsReply{
		Address:          gointerfaces.ConvertAddressToH160([20]byte{}),
		BlockHash:        gointerfaces.ConvertHashToH256([32]byte{}),
		BlockNumber:      0,
		Data:             []byte{},
		LogIndex:         0,
		Topics:           []*types2.H256{gointerfaces.ConvertHashToH256([32]byte{99, 99})},
		TransactionHash:  gointerfaces.ConvertHashToH256([32]byte{}),
		TransactionIndex: 0,
		Removed:          false,
	}
}
