package rpchelper_test

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
)

func TestFiltersDeadlock_Test(t *testing.T) {
	f := rpchelper.New(context.TODO(), nil, nil, nil, func() {})
	crit := filters.FilterCriteria{
		Addresses: nil,
		Topics:    [][]common.Hash{},
	}
	subCount := 2
	logCount := 100
	type sub struct {
		id rpchelper.LogsSubID
		ch chan *types.Log
	}
	ctx, cancel := context.WithCancel(context.TODO())
	subs := []*sub{}
	for i := 0; i < subCount; i++ {
		n := &sub{}
		n.ch = make(chan *types.Log, 1)
		n.id = f.SubscribeLogs(n.ch, crit)
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
		subs = append(subs, n)
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
