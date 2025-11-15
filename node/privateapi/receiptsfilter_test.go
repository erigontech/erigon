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

package privateapi

import (
	"context"
	"io"
	"testing"

	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/node/shards"
)

var (
	txHash1     = common.HexToHash("0xffc4978dfe7ab496f0158ae8916adae6ffd0c1fca4f09f7a7134556011357424")
	txHash2     = common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	txHash1H256 *typesproto.H256
	txHash2H256 *typesproto.H256
)

func init() {
	txHash1H256 = gointerfaces.ConvertHashToH256(txHash1)
	txHash2H256 = gointerfaces.ConvertHashToH256(txHash2)
}

type testReceiptsServer struct {
	received         chan *remoteproto.ReceiptsFilterRequest
	receiveCompleted chan struct{}
	sent             []*remoteproto.SubscribeReceiptsReply
	ctx              context.Context
	grpc.ServerStream
}

func newTestReceiptsServer(ctx context.Context) *testReceiptsServer {
	ts := &testReceiptsServer{
		received:         make(chan *remoteproto.ReceiptsFilterRequest, 256),
		receiveCompleted: make(chan struct{}, 1),
		sent:             make([]*remoteproto.SubscribeReceiptsReply, 0),
		ctx:              ctx,
		ServerStream:     nil,
	}
	go func() {
		<-ts.ctx.Done()
		close(ts.received)
	}()
	return ts
}

func (ts *testReceiptsServer) Send(m *remoteproto.SubscribeReceiptsReply) error {
	ts.sent = append(ts.sent, m)
	return nil
}

func (ts *testReceiptsServer) Recv() (*remoteproto.ReceiptsFilterRequest, error) {
	// notify receive completed when the last request has been processed
	if len(ts.received) == 0 {
		ts.receiveCompleted <- struct{}{}
	}

	request, ok := <-ts.received
	if !ok {
		return nil, io.EOF
	}
	return request, nil
}

func createReceipt(txHash common.Hash) *remoteproto.SubscribeReceiptsReply {
	return &remoteproto.SubscribeReceiptsReply{
		BlockHash:         gointerfaces.ConvertHashToH256([32]byte{1}),
		BlockNumber:       100,
		TransactionHash:   gointerfaces.ConvertHashToH256(txHash),
		TransactionIndex:  0,
		Type:              0,
		Status:            1,
		CumulativeGasUsed: 21000,
		GasUsed:           21000,
		ContractAddress:   nil,
		Logs:              []*remoteproto.SubscribeLogsReply{},
		LogsBloom:         make([]byte, 256),
		From:              gointerfaces.ConvertAddressToH160([20]byte{2}),
		To:                gointerfaces.ConvertAddressToH160([20]byte{3}),
	}
}

func TestReceiptsFilter_EmptyFilter_DoesNotDistributeAnything(t *testing.T) {
	events := shards.NewEvents()
	agg := NewReceiptsFilterAggregator(events)

	ctx := t.Context()
	srv := newTestReceiptsServer(ctx)

	// Empty filter - no transaction hashes specified
	req1 := &remoteproto.ReceiptsFilterRequest{
		TransactionHashes: nil,
	}
	srv.received <- req1

	go func() {
		err := agg.subscribeReceipts(srv)
		if err != nil {
			t.Error(err)
		}
	}()

	<-srv.receiveCompleted

	// Try to distribute a receipt - but empty filter means nothing matches
	receipt := createReceipt(txHash1)
	_ = agg.distributeReceipts([]*remoteproto.SubscribeReceiptsReply{receipt})

	if len(srv.sent) != 0 {
		t.Error("expected the sent slice to be empty for empty filter")
	}
}

func TestReceiptsFilter_AllTransactionsFilter_DistributesAllReceipts(t *testing.T) {
	events := shards.NewEvents()
	agg := NewReceiptsFilterAggregator(events)

	ctx := t.Context()
	srv := newTestReceiptsServer(ctx)

	// Empty TransactionHashes means subscribe to all receipts
	req1 := &remoteproto.ReceiptsFilterRequest{
		TransactionHashes: []*typesproto.H256{},
	}
	srv.received <- req1

	go func() {
		err := agg.subscribeReceipts(srv)
		if err != nil {
			t.Error(err)
		}
	}()

	<-srv.receiveCompleted

	// Should distribute any receipt
	receipt1 := createReceipt(txHash1)
	_ = agg.distributeReceipts([]*remoteproto.SubscribeReceiptsReply{receipt1})
	if len(srv.sent) != 1 {
		t.Error("expected the sent slice to have the receipt present")
	}

	receipt2 := createReceipt(txHash2)
	_ = agg.distributeReceipts([]*remoteproto.SubscribeReceiptsReply{receipt2})
	if len(srv.sent) != 2 {
		t.Error("expected any receipt to be allowed through the filter")
	}
}

func TestReceiptsFilter_SpecificTransactionHash_OnlyAllowsThatTransactionThrough(t *testing.T) {
	events := shards.NewEvents()
	agg := NewReceiptsFilterAggregator(events)

	ctx := t.Context()
	srv := newTestReceiptsServer(ctx)

	// Filter for specific transaction hash
	req1 := &remoteproto.ReceiptsFilterRequest{
		TransactionHashes: []*typesproto.H256{txHash1H256},
	}
	srv.received <- req1

	go func() {
		err := agg.subscribeReceipts(srv)
		if err != nil {
			t.Error(err)
		}
	}()

	<-srv.receiveCompleted

	// Try with non-matching transaction hash
	receipt := createReceipt(txHash2)
	_ = agg.distributeReceipts([]*remoteproto.SubscribeReceiptsReply{receipt})
	if len(srv.sent) != 0 {
		t.Error("the sent slice should be empty as the transaction hash didn't match")
	}

	// Try with matching transaction hash
	receipt = createReceipt(txHash1)
	_ = agg.distributeReceipts([]*remoteproto.SubscribeReceiptsReply{receipt})
	if len(srv.sent) != 1 {
		t.Error("expected the receipt to be distributed as the transaction hash matched")
	}
}

func TestReceiptsFilter_MultipleTransactionHashes_AllowsAnyOfThem(t *testing.T) {
	events := shards.NewEvents()
	agg := NewReceiptsFilterAggregator(events)

	ctx := t.Context()
	srv := newTestReceiptsServer(ctx)

	// Filter for multiple transaction hashes
	req1 := &remoteproto.ReceiptsFilterRequest{
		TransactionHashes: []*typesproto.H256{txHash1H256, txHash2H256},
	}
	srv.received <- req1

	go func() {
		err := agg.subscribeReceipts(srv)
		if err != nil {
			t.Error(err)
		}
	}()

	<-srv.receiveCompleted

	// Try with first transaction hash
	receipt1 := createReceipt(txHash1)
	_ = agg.distributeReceipts([]*remoteproto.SubscribeReceiptsReply{receipt1})
	if len(srv.sent) != 1 {
		t.Error("expected the receipt to be distributed as txHash1 matched")
	}

	// Try with second transaction hash
	receipt2 := createReceipt(txHash2)
	_ = agg.distributeReceipts([]*remoteproto.SubscribeReceiptsReply{receipt2})
	if len(srv.sent) != 2 {
		t.Error("expected the receipt to be distributed as txHash2 matched")
	}

	// Try with non-matching transaction hash
	txHash3 := common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	receipt3 := createReceipt(txHash3)
	_ = agg.distributeReceipts([]*remoteproto.SubscribeReceiptsReply{receipt3})
	if len(srv.sent) != 2 {
		t.Error("the sent slice should not increase as txHash3 didn't match")
	}
}

func TestReceiptsFilter_UpdateFilter_ChangesWhatIsAllowed(t *testing.T) {
	events := shards.NewEvents()
	agg := NewReceiptsFilterAggregator(events)

	ctx := t.Context()
	srv := newTestReceiptsServer(ctx)

	// Start with filter for txHash1
	req1 := &remoteproto.ReceiptsFilterRequest{
		TransactionHashes: []*typesproto.H256{txHash1H256},
	}
	srv.received <- req1

	go func() {
		err := agg.subscribeReceipts(srv)
		if err != nil {
			t.Error(err)
		}
	}()

	<-srv.receiveCompleted

	// Should allow txHash1
	receipt1 := createReceipt(txHash1)
	_ = agg.distributeReceipts([]*remoteproto.SubscribeReceiptsReply{receipt1})
	if len(srv.sent) != 1 {
		t.Error("expected txHash1 to be allowed")
	}

	// Update filter to txHash2
	req2 := &remoteproto.ReceiptsFilterRequest{
		TransactionHashes: []*typesproto.H256{txHash2H256},
	}
	srv.received <- req2
	<-srv.receiveCompleted

	// Now txHash1 should be rejected
	receipt1Again := createReceipt(txHash1)
	_ = agg.distributeReceipts([]*remoteproto.SubscribeReceiptsReply{receipt1Again})
	if len(srv.sent) != 1 {
		t.Error("expected txHash1 to be rejected after filter update")
	}

	// And txHash2 should be allowed
	receipt2 := createReceipt(txHash2)
	_ = agg.distributeReceipts([]*remoteproto.SubscribeReceiptsReply{receipt2})
	if len(srv.sent) != 2 {
		t.Error("expected txHash2 to be allowed after filter update")
	}
}
