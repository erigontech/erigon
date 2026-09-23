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
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/notifications"
	"github.com/erigontech/erigon/execution/types"
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

func createReceiptNotification(txHash common.Hash) *notifications.ReceiptNotification {
	return &notifications.ReceiptNotification{
		Receipt: &types.Receipt{
			BlockHash:         common.Hash{1},
			BlockNumber:       uint256.NewInt(100),
			TxHash:            txHash,
			TransactionIndex:  0,
			Type:              0,
			Status:            1,
			CumulativeGasUsed: 21000,
			GasUsed:           21000,
			Logs:              []*types.Log{},
		},
	}
}

func TestReceiptsFilter_EmptyFilter_DoesNotDistributeAnything(t *testing.T) {
	events := shards.NewEvents()
	agg := NewReceiptsFilterAggregator(events, chain.AllProtocolChanges)

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
	receipt := createReceiptNotification(txHash1)
	agg.distributeReceipts([]*notifications.ReceiptNotification{receipt})

	if len(srv.sent) != 0 {
		t.Error("expected the sent slice to be empty for empty filter")
	}
}

func TestReceiptsFilter_AllTransactionsFilter_DistributesAllReceipts(t *testing.T) {
	events := shards.NewEvents()
	agg := NewReceiptsFilterAggregator(events, chain.AllProtocolChanges)

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
	receipt1 := createReceiptNotification(txHash1)
	agg.distributeReceipts([]*notifications.ReceiptNotification{receipt1})
	if len(srv.sent) != 1 {
		t.Error("expected the sent slice to have the receipt present")
	}

	receipt2 := createReceiptNotification(txHash2)
	agg.distributeReceipts([]*notifications.ReceiptNotification{receipt2})
	if len(srv.sent) != 2 {
		t.Error("expected any receipt to be allowed through the filter")
	}
}

func TestReceiptsFilter_SpecificTransactionHash_OnlyAllowsThatTransactionThrough(t *testing.T) {
	events := shards.NewEvents()
	agg := NewReceiptsFilterAggregator(events, chain.AllProtocolChanges)

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
	receipt := createReceiptNotification(txHash2)
	agg.distributeReceipts([]*notifications.ReceiptNotification{receipt})
	if len(srv.sent) != 0 {
		t.Error("the sent slice should be empty as the transaction hash didn't match")
	}

	// Try with matching transaction hash
	receipt = createReceiptNotification(txHash1)
	agg.distributeReceipts([]*notifications.ReceiptNotification{receipt})
	if len(srv.sent) != 1 {
		t.Error("expected the receipt to be distributed as the transaction hash matched")
	}
}

func TestReceiptsFilter_MultipleTransactionHashes_AllowsAnyOfThem(t *testing.T) {
	events := shards.NewEvents()
	agg := NewReceiptsFilterAggregator(events, chain.AllProtocolChanges)

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
	receipt1 := createReceiptNotification(txHash1)
	agg.distributeReceipts([]*notifications.ReceiptNotification{receipt1})
	if len(srv.sent) != 1 {
		t.Error("expected the receipt to be distributed as txHash1 matched")
	}

	// Try with second transaction hash
	receipt2 := createReceiptNotification(txHash2)
	agg.distributeReceipts([]*notifications.ReceiptNotification{receipt2})
	if len(srv.sent) != 2 {
		t.Error("expected the receipt to be distributed as txHash2 matched")
	}

	// Try with non-matching transaction hash
	txHash3 := common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
	receipt3 := createReceiptNotification(txHash3)
	agg.distributeReceipts([]*notifications.ReceiptNotification{receipt3})
	if len(srv.sent) != 2 {
		t.Error("the sent slice should not increase as txHash3 didn't match")
	}
}

func TestReceiptsFilter_UpdateFilter_ChangesWhatIsAllowed(t *testing.T) {
	events := shards.NewEvents()
	agg := NewReceiptsFilterAggregator(events, chain.AllProtocolChanges)

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
	receipt1 := createReceiptNotification(txHash1)
	agg.distributeReceipts([]*notifications.ReceiptNotification{receipt1})
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
	receipt1Again := createReceiptNotification(txHash1)
	agg.distributeReceipts([]*notifications.ReceiptNotification{receipt1Again})
	if len(srv.sent) != 1 {
		t.Error("expected txHash1 to be rejected after filter update")
	}

	// And txHash2 should be allowed
	receipt2 := createReceiptNotification(txHash2)
	agg.distributeReceipts([]*notifications.ReceiptNotification{receipt2})
	if len(srv.sent) != 2 {
		t.Error("expected txHash2 to be allowed after filter update")
	}
}

// A receipt notification carries the transaction as executed, whose sender may
// no longer be cached, so the conversion must recover it from the chain config.
func TestReceiptsFilter_RecoversSenderWithoutCachedFrom(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	chainConfig := chain.AllProtocolChanges
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	txn, err := types.SignNewTx(key, *types.LatestSigner(chainConfig), &types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{Nonce: 3, GasLimit: 21000, To: &to},
		ChainID:  *chainConfig.ChainID,
		TipCap:   *uint256.NewInt(2),
		FeeCap:   *uint256.NewInt(100),
	})
	require.NoError(t, err)

	agg := NewReceiptsFilterAggregator(shards.NewEvents(), chainConfig)
	rn := createReceiptNotification(txHash1)
	rn.Tx = txn

	proto := agg.receiptNotificationToProto(rn)
	require.NotNil(t, proto.From)
	assert.Equal(t, crypto.PubkeyToAddress(key.PublicKey), common.Address(gointerfaces.ConvertH160toAddress(proto.From)))
}

// The RPC side sends one notification per block, so the last receipt of a block that a stream
// gets carries the flag, whichever receipts its filter lets through.
func TestReceiptsFilter_FlagsLastReceiptOfBlockPerStream(t *testing.T) {
	for name, tc := range map[string]struct {
		hashes []*typesproto.H256
		want   []bool
	}{
		"all receipts":  {[]*typesproto.H256{}, []bool{false, true}},
		"first matched": {[]*typesproto.H256{gointerfaces.ConvertHashToH256(txHash1)}, []bool{true}},
	} {
		t.Run(name, func(t *testing.T) {
			agg := NewReceiptsFilterAggregator(shards.NewEvents(), chain.AllProtocolChanges)
			srv := newTestReceiptsServer(t.Context())
			srv.received <- &remoteproto.ReceiptsFilterRequest{TransactionHashes: tc.hashes}
			go func() {
				if err := agg.subscribeReceipts(srv); err != nil {
					t.Error(err)
				}
			}()
			<-srv.receiveCompleted

			agg.distributeReceipts([]*notifications.ReceiptNotification{createReceiptNotification(txHash1), createReceiptNotification(txHash2)})
			var got []bool
			for _, r := range srv.sent {
				got = append(got, r.LastInBlock)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("LastInBlock per sent receipt = %v, want %v", got, tc.want)
			}
		})
	}
}
