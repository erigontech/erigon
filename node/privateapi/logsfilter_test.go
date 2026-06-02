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
	"errors"
	"io"
	"math"
	"testing"

	"google.golang.org/grpc"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/notifications"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/node/shards"
)

var (
	address1   = common.HexToHash("0xdac17f958d2ee523a2206206994597c13d831ec7")
	topic1     = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	address160 *typesproto.H160
	topic1H256 *typesproto.H256
)

func init() {
	var a common.Address
	a.SetBytes(address1[:])
	address160 = gointerfaces.ConvertAddressToH160(a)
	topic1H256 = gointerfaces.ConvertHashToH256(topic1)
}

type testServer struct {
	received         chan *remoteproto.LogsFilterRequest
	receiveCompleted chan struct{}
	acks             int
	sent             []*remoteproto.SubscribeLogsReply
	sendErr          error
	sendCalls        int
	ctx              context.Context
	grpc.ServerStream
}

type failingTestServer struct {
	*testServer
}

func newTestServer(ctx context.Context) *testServer {
	ts := &testServer{
		received:         make(chan *remoteproto.LogsFilterRequest, 256),
		receiveCompleted: make(chan struct{}, 1),
		sent:             make([]*remoteproto.SubscribeLogsReply, 0),
		ctx:              ctx,
		ServerStream:     nil,
	}
	go func() {
		<-ts.ctx.Done()
		close(ts.received)
	}()
	return ts
}

func newFailingTestServer(ctx context.Context) *failingTestServer {
	return &failingTestServer{testServer: newTestServer(ctx)}
}

func (ts *testServer) Send(m *remoteproto.SubscribeLogsReply) error {
	ts.sendCalls++
	if m.GetBlockNumber() == math.MaxUint64 && m.GetLogIndex() == math.MaxUint64 {
		ts.acks++
		return nil
	}
	if ts.sendErr != nil {
		return ts.sendErr
	}
	ts.sent = append(ts.sent, m)

	return nil
}

func (ts *failingTestServer) Send(m *remoteproto.SubscribeLogsReply) error {
	if m.GetBlockNumber() == math.MaxUint64 && m.GetLogIndex() == math.MaxUint64 {
		ts.acks++
		return nil
	}
	return errors.New("send failed")
}

func (ts *testServer) Recv() (*remoteproto.LogsFilterRequest, error) {
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

func createLog() *notifications.LogNotification {
	return &notifications.LogNotification{
		Log: &types.Log{
			Address: common.Address{},
			Topics:  []common.Hash{{99, 99}},
			Data:    []byte{},
		},
		Removed: false,
	}
}

func TestLogsFilter_EmptyFilter_DoesNotDistributeAnything(t *testing.T) {
	events := shards.NewEvents()
	agg := NewLogsFilterAggregator(events)

	ctx := t.Context()
	srv := newTestServer(ctx)

	req1 := &remoteproto.LogsFilterRequest{
		AllAddresses: false,
		Addresses:    nil,
		AllTopics:    false,
		Topics:       nil,
	}
	srv.received <- req1

	go func() {
		err := agg.subscribeLogs(srv)
		if err != nil {
			t.Error(err)
		}
	}()

	<-srv.receiveCompleted

	// now see if a log would be sent or not
	lg := createLog()
	_ = agg.distributeLogs([]*notifications.LogNotification{lg})

	if len(srv.sent) != 0 {
		t.Error("expected the sent slice to be empty")
	}
}

func TestLogsFilter_AllAddressesAndTopicsFilter_DistributesLogRegardless(t *testing.T) {
	events := shards.NewEvents()
	agg := NewLogsFilterAggregator(events)

	ctx := t.Context()
	srv := newTestServer(ctx)

	req1 := &remoteproto.LogsFilterRequest{
		AllAddresses: true,
		Addresses:    nil,
		AllTopics:    true,
		Topics:       nil,
	}
	srv.received <- req1

	go func() {
		err := agg.subscribeLogs(srv)
		if err != nil {
			t.Error(err)
		}
	}()

	<-srv.receiveCompleted

	// now see if a log would be sent or not
	lg := createLog()
	_ = agg.distributeLogs([]*notifications.LogNotification{lg})
	if len(srv.sent) != 1 {
		t.Error("expected the sent slice to have the log present")
	}

	lg = createLog()
	lg.Topics = []common.Hash{topic1}
	_ = agg.distributeLogs([]*notifications.LogNotification{lg})
	if len(srv.sent) != 2 {
		t.Error("expected any topic to be allowed through the filter")
	}

	lg = createLog()
	var addr common.Address
	addr.SetBytes(address1[:])
	lg.Address = addr
	_ = agg.distributeLogs([]*notifications.LogNotification{lg})
	if len(srv.sent) != 3 {
		t.Error("expected any address to be allowed through the filter")
	}
}

func TestLogsFilter_TopicFilter_OnlyAllowsThatTopicThrough(t *testing.T) {
	events := shards.NewEvents()
	agg := NewLogsFilterAggregator(events)

	ctx := t.Context()
	srv := newTestServer(ctx)

	req1 := &remoteproto.LogsFilterRequest{
		AllAddresses: true, // need to allow all addresses on the request else it will filter on them
		Addresses:    nil,
		AllTopics:    false,
		Topics:       []*typesproto.H256{topic1H256},
	}
	srv.received <- req1

	go func() {
		err := agg.subscribeLogs(srv)
		if err != nil {
			t.Error(err)
		}
	}()

	<-srv.receiveCompleted

	// now see if a log would be sent or not
	lg := createLog()
	_ = agg.distributeLogs([]*notifications.LogNotification{lg})
	if len(srv.sent) != 0 {
		t.Error("the sent slice should be empty as the topic didn't match")
	}

	lg = createLog()
	lg.Topics = []common.Hash{topic1}
	_ = agg.distributeLogs([]*notifications.LogNotification{lg})
	if len(srv.sent) != 1 {
		t.Error("expected the log to be distributed as the topic matched")
	}
}

func TestLogsFilter_AddressFilter_OnlyAllowsThatAddressThrough(t *testing.T) {
	events := shards.NewEvents()
	agg := NewLogsFilterAggregator(events)

	ctx := t.Context()
	srv := newTestServer(ctx)

	req1 := &remoteproto.LogsFilterRequest{
		AllAddresses: false,
		Addresses:    []*typesproto.H160{address160},
		AllTopics:    true,
		Topics:       []*typesproto.H256{},
	}
	srv.received <- req1

	go func() {
		err := agg.subscribeLogs(srv)
		if err != nil {
			t.Error(err)
		}
	}()

	<-srv.receiveCompleted

	// now see if a log would be sent or not
	lg := createLog()
	_ = agg.distributeLogs([]*notifications.LogNotification{lg})
	if len(srv.sent) != 0 {
		t.Error("the sent slice should be empty as the address didn't match")
	}

	lg = createLog()
	var addr common.Address
	addr.SetBytes(address1[:])
	lg.Address = addr
	_ = agg.distributeLogs([]*notifications.LogNotification{lg})
	if len(srv.sent) != 1 {
		t.Error("expected the log to be distributed as the address matched")
	}
}

func TestLogsFilter_UpdateSignalsApplied(t *testing.T) {
	events := shards.NewEvents()
	agg := NewLogsFilterAggregator(events)

	ctx := t.Context()
	srv := newTestServer(ctx)
	srv.received <- &remoteproto.LogsFilterRequest{
		AllAddresses: true,
		AllTopics:    true,
	}

	go func() {
		err := agg.subscribeLogs(srv)
		if err != nil {
			t.Error(err)
		}
	}()

	<-srv.receiveCompleted

	if srv.acks != 1 {
		t.Fatalf("expected 1 logs filter applied ack, got %d", srv.acks)
	}
}

func TestLogsFilter_SendFailure_DoesNotSkipHealthySubscribers(t *testing.T) {
	events := shards.NewEvents()
	agg := NewLogsFilterAggregator(events)

	ctx := t.Context()

	const badSubscribers = 8
	badServers := make([]*failingTestServer, 0, badSubscribers)
	req := &remoteproto.LogsFilterRequest{
		AllAddresses: true,
		AllTopics:    true,
	}

	for range badSubscribers {
		srv := newFailingTestServer(ctx)
		srv.received <- req
		badServers = append(badServers, srv)
		go func(server *failingTestServer) {
			err := agg.subscribeLogs(server)
			if err != nil {
				t.Error(err)
			}
		}(srv)
	}

	healthySrv := newTestServer(ctx)
	healthySrv.received <- req
	go func() {
		err := agg.subscribeLogs(healthySrv)
		if err != nil {
			t.Error(err)
		}
	}()

	for _, srv := range badServers {
		<-srv.receiveCompleted
	}
	<-healthySrv.receiveCompleted

	const logsToSend = 32
	logs := make([]*notifications.LogNotification, 0, logsToSend)
	for i := range logsToSend {
		lg := createLog()
		lg.Index = hexutil.Uint(i)
		logs = append(logs, lg)
	}

	_ = agg.distributeLogs(logs)

	if got, want := len(healthySrv.sent), logsToSend; got != want {
		t.Fatalf("expected healthy subscriber to receive %d logs, got %d", want, got)
	}
	if got := len(agg.logsFilters); got != 1 {
		t.Fatalf("expected only healthy subscriber to remain, got %d", got)
	}
}

func TestLogsFilter_RemoveLogsFilter_IsIdempotent(t *testing.T) {
	events := shards.NewEvents()
	agg := NewLogsFilterAggregator(events)

	ctx := t.Context()
	brokenSrv := newTestServer(ctx)
	brokenSrv.sendErr = errors.New("send failed")
	healthySrv := newTestServer(ctx)

	brokenID, brokenFilter := agg.insertLogsFilter(brokenSrv)
	healthyID, healthyFilter := agg.insertLogsFilter(healthySrv)

	req := &remoteproto.LogsFilterRequest{AllAddresses: true, AllTopics: true}
	agg.updateLogsFilter(brokenFilter, req)
	agg.updateLogsFilter(healthyFilter, req)

	if err := agg.distributeLogs([]*notifications.LogNotification{createLog()}); err != nil {
		t.Fatalf("distributeLogs returned error: %v", err)
	}

	// Simulate deferred cleanup in subscribeLogs for a filter already removed in distributeLogs.
	agg.removeLogsFilter(brokenID, brokenFilter)

	if agg.aggLogsFilter.allAddrs != 1 {
		t.Fatalf("expected allAddrs to remain 1 after duplicate removal, got %d", agg.aggLogsFilter.allAddrs)
	}
	if agg.aggLogsFilter.allTopics != 1 {
		t.Fatalf("expected allTopics to remain 1 after duplicate removal, got %d", agg.aggLogsFilter.allTopics)
	}
	if _, ok := agg.logsFilters[healthyID]; !ok {
		t.Fatalf("expected healthy filter %d to remain", healthyID)
	}
}
