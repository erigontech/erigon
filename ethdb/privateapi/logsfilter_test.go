package privateapi

import (
	"context"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"google.golang.org/grpc"

	"github.com/ledgerwatch/erigon/turbo/shards"
)

var (
	address1   = libcommon.HexToHash("0xdac17f958d2ee523a2206206994597c13d831ec7")
	topic1     = libcommon.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	address160 *types2.H160
	topic1H256 *types2.H256
)

func init() {
	var a libcommon.Address
	a.SetBytes(address1.Bytes())
	address160 = gointerfaces.ConvertAddressToH160(a)
	topic1H256 = gointerfaces.ConvertHashToH256(topic1)
}

type testServer struct {
	received         chan *remote.LogsFilterRequest
	receiveCompleted chan struct{}
	sent             []*remote.SubscribeLogsReply
	ctx              context.Context
	grpc.ServerStream
}

func (ts *testServer) Send(m *remote.SubscribeLogsReply) error {
	ts.sent = append(ts.sent, m)
	return nil
}

func (ts *testServer) Recv() (*remote.LogsFilterRequest, error) {
	// notify complete when the last request has been processed
	defer func() {
		if len(ts.received) == 0 {
			ts.receiveCompleted <- struct{}{}
		}
	}()

	return <-ts.received, nil
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

func TestLogsFilter_EmptyFilter_DoesNotDistributeAnything(t *testing.T) {
	events := shards.NewEvents()
	agg := NewLogsFilterAggregator(events)

	srv := &testServer{
		received:         make(chan *remote.LogsFilterRequest, 256),
		receiveCompleted: make(chan struct{}, 1),
		sent:             make([]*remote.SubscribeLogsReply, 0),
		ctx:              context.Background(),
		ServerStream:     nil,
	}

	req1 := &remote.LogsFilterRequest{
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
	log := createLog()
	agg.distributeLogs([]*remote.SubscribeLogsReply{log})

	if len(srv.sent) != 0 {
		t.Error("expected the sent slice to be empty")
	}
}

func TestLogsFilter_AllAddressesAndTopicsFilter_DistributesLogRegardless(t *testing.T) {
	events := shards.NewEvents()
	agg := NewLogsFilterAggregator(events)

	srv := &testServer{
		received:         make(chan *remote.LogsFilterRequest, 256),
		receiveCompleted: make(chan struct{}, 1),
		sent:             make([]*remote.SubscribeLogsReply, 0),
		ctx:              context.Background(),
		ServerStream:     nil,
	}

	req1 := &remote.LogsFilterRequest{
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
	log := createLog()
	agg.distributeLogs([]*remote.SubscribeLogsReply{log})

	if len(srv.sent) != 1 {
		t.Error("expected the sent slice to have the log present")
	}

	log = createLog()
	log.Topics = []*types2.H256{topic1H256}
	agg.distributeLogs([]*remote.SubscribeLogsReply{log})
	if len(srv.sent) != 2 {
		t.Error("expected any topic to be allowed through the filter")
	}

	log = createLog()
	log.Address = address160
	agg.distributeLogs([]*remote.SubscribeLogsReply{log})
	if len(srv.sent) != 3 {
		t.Error("expected any address to be allowed through the filter")
	}
}

func TestLogsFilter_TopicFilter_OnlyAllowsThatTopicThrough(t *testing.T) {
	events := shards.NewEvents()
	agg := NewLogsFilterAggregator(events)

	srv := &testServer{
		received:         make(chan *remote.LogsFilterRequest, 256),
		receiveCompleted: make(chan struct{}, 1),
		sent:             make([]*remote.SubscribeLogsReply, 0),
		ctx:              context.Background(),
		ServerStream:     nil,
	}

	req1 := &remote.LogsFilterRequest{
		AllAddresses: true, // need to allow all addresses on the request else it will filter on them
		Addresses:    nil,
		AllTopics:    false,
		Topics:       []*types2.H256{topic1H256},
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
	log := createLog()
	agg.distributeLogs([]*remote.SubscribeLogsReply{log})

	if len(srv.sent) != 0 {
		t.Error("the sent slice should be empty as the topic didn't match")
	}

	log = createLog()
	log.Topics = []*types2.H256{topic1H256}
	agg.distributeLogs([]*remote.SubscribeLogsReply{log})
	if len(srv.sent) != 1 {
		t.Error("expected the log to be distributed as the topic matched")
	}
}

func TestLogsFilter_AddressFilter_OnlyAllowsThatAddressThrough(t *testing.T) {
	events := shards.NewEvents()
	agg := NewLogsFilterAggregator(events)

	srv := &testServer{
		received:         make(chan *remote.LogsFilterRequest, 256),
		receiveCompleted: make(chan struct{}, 1),
		sent:             make([]*remote.SubscribeLogsReply, 0),
		ctx:              context.Background(),
		ServerStream:     nil,
	}

	req1 := &remote.LogsFilterRequest{
		AllAddresses: false,
		Addresses:    []*types2.H160{address160},
		AllTopics:    true,
		Topics:       []*types2.H256{},
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
	log := createLog()
	agg.distributeLogs([]*remote.SubscribeLogsReply{log})

	if len(srv.sent) != 0 {
		t.Error("the sent slice should be empty as the address didn't match")
	}

	log = createLog()
	log.Address = address160
	agg.distributeLogs([]*remote.SubscribeLogsReply{log})
	if len(srv.sent) != 1 {
		t.Error("expected the log to be distributed as the address matched")
	}
}
