package rpchelper

import (
	"context"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"

	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/types"

	"github.com/ledgerwatch/erigon/eth/filters"
)

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

var (
	address1     = libcommon.HexToAddress("0xdac17f958d2ee523a2206206994597c13d831ec7")
	address1H160 = gointerfaces.ConvertAddressToH160(address1)
	topic1       = libcommon.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	topic1H256   = gointerfaces.ConvertHashToH256(topic1)
)

func TestFilters_GenerateSubscriptionID(t *testing.T) {
	sz := 1000
	subs := make(chan SubscriptionID, sz)
	for i := 0; i < sz; i++ {
		go func() {
			subs <- generateSubscriptionID()
		}()
	}
	set := map[SubscriptionID]struct{}{}
	for i := 0; i < sz; i++ {
		v := <-subs
		_, ok := set[v]
		if ok {
			t.Errorf("SubscriptionID Confict: %s", v)
			return
		}
		set[v] = struct{}{}
	}
}

func TestFilters_SingleSubscription_OnlyTopicsSubscribedAreBroadcast(t *testing.T) {
	f := New(context.TODO(), nil, nil, nil, func() {})

	subbedTopic := libcommon.BytesToHash([]byte{10, 20})

	criteria := filters.FilterCriteria{
		Addresses: nil,
		Topics:    [][]libcommon.Hash{{subbedTopic}},
	}

	outChan, _ := f.SubscribeLogs(10, criteria)

	// now create a log for some other topic and distribute it
	log := createLog()

	f.OnNewLogs(log)

	if len(outChan) != 0 {
		t.Error("expected the subscription channel to be empty")
	}

	// now a log that the subscription cares about
	log.Topics = []*types2.H256{gointerfaces.ConvertHashToH256(subbedTopic)}

	f.OnNewLogs(log)

	if len(outChan) != 1 {
		t.Error("expected a message in the channel for the subscribed topic")
	}
}

func TestFilters_SingleSubscription_EmptyTopicsInCriteria_OnlyTopicsSubscribedAreBroadcast(t *testing.T) {
	f := New(context.TODO(), nil, nil, nil, func() {})

	var nilTopic libcommon.Hash
	subbedTopic := libcommon.BytesToHash([]byte{10, 20})

	criteria := filters.FilterCriteria{
		Addresses: nil,
		Topics:    [][]libcommon.Hash{{nilTopic, subbedTopic, nilTopic}},
	}

	outChan, _ := f.SubscribeLogs(10, criteria)

	// now create a log for some other topic and distribute it
	log := createLog()

	f.OnNewLogs(log)

	if len(outChan) != 0 {
		t.Error("expected the subscription channel to be empty")
	}

	// now a log that the subscription cares about
	log.Topics = []*types2.H256{gointerfaces.ConvertHashToH256(subbedTopic)}

	f.OnNewLogs(log)

	if len(outChan) != 1 {
		t.Error("expected a message in the channel for the subscribed topic")
	}
}

func TestFilters_TwoSubscriptionsWithDifferentCriteria(t *testing.T) {
	f := New(context.TODO(), nil, nil, nil, func() {})

	criteria1 := filters.FilterCriteria{
		Addresses: nil,
		Topics:    [][]libcommon.Hash{},
	}
	criteria2 := filters.FilterCriteria{
		Addresses: nil,
		Topics:    [][]libcommon.Hash{{topic1}},
	}

	chan1, _ := f.SubscribeLogs(256, criteria1)
	chan2, _ := f.SubscribeLogs(256, criteria2)
	// now create a log for some other topic and distribute it
	log := createLog()

	f.OnNewLogs(log)

	if len(chan1) != 1 {
		t.Error("expected channel 1 to receive the log message, no filters")
	}
	if len(chan2) != 0 {
		t.Error("expected channel 2 to be empty, it has a topic filter")
	}

	// now a log that the subscription cares about
	log.Topics = []*types2.H256{gointerfaces.ConvertHashToH256(topic1)}

	f.OnNewLogs(log)

	if len(chan1) != 2 {
		t.Error("expected the second log to be in the channel with no filters")
	}
	if len(chan2) != 1 {
		t.Error("expected the channel with filters to receive the message as the filter matches")
	}
}

func TestFilters_ThreeSubscriptionsWithDifferentCriteria(t *testing.T) {
	f := New(context.TODO(), nil, nil, nil, func() {})

	criteria1 := filters.FilterCriteria{
		Addresses: nil,
		Topics:    [][]libcommon.Hash{},
	}
	criteria2 := filters.FilterCriteria{
		Addresses: nil,
		Topics:    [][]libcommon.Hash{{topic1}},
	}
	criteria3 := filters.FilterCriteria{
		Addresses: []libcommon.Address{libcommon.HexToAddress(address1.String())},
		Topics:    [][]libcommon.Hash{},
	}

	chan1, _ := f.SubscribeLogs(256, criteria1)
	chan2, _ := f.SubscribeLogs(256, criteria2)
	chan3, _ := f.SubscribeLogs(256, criteria3)

	// now create a log for some other topic and distribute it
	log := createLog()

	f.OnNewLogs(log)

	if len(chan1) != 1 {
		t.Error("expected channel 1 to receive the log message, no filters")
	}
	if len(chan2) != 0 {
		t.Error("expected channel 2 to be empty, it has a topic filter")
	}
	if len(chan3) != 0 {
		t.Error("expected channel 3 to be empty as the address doesn't match")
	}

	// now a log that the subscription cares about
	var a libcommon.Address
	a.SetBytes(address1.Bytes())
	log.Address = gointerfaces.ConvertAddressToH160(a)

	f.OnNewLogs(log)

	if len(chan1) != 2 {
		t.Error("expected the second log to be in the channel with no filters")
	}
	if len(chan2) != 0 {
		t.Error("expected the second channel to still be empty as no log has the correct topic yet")
	}
	if len(chan3) != 1 {
		t.Error("expected the third channel to have 1 entry as the previous log address matched")
	}

	log = createLog()
	log.Topics = []*types2.H256{topic1H256}
	f.OnNewLogs(log)

	if len(chan1) != 3 {
		t.Error("expected the third log to be in the channel with no filters")
	}
	if len(chan2) != 1 {
		t.Error("expected the third channel to contain a log as the topic matched")
	}
	if len(chan3) != 1 {
		t.Error("expected the third channel to still have 1 as the address didn't match in the third log")
	}

}

func TestFilters_SubscribeLogsGeneratesCorrectLogFilterRequest(t *testing.T) {
	var lastFilterRequest *remote.LogsFilterRequest
	loadRequester := func(r *remote.LogsFilterRequest) error {
		lastFilterRequest = r
		return nil
	}

	f := New(context.TODO(), nil, nil, nil, func() {})
	f.logsRequestor.Store(loadRequester)

	// first request has no filters
	criteria1 := filters.FilterCriteria{
		Addresses: []libcommon.Address{},
		Topics:    [][]libcommon.Hash{},
	}
	_, id1 := f.SubscribeLogs(1, criteria1)

	// request should have all addresses and topics enabled
	if lastFilterRequest.AllAddresses == false {
		t.Error("1: expected all addresses to be true")
	}
	if lastFilterRequest.AllTopics == false {
		t.Error("1: expected all topics to be true")
	}

	// second request filters on an address
	criteria2 := filters.FilterCriteria{
		Addresses: []libcommon.Address{address1},
		Topics:    [][]libcommon.Hash{},
	}
	_, id2 := f.SubscribeLogs(1, criteria2)

	// request should have all addresses and all topics still and the new address
	if lastFilterRequest.AllAddresses == false {
		t.Error("2: expected all addresses to be true")
	}
	if lastFilterRequest.AllTopics == false {
		t.Error("2: expected all topics to be true")
	}
	if len(lastFilterRequest.Addresses) != 1 && lastFilterRequest.Addresses[0] != address1H160 {
		t.Error("2: expected the address to match the last request")
	}

	// third request filters on topic
	criteria3 := filters.FilterCriteria{
		Addresses: []libcommon.Address{},
		Topics:    [][]libcommon.Hash{{topic1}},
	}
	_, id3 := f.SubscribeLogs(1, criteria3)

	// request should have all addresses and all topics as well as the previous address and new topic
	if lastFilterRequest.AllAddresses == false {
		t.Error("3: expected all addresses to be true")
	}
	if lastFilterRequest.AllTopics == false {
		t.Error("3: expected all topics to be true")
	}
	if len(lastFilterRequest.Addresses) != 1 && lastFilterRequest.Addresses[0] != address1H160 {
		t.Error("3: expected the address to match the previous request")
	}
	if len(lastFilterRequest.Topics) != 1 && lastFilterRequest.Topics[0] != topic1H256 {
		t.Error("3: expected the topics to match the last request")
	}

	// now start unsubscribing to check the state of things

	// unsubscribing the first filter should leave us with all topics and all addresses 2 because request 2 and 3
	// have empty addresses and topics between the two of them.  Effectively the state should be the same as the
	// subscription in step 3
	f.UnsubscribeLogs(id1)
	if lastFilterRequest.AllAddresses == false {
		t.Error("4: expected all addresses to be true")
	}
	if lastFilterRequest.AllTopics == false {
		t.Error("4: expected all topics to be true")
	}
	if len(lastFilterRequest.Addresses) != 1 && lastFilterRequest.Addresses[0] != address1H160 {
		t.Error("4: expected an address to be present")
	}
	if len(lastFilterRequest.Topics) != 1 && lastFilterRequest.Topics[0] != topic1H256 {
		t.Error("4: expected a topic to be present")
	}

	// unsubscribing the second filter should remove the all topics filter as the only subscription remaining
	// specifies a topic.  All addresses should be present still.  The state should represent the single
	// subscription in step 3
	f.UnsubscribeLogs(id2)
	if lastFilterRequest.AllAddresses == false {
		t.Error("5: expected all addresses to be true")
	}
	if lastFilterRequest.AllTopics == true {
		t.Error("5: expected all topics to be false")
	}
	if len(lastFilterRequest.Addresses) != 0 {
		t.Error("5: expected addresses to be empty")
	}
	if len(lastFilterRequest.Topics) != 1 && lastFilterRequest.Topics[0] != topic1H256 {
		t.Error("5: expected a topic to be present")
	}

	// unsubscribing the last filter should leave us with false for the all addresses and all topics
	// and nothing in the address or topics lists
	f.UnsubscribeLogs(id3)
	if lastFilterRequest.AllAddresses == true {
		t.Error("5: expected all addresses to be false")
	}
	if lastFilterRequest.AllTopics == true {
		t.Error("5: expected all topics to be false")
	}
	if len(lastFilterRequest.Addresses) != 0 {
		t.Error("5: expected addresses to be empty")
	}
	if len(lastFilterRequest.Topics) != 0 {
		t.Error("5: expected topics to be empty")
	}
}
