package rpchelper

import (
	"context"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/core/types"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	remote "github.com/ledgerwatch/erigon-lib/gointerfaces/remoteproto"

	types2 "github.com/ledgerwatch/erigon-lib/gointerfaces/typesproto"

	"github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/log/v3"
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
	t.Parallel()
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
			t.Errorf("SubscriptionID Conflict: %s", v)
			return
		}
		set[v] = struct{}{}
	}
}

func TestFilters_SingleSubscription_OnlyTopicsSubscribedAreBroadcast(t *testing.T) {
	t.Parallel()
	config := FiltersConfig{}
	f := New(context.TODO(), config, nil, nil, nil, func() {}, log.New())

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
	t.Parallel()
	config := FiltersConfig{}
	f := New(context.TODO(), config, nil, nil, nil, func() {}, log.New())

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
	t.Parallel()
	config := FiltersConfig{}
	f := New(context.TODO(), config, nil, nil, nil, func() {}, log.New())

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
	t.Parallel()
	config := FiltersConfig{}
	f := New(context.TODO(), config, nil, nil, nil, func() {}, log.New())

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
	t.Parallel()
	var lastFilterRequest *remote.LogsFilterRequest
	loadRequester := func(r *remote.LogsFilterRequest) error {
		lastFilterRequest = r
		return nil
	}

	config := FiltersConfig{}
	f := New(context.TODO(), config, nil, nil, nil, func() {}, log.New())
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
	if len(lastFilterRequest.Addresses) != 1 && gointerfaces.ConvertH160toAddress(lastFilterRequest.Addresses[0]) != gointerfaces.ConvertH160toAddress(address1H160) {
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
	if len(lastFilterRequest.Addresses) != 1 && gointerfaces.ConvertH160toAddress(lastFilterRequest.Addresses[0]) != gointerfaces.ConvertH160toAddress(address1H160) {
		t.Error("3: expected the address to match the previous request")
	}
	if len(lastFilterRequest.Topics) != 1 && gointerfaces.ConvertH256ToHash(lastFilterRequest.Topics[0]) != gointerfaces.ConvertH256ToHash(topic1H256) {
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
	if len(lastFilterRequest.Addresses) != 1 && gointerfaces.ConvertH160toAddress(lastFilterRequest.Addresses[0]) != gointerfaces.ConvertH160toAddress(address1H160) {
		t.Error("4: expected an address to be present")
	}
	if len(lastFilterRequest.Topics) != 1 && gointerfaces.ConvertH256ToHash(lastFilterRequest.Topics[0]) != gointerfaces.ConvertH256ToHash(topic1H256) {
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
	if len(lastFilterRequest.Topics) != 1 && gointerfaces.ConvertH256ToHash(lastFilterRequest.Topics[0]) != gointerfaces.ConvertH256ToHash(topic1H256) {
		t.Error("5: expected a topic to be present")
	}

	// unsubscribing the last filter should leave us with false for the all addresses and all topics
	// and nothing in the address or topics lists
	f.UnsubscribeLogs(id3)
	if lastFilterRequest.AllAddresses == true {
		t.Error("6: expected all addresses to be false")
	}
	if lastFilterRequest.AllTopics == true {
		t.Error("6: expected all topics to be false")
	}
	if len(lastFilterRequest.Addresses) != 0 {
		t.Error("6: expected addresses to be empty")
	}
	if len(lastFilterRequest.Topics) != 0 {
		t.Error("6: expected topics to be empty")
	}
}

func TestFilters_AddLogs(t *testing.T) {
	config := FiltersConfig{RpcSubscriptionFiltersMaxLogs: 5}
	f := New(context.TODO(), config, nil, nil, nil, func() {}, log.New())
	logID := LogsSubID("test-log")
	logEntry := &types.Log{}

	// Add 10 logs to the store, but limit is 5
	for i := 0; i < 10; i++ {
		f.AddLogs(logID, logEntry)
	}

	logs, found := f.ReadLogs(logID)
	if !found {
		t.Error("expected to find logs in the store")
	}
	if len(logs) != 5 {
		t.Errorf("expected 5 logs in the store, got %d", len(logs))
	}
}

func TestFilters_AddLogs_Unlimited(t *testing.T) {
	config := FiltersConfig{RpcSubscriptionFiltersMaxLogs: 0}
	f := New(context.TODO(), config, nil, nil, nil, func() {}, log.New())
	logID := LogsSubID("test-log")
	logEntry := &types.Log{}

	// Add 10 logs to the store, limit is unlimited
	for i := 0; i < 10; i++ {
		f.AddLogs(logID, logEntry)
	}

	logs, found := f.ReadLogs(logID)
	if !found {
		t.Error("expected to find logs in the store")
	}
	if len(logs) != 10 {
		t.Errorf("expected 10 logs in the store, got %d", len(logs))
	}
}

func TestFilters_AddPendingBlocks(t *testing.T) {
	config := FiltersConfig{RpcSubscriptionFiltersMaxHeaders: 3}
	f := New(context.TODO(), config, nil, nil, nil, func() {}, log.New())
	headerID := HeadsSubID("test-header")
	header := &types.Header{}

	// Add 5 headers to the store, but limit is 3
	for i := 0; i < 5; i++ {
		f.AddPendingBlock(headerID, header)
	}

	headers, found := f.ReadPendingBlocks(headerID)
	if !found {
		t.Error("expected to find headers in the store")
	}
	if len(headers) != 3 {
		t.Errorf("expected 3 headers in the store, got %d", len(headers))
	}
}

func TestFilters_AddPendingBlocks_Unlimited(t *testing.T) {
	config := FiltersConfig{RpcSubscriptionFiltersMaxHeaders: 0}
	f := New(context.TODO(), config, nil, nil, nil, func() {}, log.New())
	headerID := HeadsSubID("test-header")
	header := &types.Header{}

	// Add 5 headers to the store, limit is unlimited
	for i := 0; i < 5; i++ {
		f.AddPendingBlock(headerID, header)
	}

	headers, found := f.ReadPendingBlocks(headerID)
	if !found {
		t.Error("expected to find headers in the store")
	}
	if len(headers) != 5 {
		t.Errorf("expected 5 headers in the store, got %d", len(headers))
	}
}

func TestFilters_AddPendingTxs(t *testing.T) {
	config := FiltersConfig{RpcSubscriptionFiltersMaxTxs: 4}
	f := New(context.TODO(), config, nil, nil, nil, func() {}, log.New())
	txID := PendingTxsSubID("test-tx")
	var tx types.Transaction = types.NewTransaction(0, libcommon.HexToAddress("095e7baea6a6c7c4c2dfeb977efac326af552d87"), uint256.NewInt(10), 50000, uint256.NewInt(10), nil)
	tx, _ = tx.WithSignature(*types.LatestSignerForChainID(nil), libcommon.Hex2Bytes("9bea4c4daac7c7c52e093e6a4c35dbbcf8856f1af7b059ba20253e70848d094f8a8fae537ce25ed8cb5af9adac3f141af69bd515bd2ba031522df09b97dd72b100"))

	// Add 6 txs to the store, but limit is 4
	for i := 0; i < 6; i++ {
		f.AddPendingTxs(txID, []types.Transaction{tx})
	}

	txs, found := f.ReadPendingTxs(txID)
	if !found {
		t.Error("expected to find txs in the store")
	}
	totalTxs := 0
	for _, batch := range txs {
		totalTxs += len(batch)
	}
	if totalTxs != 4 {
		t.Errorf("expected 4 txs in the store, got %d", totalTxs)
	}
}

func TestFilters_AddPendingTxs_Unlimited(t *testing.T) {
	config := FiltersConfig{RpcSubscriptionFiltersMaxTxs: 0}
	f := New(context.TODO(), config, nil, nil, nil, func() {}, log.New())
	txID := PendingTxsSubID("test-tx")
	var tx types.Transaction = types.NewTransaction(0, libcommon.HexToAddress("095e7baea6a6c7c4c2dfeb977efac326af552d87"), uint256.NewInt(10), 50000, uint256.NewInt(10), nil)
	tx, _ = tx.WithSignature(*types.LatestSignerForChainID(nil), libcommon.Hex2Bytes("9bea4c4daac7c7c52e093e6a4c35dbbcf8856f1af7b059ba20253e70848d094f8a8fae537ce25ed8cb5af9adac3f141af69bd515bd2ba031522df09b97dd72b100"))

	// Add 6 txs to the store, limit is unlimited
	for i := 0; i < 6; i++ {
		f.AddPendingTxs(txID, []types.Transaction{tx})
	}

	txs, found := f.ReadPendingTxs(txID)
	if !found {
		t.Error("expected to find txs in the store")
	}
	totalTxs := 0
	for _, batch := range txs {
		totalTxs += len(batch)
	}
	if totalTxs != 6 {
		t.Errorf("expected 6 txs in the store, got %d", totalTxs)
	}
}
