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

package rpchelper

import (
	"context"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/rpc/filters"
)

func createLog() *remoteproto.SubscribeLogsReply {
	return &remoteproto.SubscribeLogsReply{
		Address:          gointerfaces.ConvertAddressToH160([20]byte{}),
		BlockHash:        gointerfaces.ConvertHashToH256([32]byte{}),
		BlockNumber:      0,
		Data:             []byte{},
		LogIndex:         0,
		Topics:           []*typesproto.H256{gointerfaces.ConvertHashToH256([32]byte{99, 99})},
		TransactionHash:  gointerfaces.ConvertHashToH256([32]byte{}),
		TransactionIndex: 0,
		Removed:          false,
	}
}

var (
	address1     = common.HexToAddress("0xdac17f958d2ee523a2206206994597c13d831ec7")
	address1H160 = gointerfaces.ConvertAddressToH160(address1)
	topic1       = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
	topic1H256   = gointerfaces.ConvertHashToH256(topic1)
)

func TestFilters_SingleSubscription_OnlyTopicsSubscribedAreBroadcast(t *testing.T) {
    t.Parallel()
    config := FiltersConfig{}

    f := New(t.Context(), config, nil, nil, nil, nil, func() {}, log.New())
    // ...
}

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
	f := New(t.Context(), config, nil, nil, nil, func() {}, log.New())

	subbedTopic := common.BytesToHash([]byte{10, 20})

	criteria := filters.FilterCriteria{
		Addresses: nil,
		Topics:    [][]common.Hash{{subbedTopic}},
	}

	outChan, _ := f.SubscribeLogs(10, criteria)

	// now create a log for some other topic and distribute it
	log := createLog()

	f.OnNewLogs(log)

	if len(outChan) != 0 {
		t.Error("expected the subscription channel to be empty")
	}

	// now a log that the subscription cares about
	log.Topics = []*typesproto.H256{gointerfaces.ConvertHashToH256(subbedTopic)}

	f.OnNewLogs(log)

	if len(outChan) != 1 {
		t.Error("expected a message in the channel for the subscribed topic")
	}
}

func TestFilters_SingleSubscription_EmptyTopicsInCriteria_OnlyTopicsSubscribedAreBroadcast(t *testing.T) {
	t.Parallel()
	config := FiltersConfig{}
	f := New(t.Context(), config, nil, nil, nil, func() {}, log.New())

	var nilTopic common.Hash
	subbedTopic := common.BytesToHash([]byte{10, 20})

	criteria := filters.FilterCriteria{
		Addresses: nil,
		Topics:    [][]common.Hash{{nilTopic, subbedTopic, nilTopic}},
	}

	outChan, _ := f.SubscribeLogs(10, criteria)

	// now create a log for some other topic and distribute it
	log := createLog()

	f.OnNewLogs(log)

	if len(outChan) != 0 {
		t.Error("expected the subscription channel to be empty")
	}

	// now a log that the subscription cares about
	log.Topics = []*typesproto.H256{gointerfaces.ConvertHashToH256(subbedTopic)}

	f.OnNewLogs(log)

	if len(outChan) != 1 {
		t.Error("expected a message in the channel for the subscribed topic")
	}
}

func TestFilters_TwoSubscriptionsWithDifferentCriteria(t *testing.T) {
	t.Parallel()
	config := FiltersConfig{}
	f := New(t.Context(), config, nil, nil, nil, func() {}, log.New())
	
	criteria1 := filters.FilterCriteria{
		Addresses: nil,
		Topics:    [][]common.Hash{},
	}
	criteria2 := filters.FilterCriteria{
		Addresses: nil,
		Topics:    [][]common.Hash{{topic1}},
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
	log.Topics = []*typesproto.H256{gointerfaces.ConvertHashToH256(topic1)}

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
	f := New(t.Context(), config, nil, nil, nil, func() {}, log.New())
	
	criteria1 := filters.FilterCriteria{
		Addresses: nil,
		Topics:    [][]common.Hash{},
	}
	criteria2 := filters.FilterCriteria{
		Addresses: nil,
		Topics:    [][]common.Hash{{topic1}},
	}
	criteria3 := filters.FilterCriteria{
		Addresses: []common.Address{common.HexToAddress(address1.String())},
		Topics:    [][]common.Hash{},
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
	var a common.Address
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
	log.Topics = []*typesproto.H256{topic1H256}
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
	var lastFilterRequest *remoteproto.LogsFilterRequest
	loadRequester := func(r *remoteproto.LogsFilterRequest) error {
		lastFilterRequest = r
		return nil
	}

	config := FiltersConfig{}
	f := New(context.TODO(), config, nil, nil, nil, func() {}, log.New())
	f.logsRequestor.Store(loadRequester)

	// first request has no filters
	criteria1 := filters.FilterCriteria{
		Addresses: []common.Address{},
		Topics:    [][]common.Hash{},
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
		Addresses: []common.Address{address1},
		Topics:    [][]common.Hash{},
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
		Addresses: []common.Address{},
		Topics:    [][]common.Hash{{topic1}},
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
	if !lastFilterRequest.AllAddresses {
		t.Error("5: expected all addresses to be true")
	}
	if lastFilterRequest.AllTopics {
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
	if lastFilterRequest.AllAddresses {
		t.Error("6: expected all addresses to be false")
	}
	if lastFilterRequest.AllTopics {
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
	tests := []struct {
		name        string
		maxLogs     int
		numToAdd    int
		expectedLen int
	}{
		{"WithinLimit", 5, 5, 5},
		{"ExceedingLimit", 2, 3, 2},
		{"UnlimitedLogs", 0, 10, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := FiltersConfig{RpcSubscriptionFiltersMaxLogs: tt.maxLogs}
			f := New(t.Context(), config, nil, nil, nil, func() {}, log.New())
			logID := LogsSubID("test-log")
			logEntry := &types.Log{Address: common.HexToAddress("095e7baea6a6c7c4c2dfeb977efac326af552d87")}

			for i := 0; i < tt.numToAdd; i++ {
				f.AddLogs(logID, logEntry)
			}

			logs, found := f.logsStores.Get(logID)
			if !found {
				t.Fatal("Expected to find logs in the store")
			}
			if len(logs) != tt.expectedLen {
				t.Fatalf("Expected %d logs, but got %d", tt.expectedLen, len(logs))
			}
		})
	}
}

func TestFilters_AddPendingBlocks(t *testing.T) {
	tests := []struct {
		name        string
		maxHeaders  int
		numToAdd    int
		expectedLen int
	}{
		{"WithinLimit", 3, 3, 3},
		{"ExceedingLimit", 2, 5, 2},
		{"UnlimitedHeaders", 0, 10, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := FiltersConfig{RpcSubscriptionFiltersMaxHeaders: tt.maxHeaders}
			f := New(t.Context(), config, nil, nil, nil, func() {}, log.New())
			blockID := HeadsSubID("test-block")
			header := &types.Header{}

			for i := 0; i < tt.numToAdd; i++ {
				f.AddPendingBlock(blockID, header)
			}

			blocks, found := f.pendingHeadsStores.Get(blockID)
			if !found {
				t.Fatal("Expected to find blocks in the store")
			}
			if len(blocks) != tt.expectedLen {
				t.Fatalf("Expected %d blocks, but got %d", tt.expectedLen, len(blocks))
			}
		})
	}
}

func TestFilters_AddPendingTxs(t *testing.T) {
	tests := []struct {
		name        string
		maxTxs      int
		numToAdd    int
		expectedLen int
	}{
		{"WithinLimit", 5, 5, 5},
		{"ExceedingLimit", 2, 6, 2},
		{"UnlimitedTxs", 0, 10, 10},
		{"TriggerPanic", 5, 10, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := FiltersConfig{RpcSubscriptionFiltersMaxTxs: tt.maxTxs}
			f := New(t.Context(), config, nil, nil, nil, func() {}, log.New())
			txID := PendingTxsSubID("test-tx")
			var txn types.Transaction = types.NewTransaction(0, common.HexToAddress("095e7baea6a6c7c4c2dfeb977efac326af552d87"), uint256.NewInt(10), 50000, uint256.NewInt(10), nil)
			txn, _ = txn.WithSignature(*types.LatestSignerForChainID(nil), common.Hex2Bytes("9bea4c4daac7c7c52e093e6a4c35dbbcf8856f1af7b059ba20253e70848d094f8a8fae537ce25ed8cb5af9adac3f141af69bd515bd2ba031522df09b97dd72b100"))

			// Testing for panic
			if tt.name == "TriggerPanic" {
				defer func() {
					if r := recover(); r != nil {
						t.Errorf("AddPendingTxs caused a panic: %v", r)
					}
				}()

				// Add transactions to trigger panic
				// Initial batch to set the stage
				for i := 0; i < 4; i++ {
					f.AddPendingTxs(txID, []types.Transaction{txn})
				}

				// Adding more transactions in smaller increments to ensure the panic
				for i := 0; i < 2; i++ {
					f.AddPendingTxs(txID, []types.Transaction{txn})
				}

				// Adding another large batch to ensure it exceeds the limit and triggers the panic
				largeBatch := make([]types.Transaction, 10)
				for i := range largeBatch {
					largeBatch[i] = txn
				}
				f.AddPendingTxs(txID, largeBatch)
			} else {
				for i := 0; i < tt.numToAdd; i++ {
					f.AddPendingTxs(txID, []types.Transaction{txn})
				}

				txs, found := f.ReadPendingTxs(txID)
				if !found {
					t.Fatal("Expected to find transactions in the store")
				}
				totalTxs := 0
				for _, batch := range txs {
					totalTxs += len(batch)
				}
				if totalTxs != tt.expectedLen {
					t.Fatalf("Expected %d transactions, but got %d", tt.expectedLen, totalTxs)
				}
			}
		})
	}
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

var (
	txHash1 = common.HexToHash("0xffc4978dfe7ab496f0158ae8916adae6ffd0c1fca4f09f7a7134556011357424")
	txHash2 = common.HexToHash("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	txHash3 = common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
)

func TestFilters_SingleReceiptsSubscription_OnlyTransactionHashesSubscribedAreBroadcast(t *testing.T) {
	t.Parallel()
	config := FiltersConfig{}
	f := New(t.Context(), config, nil, nil, nil, func() {}, log.New())

	criteria := filters.ReceiptsFilterCriteria{
		TransactionHashes: []common.Hash{txHash1},
	}

	outChan, _ := f.SubscribeReceipts(10, criteria)

	// Create a receipt for a different transaction hash
	receipt := createReceipt(txHash2)

	f.OnReceipts(receipt)

	if len(outChan) != 0 {
		t.Error("expected the subscription channel to be empty for non-matching txHash")
	}

	// Now a receipt that the subscription cares about
	receipt = createReceipt(txHash1)

	f.OnReceipts(receipt)

	if len(outChan) != 1 {
		t.Error("expected a message in the channel for the subscribed transaction hash")
	}
}

func TestFilters_ReceiptsSubscription_EmptyFilterSubscribesToAll(t *testing.T) {
	t.Parallel()
	config := FiltersConfig{}
	f := New(t.Context(), config, nil, nil, nil, func() {}, log.New())

	// Empty TransactionHashes means subscribe to all receipts
	criteria := filters.ReceiptsFilterCriteria{
		TransactionHashes: []common.Hash{},
	}

	outChan, _ := f.SubscribeReceipts(10, criteria)

	// Any receipt should be received
	receipt1 := createReceipt(txHash1)
	f.OnReceipts(receipt1)

	if len(outChan) != 1 {
		t.Error("expected empty filter to receive all receipts")
	}

	receipt2 := createReceipt(txHash2)
	f.OnReceipts(receipt2)

	if len(outChan) != 2 {
		t.Error("expected empty filter to receive all receipts")
	}
}

func TestFilters_TwoReceiptsSubscriptionsWithDifferentCriteria(t *testing.T) {
	t.Parallel()
	config := FiltersConfig{}
	f := New(t.Context(), config, nil, nil, nil, func() {}, log.New())

	// First subscription: all receipts
	criteria1 := filters.ReceiptsFilterCriteria{
		TransactionHashes: []common.Hash{},
	}
	// Second subscription: specific transaction hash
	criteria2 := filters.ReceiptsFilterCriteria{
		TransactionHashes: []common.Hash{txHash1},
	}

	chan1, _ := f.SubscribeReceipts(256, criteria1)
	chan2, _ := f.SubscribeReceipts(256, criteria2)

	// Create a receipt for txHash2
	receipt := createReceipt(txHash2)

	f.OnReceipts(receipt)

	if len(chan1) != 1 {
		t.Error("expected channel 1 to receive the receipt, no filters")
	}
	if len(chan2) != 0 {
		t.Error("expected channel 2 to be empty, it has a transaction hash filter")
	}

	// Now a receipt that the second subscription cares about
	receipt = createReceipt(txHash1)

	f.OnReceipts(receipt)

	if len(chan1) != 2 {
		t.Error("expected the second receipt to be in the channel with no filters")
	}
	if len(chan2) != 1 {
		t.Error("expected the channel with filters to receive the message as the filter matches")
	}
}

func TestFilters_ThreeReceiptsSubscriptionsWithDifferentCriteria(t *testing.T) {
	t.Parallel()
	config := FiltersConfig{}
	f := New(t.Context(), config, nil, nil, nil, func() {}, log.New())

	criteria1 := filters.ReceiptsFilterCriteria{
		TransactionHashes: []common.Hash{},
	}
	criteria2 := filters.ReceiptsFilterCriteria{
		TransactionHashes: []common.Hash{txHash1},
	}
	criteria3 := filters.ReceiptsFilterCriteria{
		TransactionHashes: []common.Hash{txHash1, txHash2},
	}

	chan1, _ := f.SubscribeReceipts(256, criteria1)
	chan2, _ := f.SubscribeReceipts(256, criteria2)
	chan3, _ := f.SubscribeReceipts(256, criteria3)

	// Receipt for txHash3 (not subscribed by chan2 or chan3)
	receipt := createReceipt(txHash3)

	f.OnReceipts(receipt)

	if len(chan1) != 1 {
		t.Error("expected channel 1 to receive the receipt, no filters")
	}
	if len(chan2) != 0 {
		t.Error("expected channel 2 to be empty, txHash doesn't match")
	}
	if len(chan3) != 0 {
		t.Error("expected channel 3 to be empty, txHash doesn't match")
	}

	// Receipt for txHash1 (subscribed by chan2 and chan3)
	receipt = createReceipt(txHash1)

	f.OnReceipts(receipt)

	if len(chan1) != 2 {
		t.Error("expected the second receipt to be in channel 1 with no filters")
	}
	if len(chan2) != 1 {
		t.Error("expected channel 2 to contain a receipt as txHash matched")
	}
	if len(chan3) != 1 {
		t.Error("expected channel 3 to contain a receipt as txHash matched")
	}

	// Receipt for txHash2 (subscribed by chan3 only)
	receipt = createReceipt(txHash2)

	f.OnReceipts(receipt)

	if len(chan1) != 3 {
		t.Error("expected the third receipt to be in channel 1 with no filters")
	}
	if len(chan2) != 1 {
		t.Error("expected channel 2 to still have 1 as txHash2 doesn't match")
	}
	if len(chan3) != 2 {
		t.Error("expected channel 3 to have 2 receipts as txHash2 matched")
	}
}

func TestFilters_SubscribeReceiptsGeneratesCorrectReceiptsFilterRequest(t *testing.T) {
	t.Parallel()
	var lastFilterRequest *remoteproto.ReceiptsFilterRequest
	loadRequester := func(r *remoteproto.ReceiptsFilterRequest) error {
		lastFilterRequest = r
		return nil
	}

	config := FiltersConfig{}
	f := New(t.Context(), config, nil, nil, nil, func() {}, log.New())
	f.receiptsRequestor.Store(loadRequester)

	// First request: subscribe to all receipts
	criteria1 := filters.ReceiptsFilterCriteria{
		TransactionHashes: []common.Hash{},
	}
	_, id1 := f.SubscribeReceipts(1, criteria1)

	// Request should have AllTransactions=true and empty TransactionHashes
	if !lastFilterRequest.AllTransactions {
		t.Error("1: expected AllTransactions to be true for subscribe-all")
	}
	if len(lastFilterRequest.TransactionHashes) != 0 {
		t.Error("1: expected transaction hashes to be empty for subscribe-all")
	}

	// Second request: filter on a specific transaction hash
	criteria2 := filters.ReceiptsFilterCriteria{
		TransactionHashes: []common.Hash{txHash1},
	}
	_, id2 := f.SubscribeReceipts(1, criteria2)

	// Request should have AllTransactions=true and include txHash1
	// Backend uses OR logic: send if (AllTransactions OR hash matches)
	if !lastFilterRequest.AllTransactions {
		t.Error("2: expected AllTransactions to be true")
	}
	if len(lastFilterRequest.TransactionHashes) != 1 {
		t.Errorf("2: expected 1 transaction hash, got %d", len(lastFilterRequest.TransactionHashes))
	}
	if gointerfaces.ConvertH256ToHash(lastFilterRequest.TransactionHashes[0]) != txHash1 {
		t.Error("2: expected transaction hash to match txHash1")
	}

	// Unsubscribe the first filter (subscribe-all)
	f.UnsubscribeReceipts(id1)

	// Now request should only have txHash1
	if len(lastFilterRequest.TransactionHashes) != 1 {
		t.Errorf("3: expected 1 transaction hash, got %d", len(lastFilterRequest.TransactionHashes))
	}
	if gointerfaces.ConvertH256ToHash(lastFilterRequest.TransactionHashes[0]) != txHash1 {
		t.Error("3: expected transaction hash to match txHash1")
	}

	// Third request: filter on multiple transaction hashes
	criteria3 := filters.ReceiptsFilterCriteria{
		TransactionHashes: []common.Hash{txHash2, txHash3},
	}
	_, id3 := f.SubscribeReceipts(1, criteria3)

	// Request should have all three transaction hashes
	if len(lastFilterRequest.TransactionHashes) != 3 {
		t.Errorf("4: expected 3 transaction hashes, got %d", len(lastFilterRequest.TransactionHashes))
	}

	// Unsubscribe the second filter
	f.UnsubscribeReceipts(id2)

	// Request should have only txHash2 and txHash3
	if len(lastFilterRequest.TransactionHashes) != 2 {
		t.Errorf("5: expected 2 transaction hashes, got %d", len(lastFilterRequest.TransactionHashes))
	}

	// Unsubscribe the last filter
	f.UnsubscribeReceipts(id3)

	// Request should be nil (no active subscriptions)
	if lastFilterRequest.TransactionHashes != nil {
		t.Error("6: expected transaction hashes to be nil with no subscriptions")
	}
}
