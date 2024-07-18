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

	"github.com/erigontech/erigon/core/types"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"

	types2 "github.com/erigontech/erigon-lib/gointerfaces/typesproto"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/eth/filters"
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
			f := New(context.TODO(), config, nil, nil, nil, func() {}, log.New())
			logID := LogsSubID("test-log")
			logEntry := &types.Log{Address: libcommon.HexToAddress("095e7baea6a6c7c4c2dfeb977efac326af552d87")}

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
			f := New(context.TODO(), config, nil, nil, nil, func() {}, log.New())
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
			f := New(context.TODO(), config, nil, nil, nil, func() {}, log.New())
			txID := PendingTxsSubID("test-tx")
			var txn types.Transaction = types.NewTransaction(0, libcommon.HexToAddress("095e7baea6a6c7c4c2dfeb977efac326af552d87"), uint256.NewInt(10), 50000, uint256.NewInt(10), nil)
			txn, _ = txn.WithSignature(*types.LatestSignerForChainID(nil), libcommon.Hex2Bytes("9bea4c4daac7c7c52e093e6a4c35dbbcf8856f1af7b059ba20253e70848d094f8a8fae537ce25ed8cb5af9adac3f141af69bd515bd2ba031522df09b97dd72b100"))

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
