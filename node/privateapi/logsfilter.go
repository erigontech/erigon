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
	"fmt"
	"io"
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/notifications"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/node/shards"
)

type LogsFilterAggregator struct {
	aggLogsFilter  LogsFilter             // Aggregation of all current log filters
	logsFilters    map[uint64]*LogsFilter // Filter for each subscriber, keyed by filterID
	logsFilterLock sync.Mutex
	nextFilterId   uint64
	events         *shards.Events
}

// LogsFilter is used for both representing log filter for a specific subscriber (RPC daemon usually)
// and "aggregated" log filter representing a union of all subscribers. Therefore, the values in
// the mappings are counters (of type int) and they get deleted when counter goes back to 0
// Also, addAddr and allTopic are int instead of bool because they are also counter, counting
// how many subscribers have this set on
type LogsFilter struct {
	allAddrs  int
	addrs     map[common.Address]int
	allTopics int
	topics    map[common.Hash]int
	sender    remoteproto.ETHBACKEND_SubscribeLogsServer // nil for aggregate subscriber, for appropriate stream server otherwise
}

func NewLogsFilterAggregator(events *shards.Events) *LogsFilterAggregator {
	return &LogsFilterAggregator{
		aggLogsFilter: LogsFilter{
			addrs:  make(map[common.Address]int),
			topics: make(map[common.Hash]int),
		},
		logsFilters:  make(map[uint64]*LogsFilter),
		nextFilterId: 0,
		events:       events,
	}
}

func (a *LogsFilterAggregator) insertLogsFilter(sender remoteproto.ETHBACKEND_SubscribeLogsServer) (uint64, *LogsFilter) {
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()
	filterId := a.nextFilterId
	a.nextFilterId++
	filter := &LogsFilter{addrs: make(map[common.Address]int), topics: make(map[common.Hash]int), sender: sender}
	a.logsFilters[filterId] = filter
	return filterId, filter
}

func (a *LogsFilterAggregator) checkEmpty() {
	a.events.EmptyLogSubscription(a.aggLogsFilter.allAddrs == 0 && len(a.aggLogsFilter.addrs) == 0 && a.aggLogsFilter.allTopics == 0 && len(a.aggLogsFilter.topics) == 0)
}

func (a *LogsFilterAggregator) removeLogsFilter(filterId uint64, filter *LogsFilter) {
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()
	a.subtractLogFilters(filter)
	delete(a.logsFilters, filterId)
	a.checkEmpty()
}

func (a *LogsFilterAggregator) updateLogsFilter(filter *LogsFilter, filterReq *remoteproto.LogsFilterRequest) {
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()
	a.subtractLogFilters(filter)
	filter.addrs = make(map[common.Address]int)
	if filterReq.GetAllAddresses() {
		filter.allAddrs = 1
	} else {
		filter.allAddrs = 0
		for _, addr := range filterReq.GetAddresses() {
			filter.addrs[gointerfaces.ConvertH160toAddress(addr)] = 1
		}
	}
	filter.topics = make(map[common.Hash]int)
	if filterReq.GetAllTopics() {
		filter.allTopics = 1
	} else {
		filter.allTopics = 0
		for _, topic := range filterReq.GetTopics() {
			filter.topics[gointerfaces.ConvertH256ToHash(topic)] = 1
		}
	}
	a.addLogsFilters(filter)
	a.checkEmpty()
}

func (a *LogsFilterAggregator) subtractLogFilters(f *LogsFilter) {
	a.aggLogsFilter.allAddrs -= f.allAddrs
	for addr, count := range f.addrs {
		a.aggLogsFilter.addrs[addr] -= count
		if a.aggLogsFilter.addrs[addr] == 0 {
			delete(a.aggLogsFilter.addrs, addr)
		}
	}
	a.aggLogsFilter.allTopics -= f.allTopics
	for topic, count := range f.topics {
		a.aggLogsFilter.topics[topic] -= count
		if a.aggLogsFilter.topics[topic] == 0 {
			delete(a.aggLogsFilter.topics, topic)
		}
	}
}

func (a *LogsFilterAggregator) addLogsFilters(f *LogsFilter) {
	a.aggLogsFilter.allAddrs += f.allAddrs
	for addr, count := range f.addrs {
		a.aggLogsFilter.addrs[addr] += count
	}
	a.aggLogsFilter.allTopics += f.allTopics
	for topic, count := range f.topics {
		a.aggLogsFilter.topics[topic] += count
	}
}

// SubscribeLogs
// Only one subscription is needed to serve all the users, LogsFilterRequest allows to dynamically modifying the subscription
func (a *LogsFilterAggregator) subscribeLogs(server remoteproto.ETHBACKEND_SubscribeLogsServer) error {
	filterId, filter := a.insertLogsFilter(server)
	defer a.removeLogsFilter(filterId, filter)
	// Listen to filter updates and modify the filters, until terminated
	var filterReq *remoteproto.LogsFilterRequest
	var recvErr error
	for filterReq, recvErr = server.Recv(); recvErr == nil; filterReq, recvErr = server.Recv() {
		a.updateLogsFilter(filter, filterReq)
	}
	if recvErr != io.EOF { // termination
		return fmt.Errorf("receiving log filter request: %w", recvErr)
	}
	return nil
}

// distributeLogs receives native log notifications, filters them, and converts
// to protobuf only when sending over gRPC.
func (a *LogsFilterAggregator) distributeLogs(logs []*notifications.LogNotification) error {
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()

	filtersToDelete := make(map[uint64]*LogsFilter)
outerLoop:
	for _, lg := range logs {
		// Use aggregate filter first — native types, no conversion needed
		if a.aggLogsFilter.allAddrs == 0 {
			if _, addrOk := a.aggLogsFilter.addrs[lg.Address]; !addrOk {
				continue
			}
		}
		if a.aggLogsFilter.allTopics == 0 {
			if !a.chooseTopicsNative(a.aggLogsFilter.topics, lg.Topics) {
				continue
			}
		}
		// Convert to protobuf once for all matching subscribers
		var proto *remoteproto.SubscribeLogsReply
		for filterId, filter := range a.logsFilters {
			if filter.allAddrs == 0 {
				if _, addrOk := filter.addrs[lg.Address]; !addrOk {
					continue
				}
			}
			if filter.allTopics == 0 {
				if !a.chooseTopicsNative(filter.topics, lg.Topics) {
					continue
				}
			}
			// Lazy convert: only build protobuf when we actually need to send
			if proto == nil {
				proto = logNotificationToProto(lg)
			}
			if err := filter.sender.Send(proto); err != nil {
				filtersToDelete[filterId] = filter
				continue outerLoop
			}
		}
	}
	// remove malfunctioned filters
	for filterId, filter := range filtersToDelete {
		a.subtractLogFilters(filter)
		delete(a.logsFilters, filterId)
	}

	return nil
}

func (a *LogsFilterAggregator) chooseTopicsNative(filterTopics map[common.Hash]int, logTopics []common.Hash) bool {
	for _, topic := range logTopics {
		if _, ok := filterTopics[topic]; ok {
			return true
		}
	}
	return false
}

// logNotificationToProto converts a native LogNotification to protobuf for gRPC.
func logNotificationToProto(lg *notifications.LogNotification) *remoteproto.SubscribeLogsReply {
	topics := make([]*typesproto.H256, 0, len(lg.Topics))
	for _, topic := range lg.Topics {
		topics = append(topics, gointerfaces.ConvertHashToH256(topic))
	}
	return &remoteproto.SubscribeLogsReply{
		Address:          gointerfaces.ConvertAddressToH160(lg.Address),
		BlockHash:        gointerfaces.ConvertHashToH256(lg.BlockHash),
		BlockNumber:      uint64(lg.BlockNumber),
		Data:             lg.Data,
		LogIndex:         uint64(lg.Index),
		Topics:           topics,
		TransactionHash:  gointerfaces.ConvertHashToH256(lg.TxHash),
		TransactionIndex: uint64(lg.TxIndex),
		Removed:          lg.Removed,
	}
}
