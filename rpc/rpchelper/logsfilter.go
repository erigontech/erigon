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
	"slices"
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/concurrent"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/rpc/filters"
)

type LogsFilterAggregator struct {
	aggLogsFilter  LogsFilter                                  // Aggregation of all current log filters
	logsFilters    *concurrent.SyncMap[LogsSubID, *LogsFilter] // Filter for each subscriber, keyed by filterID
	logsFilterLock sync.RWMutex
}

// LogsFilter represents one subscriber or the aggregate of all subscribers.
// Aggregate address and topic values are reference counts.
type LogsFilter struct {
	allAddrs        int
	addrs           *concurrent.SyncMap[common.Address, int]
	allTopics       int
	topics          *concurrent.SyncMap[common.Hash, int]
	topicsOriginal  [][]common.Hash // Original topic filters to be applied before distributing to individual subscribers
	pollingCriteria *filters.FilterCriteria
	sender          Sub[*types.RPCLog] // nil for aggregate subscriber, for appropriate stream server otherwise
}

// Close closes the sender associated with the LogsFilter.
// It is used to properly clean up and release resources associated with the sender.
func (l *LogsFilter) Close() {
	l.sender.Close()
}

// NewLogsFilterAggregator creates and returns a new instance of LogsFilterAggregator.
// It initializes the aggregated log filter and the map of individual log filters.
func NewLogsFilterAggregator() *LogsFilterAggregator {
	return &LogsFilterAggregator{
		aggLogsFilter: LogsFilter{
			addrs:  concurrent.NewSyncMap[common.Address, int](),
			topics: concurrent.NewSyncMap[common.Hash, int](),
		},
		logsFilters: concurrent.NewSyncMap[LogsSubID, *LogsFilter](),
	}
}

func newLogsFilter(sender Sub[*types.RPCLog], criteria filters.FilterCriteria, pollingCriteria *filters.FilterCriteria) *LogsFilter {
	filter := &LogsFilter{
		addrs:           concurrent.NewSyncMap[common.Address, int](),
		topics:          concurrent.NewSyncMap[common.Hash, int](),
		pollingCriteria: pollingCriteria,
		sender:          sender,
	}
	if len(criteria.Addresses) == 0 {
		filter.allAddrs = 1
	} else {
		for _, addr := range criteria.Addresses {
			filter.addrs.Put(addr, 1)
		}
	}
	if len(criteria.Topics) == 0 {
		filter.allTopics = 1
	} else {
		for _, topics := range criteria.Topics {
			for _, topic := range topics {
				filter.topics.Put(topic, 1)
			}
		}
		filter.topicsOriginal = criteria.Topics
	}
	return filter
}

func (a *LogsFilterAggregator) insertLogsFilter(filter *LogsFilter) LogsSubID {
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()
	filterId := LogsSubID(generateSubscriptionID())
	a.addLogsFilterLocked(filter)
	a.logsFilters.Put(filterId, filter)
	return filterId
}

func (a *LogsFilterAggregator) filterCriteria(filterId LogsSubID) (filters.FilterCriteria, bool) {
	a.logsFilterLock.RLock()
	defer a.logsFilterLock.RUnlock()
	filter, ok := a.logsFilters.Get(filterId)
	if !ok || filter.pollingCriteria == nil {
		return filters.FilterCriteria{}, false
	}
	return *filter.pollingCriteria, true
}

// removeLogsFilter removes a log filter identified by filterId from the LogsFilterAggregator.
// It closes the filter and subtracts its addresses and topics from the aggregated filter.
func (a *LogsFilterAggregator) removeLogsFilter(filterId LogsSubID) bool {
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()

	filter, ok := a.logsFilters.Get(filterId)
	if !ok {
		return false
	}
	filter.Close()
	_, ok = a.logsFilters.Delete(filterId)
	if !ok {
		return false
	}
	a.subtractLogFilters(filter)
	return true
}

// hasLogsFilter checks if a log filter identified by filterId is present in the LogsFilterAggregator.
func (a *LogsFilterAggregator) hasLogsFilter(filterId LogsSubID) bool {
	a.logsFilterLock.RLock()
	defer a.logsFilterLock.RUnlock()

	_, ok := a.logsFilters.Get(filterId)
	return ok
}

// createFilterRequest creates a LogsFilterRequest from the current state of the LogsFilterAggregator.
// It generates a request that represents the union of all current log filters.
func (a *LogsFilterAggregator) createFilterRequest() *remoteproto.LogsFilterRequest {
	a.logsFilterLock.RLock()
	defer a.logsFilterLock.RUnlock()
	return &remoteproto.LogsFilterRequest{
		AllAddresses: a.aggLogsFilter.allAddrs >= 1,
		AllTopics:    a.aggLogsFilter.allTopics >= 1,
	}
}

// subtractLogFilters subtracts the counts of addresses and topics in the given LogsFilter from the aggregated filter.
// It decrements the counters for each address and topic in the aggregated filter by the corresponding counts in the
// provided LogsFilter. If the count for any address or topic reaches zero, it is removed from the aggregated filter.
func (a *LogsFilterAggregator) subtractLogFilters(f *LogsFilter) {
	a.aggLogsFilter.allAddrs -= f.allAddrs
	if f.allAddrs > 0 {
		// Decrement the count for AllAddresses
		activeSubscriptionsLogsAllAddressesGauge.Dec()
	}
	f.addrs.Range(func(addr common.Address, count int) error {
		a.aggLogsFilter.addrs.Do(addr, func(value int, exists bool) (int, bool) {
			if exists {
				// Decrement the count for subscribed address
				activeSubscriptionsLogsAddressesGauge.Dec()
				newValue := value - count
				if newValue <= 0 {
					return 0, false
				}
				return newValue, true
			}
			return 0, false
		})
		return nil
	})
	a.aggLogsFilter.allTopics -= f.allTopics
	if f.allTopics > 0 {
		// Decrement the count for AllTopics
		activeSubscriptionsLogsAllTopicsGauge.Dec()
	}
	f.topics.Range(func(topic common.Hash, count int) error {
		a.aggLogsFilter.topics.Do(topic, func(value int, exists bool) (int, bool) {
			if exists {
				// Decrement the count for subscribed topic
				activeSubscriptionsLogsTopicsGauge.Dec()
				newValue := value - count
				if newValue <= 0 {
					return 0, false
				}
				return newValue, true
			}
			return 0, false
		})
		return nil
	})
}

func (a *LogsFilterAggregator) addLogsFilterLocked(f *LogsFilter) {
	a.aggLogsFilter.allAddrs += f.allAddrs
	if f.allAddrs > 0 {
		// Increment the count for AllAddresses
		activeSubscriptionsLogsAllAddressesGauge.Inc()
	}
	f.addrs.Range(func(addr common.Address, count int) error {
		// Increment the count for subscribed address
		activeSubscriptionsLogsAddressesGauge.Inc()
		a.aggLogsFilter.addrs.DoAndStore(addr, func(value int, exists bool) int {
			return value + count
		})
		return nil
	})
	a.aggLogsFilter.allTopics += f.allTopics
	if f.allTopics > 0 {
		// Increment the count for AllTopics
		activeSubscriptionsLogsAllTopicsGauge.Inc()
	}
	f.topics.Range(func(topic common.Hash, count int) error {
		// Increment the count for subscribed topic
		activeSubscriptionsLogsTopicsGauge.Inc()
		a.aggLogsFilter.topics.DoAndStore(topic, func(value int, exists bool) int {
			return value + count
		})
		return nil
	})
}

// getAggMaps returns the aggregated maps of addresses and topics from the LogsFilterAggregator.
// It creates copies of the current state of the aggregated addresses and topics filters.
func (a *LogsFilterAggregator) getAggMaps() (map[common.Address]int, map[common.Hash]int) {
	a.logsFilterLock.RLock()
	defer a.logsFilterLock.RUnlock()
	addresses := make(map[common.Address]int)
	a.aggLogsFilter.addrs.Range(func(k common.Address, v int) error {
		addresses[k] = v
		return nil
	})
	topics := make(map[common.Hash]int)
	a.aggLogsFilter.topics.Range(func(k common.Hash, v int) error {
		topics[k] = v
		return nil
	})
	return addresses, topics
}

// distributeLog processes an event log and distributes it to all subscribed log filters.
// It checks each filter to determine if the log should be sent based on the filter's address and topic settings.
func (a *LogsFilterAggregator) distributeLog(eventLog *remoteproto.SubscribeLogsReply) {
	addr := gointerfaces.ConvertH160toAddress(eventLog.Address)
	topics := make([]common.Hash, len(eventLog.Topics))
	for i, topic := range eventLog.Topics {
		topics[i] = gointerfaces.ConvertH256ToHash(topic)
	}
	// The same log instance is sent to every matching subscriber, each reading it from
	// its own goroutine, so it must not be mutated after the first Send.
	lg := &types.RPCLog{
		Log: types.Log{
			Address:     addr,
			Topics:      topics,
			Data:        eventLog.Data,
			BlockNumber: hexutil.Uint64(eventLog.BlockNumber),
			TxHash:      gointerfaces.ConvertH256ToHash(eventLog.TransactionHash),
			TxIndex:     hexutil.Uint(eventLog.TransactionIndex),
			BlockHash:   gointerfaces.ConvertH256ToHash(eventLog.BlockHash),
			Index:       hexutil.Uint(eventLog.LogIndex),
			Removed:     eventLog.Removed,
		},
		BlockTimestamp: hexutil.Uint64(eventLog.BlockTimestamp),
	}

	a.logsFilterLock.RLock()
	defer a.logsFilterLock.RUnlock()

	a.logsFilters.Range(func(k LogsSubID, filter *LogsFilter) error {
		if filter.allAddrs == 0 {
			if _, ok := filter.addrs.Get(addr); !ok {
				return nil
			}
		}
		if filter.allTopics == 0 && !a.chooseTopics(filter, topics) {
			return nil
		}
		filter.sender.Send(lg)
		return nil
	})
}

// chooseTopics checks if the log topics match the filter's topics.
// It returns true if the log topics match the filter's topics, otherwise false.
func (a *LogsFilterAggregator) chooseTopics(filter *LogsFilter, logTopics []common.Hash) bool {
	if len(filter.topicsOriginal) > len(logTopics) {
		return false
	}
	for i, sub := range filter.topicsOriginal {
		if len(sub) == 0 { // empty rule set == wildcard
			continue // Match any topic, so continue to next position
		}
		if !slices.Contains(sub, logTopics[i]) {
			return false
		}
	}
	return true
}
