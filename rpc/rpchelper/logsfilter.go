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
	"sync"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/concurrent"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"

	types2 "github.com/erigontech/erigon/core/types"
)

type LogsFilterAggregator struct {
	aggLogsFilter  LogsFilter                                  // Aggregation of all current log filters
	logsFilters    *concurrent.SyncMap[LogsSubID, *LogsFilter] // Filter for each subscriber, keyed by filterID
	logsFilterLock sync.RWMutex
}

// LogsFilter is used for both representing log filter for a specific subscriber (RPC daemon usually)
// and "aggregated" log filter representing a union of all subscribers. Therefore, the values in
// the mappings are counters (of type int) and they get deleted when counter goes back to 0.
// Also, addAddr and allTopic are int instead of bool because they are also counters, counting
// how many subscribers have this set on.
type LogsFilter struct {
	allAddrs       int
	addrs          *concurrent.SyncMap[libcommon.Address, int]
	allTopics      int
	topics         *concurrent.SyncMap[libcommon.Hash, int]
	topicsOriginal [][]libcommon.Hash // Original topic filters to be applied before distributing to individual subscribers
	sender         Sub[*types2.Log]   // nil for aggregate subscriber, for appropriate stream server otherwise
}

// Send sends a log to the subscriber represented by the LogsFilter.
// It forwards the log to the subscriber's sender.
func (l *LogsFilter) Send(lg *types2.Log) {
	l.sender.Send(lg)
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
			addrs:  concurrent.NewSyncMap[libcommon.Address, int](),
			topics: concurrent.NewSyncMap[libcommon.Hash, int](),
		},
		logsFilters: concurrent.NewSyncMap[LogsSubID, *LogsFilter](),
	}
}

// insertLogsFilter inserts a new log filter into the LogsFilterAggregator with the specified sender.
// It generates a new filter ID, creates a new LogsFilter, and adds it to the logsFilters map.
func (a *LogsFilterAggregator) insertLogsFilter(sender Sub[*types2.Log]) (LogsSubID, *LogsFilter) {
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()
	filterId := LogsSubID(generateSubscriptionID())
	filter := &LogsFilter{
		addrs:  concurrent.NewSyncMap[libcommon.Address, int](),
		topics: concurrent.NewSyncMap[libcommon.Hash, int](),
		sender: sender,
	}
	a.logsFilters.Put(filterId, filter)
	return filterId, filter
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

// createFilterRequest creates a LogsFilterRequest from the current state of the LogsFilterAggregator.
// It generates a request that represents the union of all current log filters.
func (a *LogsFilterAggregator) createFilterRequest() *remote.LogsFilterRequest {
	a.logsFilterLock.RLock()
	defer a.logsFilterLock.RUnlock()
	return &remote.LogsFilterRequest{
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
	f.addrs.Range(func(addr libcommon.Address, count int) error {
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
	f.topics.Range(func(topic libcommon.Hash, count int) error {
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

// addLogsFilters adds the counts of addresses and topics in the given LogsFilter to the aggregated filter.
// It increments the counters for each address and topic in the aggregated filter by the corresponding counts in the
// provided LogsFilter.
func (a *LogsFilterAggregator) addLogsFilters(f *LogsFilter) {
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()
	a.aggLogsFilter.allAddrs += f.allAddrs
	if f.allAddrs > 0 {
		// Increment the count for AllAddresses
		activeSubscriptionsLogsAllAddressesGauge.Inc()
	}
	f.addrs.Range(func(addr libcommon.Address, count int) error {
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
	f.topics.Range(func(topic libcommon.Hash, count int) error {
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
func (a *LogsFilterAggregator) getAggMaps() (map[libcommon.Address]int, map[libcommon.Hash]int) {
	a.logsFilterLock.RLock()
	defer a.logsFilterLock.RUnlock()
	addresses := make(map[libcommon.Address]int)
	a.aggLogsFilter.addrs.Range(func(k libcommon.Address, v int) error {
		addresses[k] = v
		return nil
	})
	topics := make(map[libcommon.Hash]int)
	a.aggLogsFilter.topics.Range(func(k libcommon.Hash, v int) error {
		topics[k] = v
		return nil
	})
	return addresses, topics
}

// distributeLog processes an event log and distributes it to all subscribed log filters.
// It checks each filter to determine if the log should be sent based on the filter's address and topic settings.
func (a *LogsFilterAggregator) distributeLog(eventLog *remote.SubscribeLogsReply) error {
	a.logsFilterLock.RLock()
	defer a.logsFilterLock.RUnlock()

	var lg types2.Log
	var topics []libcommon.Hash

	a.logsFilters.Range(func(k LogsSubID, filter *LogsFilter) error {
		if filter.allAddrs == 0 {
			_, addrOk := filter.addrs.Get(gointerfaces.ConvertH160toAddress(eventLog.Address))
			if !addrOk {
				return nil
			}
		}

		// Pre-allocate topics slice to the required size to avoid multiple allocations
		topics = topics[:0]
		if cap(topics) < len(eventLog.Topics) {
			topics = make([]libcommon.Hash, 0, len(eventLog.Topics))
		}
		for _, topic := range eventLog.Topics {
			topics = append(topics, gointerfaces.ConvertH256ToHash(topic))
		}

		if filter.allTopics == 0 {
			if !a.chooseTopics(filter, topics) {
				return nil
			}
		}

		// Reuse lg object to avoid creating new instances
		lg.Address = gointerfaces.ConvertH160toAddress(eventLog.Address)
		lg.Topics = topics
		lg.Data = eventLog.Data
		lg.BlockNumber = eventLog.BlockNumber
		lg.TxHash = gointerfaces.ConvertH256ToHash(eventLog.TransactionHash)
		lg.TxIndex = uint(eventLog.TransactionIndex)
		lg.BlockHash = gointerfaces.ConvertH256ToHash(eventLog.BlockHash)
		lg.Index = uint(eventLog.LogIndex)
		lg.Removed = eventLog.Removed

		filter.sender.Send(&lg)
		return nil
	})
	return nil
}

// chooseTopics checks if the log topics match the filter's topics.
// It returns true if the log topics match the filter's topics, otherwise false.
func (a *LogsFilterAggregator) chooseTopics(filter *LogsFilter, logTopics []libcommon.Hash) bool {
	var found bool
	for _, logTopic := range logTopics {
		if _, ok := filter.topics.Get(logTopic); ok {
			found = true
			break
		}
	}
	if !found {
		return false
	}
	if len(filter.topicsOriginal) > len(logTopics) {
		return false
	}
	for i, sub := range filter.topicsOriginal {
		match := len(sub) == 0 // empty rule set == wildcard
		for _, topic := range sub {
			if logTopics[i] == topic {
				match = true
				break
			}
		}
		if !match {
			return false
		}
	}
	return true
}
