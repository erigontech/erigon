package rpchelper

import (
	"sync"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/concurrent"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	remote "github.com/ledgerwatch/erigon-lib/gointerfaces/remoteproto"

	types2 "github.com/ledgerwatch/erigon/core/types"
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
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()
	a.aggLogsFilter.allAddrs -= f.allAddrs
	f.addrs.Range(func(addr libcommon.Address, count int) error {
		a.aggLogsFilter.addrs.Do(addr, func(value int, exists bool) (int, bool) {
			if exists {
				newValue := value - count
				if newValue == 0 {
					return 0, false
				}
				return newValue, true
			}
			return 0, false
		})
		return nil
	})
	a.aggLogsFilter.allTopics -= f.allTopics
	f.topics.Range(func(topic libcommon.Hash, count int) error {
		a.aggLogsFilter.topics.Do(topic, func(value int, exists bool) (int, bool) {
			if exists {
				newValue := value - count
				if newValue == 0 {
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
	f.addrs.Range(func(addr libcommon.Address, count int) error {
		a.aggLogsFilter.addrs.DoAndStore(addr, func(value int, exists bool) int {
			return value + count
		})
		return nil
	})
	a.aggLogsFilter.allTopics += f.allTopics
	f.topics.Range(func(topic libcommon.Hash, count int) error {
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
	a.logsFilters.Range(func(k LogsSubID, filter *LogsFilter) error {
		if filter.allAddrs == 0 {
			_, addrOk := filter.addrs.Get(gointerfaces.ConvertH160toAddress(eventLog.Address))
			if !addrOk {
				return nil
			}
		}
		var topics []libcommon.Hash
		for _, topic := range eventLog.Topics {
			topics = append(topics, gointerfaces.ConvertH256ToHash(topic))
		}
		if filter.allTopics == 0 {
			if !a.chooseTopics(filter, topics) {
				return nil
			}
		}
		lg := &types2.Log{
			Address:     gointerfaces.ConvertH160toAddress(eventLog.Address),
			Topics:      topics,
			Data:        eventLog.Data,
			BlockNumber: eventLog.BlockNumber,
			TxHash:      gointerfaces.ConvertH256ToHash(eventLog.TransactionHash),
			TxIndex:     uint(eventLog.TransactionIndex),
			BlockHash:   gointerfaces.ConvertH256ToHash(eventLog.BlockHash),
			Index:       uint(eventLog.LogIndex),
			Removed:     eventLog.Removed,
		}
		filter.sender.Send(lg)
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
