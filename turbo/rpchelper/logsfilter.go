package rpchelper

import (
	"sync"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"

	types2 "github.com/ledgerwatch/erigon/core/types"
)

type LogsFilterAggregator struct {
	aggLogsFilter  LogsFilter                       // Aggregation of all current log filters
	logsFilters    *SyncMap[LogsSubID, *LogsFilter] // Filter for each subscriber, keyed by filterID
	logsFilterLock sync.RWMutex
	nextFilterId   LogsSubID
}

// LogsFilter is used for both representing log filter for a specific subscriber (RPC daemon usually)
// and "aggregated" log filter representing a union of all subscribers. Therefore, the values in
// the mappings are counters (of type int) and they get deleted when counter goes back to 0
// Also, addAddr and allTopic are int instead of bool because they are also counter, counting
// how many subscribers have this set on
type LogsFilter struct {
	allAddrs       int
	addrs          map[libcommon.Address]int
	allTopics      int
	topics         map[libcommon.Hash]int
	topicsOriginal [][]libcommon.Hash // Original topic filters to be applied before distributing to individual subscribers
	sender         Sub[*types2.Log]   // nil for aggregate subscriber, for appropriate stream server otherwise
}

func (l *LogsFilter) Send(lg *types2.Log) {
	l.sender.Send(lg)
}
func (l *LogsFilter) Close() {
	l.sender.Close()
}

func NewLogsFilterAggregator() *LogsFilterAggregator {
	return &LogsFilterAggregator{
		aggLogsFilter: LogsFilter{
			addrs:  make(map[libcommon.Address]int),
			topics: make(map[libcommon.Hash]int),
		},
		logsFilters:  NewSyncMap[LogsSubID, *LogsFilter](),
		nextFilterId: 0,
	}
}

func (a *LogsFilterAggregator) insertLogsFilter(sender Sub[*types2.Log]) (LogsSubID, *LogsFilter) {
	filterId := a.nextFilterId
	a.nextFilterId++
	filter := &LogsFilter{addrs: map[libcommon.Address]int{}, topics: map[libcommon.Hash]int{}, sender: sender}
	a.logsFilters.Put(filterId, filter)
	return filterId, filter
}

func (a *LogsFilterAggregator) removeLogsFilter(filterId LogsSubID) bool {
	filter, ok := a.logsFilters.Get(filterId)
	if !ok {
		return false
	}
	filter.Close()
	filter, ok = a.logsFilters.Delete(filterId)
	if !ok {
		return false
	}
	a.subtractLogFilters(filter)
	return true
}

func (a *LogsFilterAggregator) createFilterRequest() *remote.LogsFilterRequest {
	a.logsFilterLock.RLock()
	defer a.logsFilterLock.RUnlock()
	return &remote.LogsFilterRequest{
		AllAddresses: a.aggLogsFilter.allAddrs >= 1,
		AllTopics:    a.aggLogsFilter.allTopics >= 1,
	}
}

func (a *LogsFilterAggregator) subtractLogFilters(f *LogsFilter) {
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()
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
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()
	a.aggLogsFilter.allAddrs += f.allAddrs
	for addr, count := range f.addrs {
		a.aggLogsFilter.addrs[addr] += count
	}
	a.aggLogsFilter.allTopics += f.allTopics
	for topic, count := range f.topics {
		a.aggLogsFilter.topics[topic] += count
	}
}

func (a *LogsFilterAggregator) getAggMaps() (map[libcommon.Address]int, map[libcommon.Hash]int) {
	a.logsFilterLock.RLock()
	defer a.logsFilterLock.RUnlock()
	addresses := make(map[libcommon.Address]int)
	for k, v := range a.aggLogsFilter.addrs {
		addresses[k] = v
	}
	topics := make(map[libcommon.Hash]int)
	for k, v := range a.aggLogsFilter.topics {
		topics[k] = v
	}
	return addresses, topics
}

func (a *LogsFilterAggregator) distributeLog(eventLog *remote.SubscribeLogsReply) error {
	a.logsFilters.Range(func(k LogsSubID, filter *LogsFilter) error {
		if filter.allAddrs == 0 {
			_, addrOk := filter.addrs[gointerfaces.ConvertH160toAddress(eventLog.Address)]
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

func (a *LogsFilterAggregator) chooseTopics(filter *LogsFilter, logTopics []libcommon.Hash) bool {
	var found bool
	for _, logTopic := range logTopics {
		if _, ok := filter.topics[logTopic]; ok {
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
