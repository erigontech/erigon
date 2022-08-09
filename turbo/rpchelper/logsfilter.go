package rpchelper

import (
	"sync"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"

	"github.com/ledgerwatch/erigon/common"
	types2 "github.com/ledgerwatch/erigon/core/types"
)

type LogsFilterAggregator struct {
	aggLogsFilter  LogsFilter                // Aggregation of all current log filters
	logsFilters    map[LogsSubID]*LogsFilter // Filter for each subscriber, keyed by filterID
	logsFilterLock sync.Mutex
	nextFilterId   LogsSubID
}

// LogsFilter is used for both representing log filter for a specific subscriber (RPC daemon usually)
// and "aggregated" log filter representing a union of all subscribers. Therefore, the values in
// the mappings are counters (of type int) and they get deleted when counter goes back to 0
// Also, addAddr and allTopic are int instead of bool because they are also counter, counting
// how many subscribers have this set on
type LogsFilter struct {
	allAddrs       int
	addrs          map[common.Address]int
	allTopics      int
	topics         map[common.Hash]int
	topicsOriginal [][]common.Hash  // Original topic filters to be applied before distributing to individual subscribers
	sender         chan *types2.Log // nil for aggregate subscriber, for appropriate stream server otherwise
}

func NewLogsFilterAggregator() *LogsFilterAggregator {
	return &LogsFilterAggregator{
		aggLogsFilter: LogsFilter{
			addrs:  make(map[common.Address]int),
			topics: make(map[common.Hash]int),
		},
		logsFilters:  make(map[LogsSubID]*LogsFilter),
		nextFilterId: 0,
	}
}

func (a *LogsFilterAggregator) insertLogsFilter(sender chan *types2.Log) (LogsSubID, *LogsFilter) {
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()
	filterId := a.nextFilterId
	a.nextFilterId++
	filter := &LogsFilter{addrs: map[common.Address]int{}, topics: map[common.Hash]int{}, sender: sender}
	a.logsFilters[filterId] = filter
	return filterId, filter
}

func (a *LogsFilterAggregator) removeLogsFilter(filterId LogsSubID) bool {
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()
	if filter, ok := a.logsFilters[filterId]; ok {
		a.subtractLogFilters(filter)
		close(filter.sender)
		delete(a.logsFilters, filterId)
		return true
	}
	return false
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

func (a *LogsFilterAggregator) distributeLog(eventLog *remote.SubscribeLogsReply) error {
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()
	for _, filter := range a.logsFilters {
		if filter.allAddrs == 0 {
			_, addrOk := filter.addrs[gointerfaces.ConvertH160toAddress(eventLog.Address)]
			if !addrOk {
				continue
			}
		}
		var topics []common.Hash
		for _, topic := range eventLog.Topics {
			topics = append(topics, gointerfaces.ConvertH256ToHash(topic))
		}
		if filter.allTopics == 0 {
			if !a.chooseTopics(filter, topics) {
				continue
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
		filter.sender <- lg
	}

	return nil
}

func (a *LogsFilterAggregator) chooseTopics(filter *LogsFilter, logTopics []common.Hash) bool {
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
