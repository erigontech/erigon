package privateapi

import (
	"fmt"
	"io"
	"sync"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"

	"github.com/ledgerwatch/erigon/turbo/shards"
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
	addrs     map[libcommon.Address]int
	allTopics int
	topics    map[libcommon.Hash]int
	sender    remote.ETHBACKEND_SubscribeLogsServer // nil for aggregate subscriber, for appropriate stream server otherwise
}

func NewLogsFilterAggregator(events *shards.Events) *LogsFilterAggregator {
	return &LogsFilterAggregator{
		aggLogsFilter: LogsFilter{
			addrs:  make(map[libcommon.Address]int),
			topics: make(map[libcommon.Hash]int),
		},
		logsFilters:  make(map[uint64]*LogsFilter),
		nextFilterId: 0,
		events:       events,
	}
}

func (a *LogsFilterAggregator) insertLogsFilter(sender remote.ETHBACKEND_SubscribeLogsServer) (uint64, *LogsFilter) {
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()
	filterId := a.nextFilterId
	a.nextFilterId++
	filter := &LogsFilter{addrs: make(map[libcommon.Address]int), topics: make(map[libcommon.Hash]int), sender: sender}
	a.logsFilters[filterId] = filter
	return filterId, filter
}

func (a *LogsFilterAggregator) checkEmpty() {
	a.events.EmptyLogSubsctiption(a.aggLogsFilter.allAddrs == 0 && len(a.aggLogsFilter.addrs) == 0 && a.aggLogsFilter.allTopics == 0 && len(a.aggLogsFilter.topics) == 0)
}

func (a *LogsFilterAggregator) removeLogsFilter(filterId uint64, filter *LogsFilter) {
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()
	a.subtractLogFilters(filter)
	delete(a.logsFilters, filterId)
	a.checkEmpty()
}

func (a *LogsFilterAggregator) updateLogsFilter(filter *LogsFilter, filterReq *remote.LogsFilterRequest) {
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()
	a.subtractLogFilters(filter)
	filter.addrs = make(map[libcommon.Address]int)
	if filterReq.GetAllAddresses() {
		filter.allAddrs = 1
	} else {
		filter.allAddrs = 0
		for _, addr := range filterReq.GetAddresses() {
			filter.addrs[gointerfaces.ConvertH160toAddress(addr)] = 1
		}
	}
	filter.topics = make(map[libcommon.Hash]int)
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
func (a *LogsFilterAggregator) subscribeLogs(server remote.ETHBACKEND_SubscribeLogsServer) error {
	filterId, filter := a.insertLogsFilter(server)
	defer a.removeLogsFilter(filterId, filter)
	// Listen to filter updates and modify the filters, until terminated
	var filterReq *remote.LogsFilterRequest
	var recvErr error
	for filterReq, recvErr = server.Recv(); recvErr == nil; filterReq, recvErr = server.Recv() {
		a.updateLogsFilter(filter, filterReq)
	}
	if recvErr != nil && recvErr != io.EOF { // termination
		return fmt.Errorf("receiving log filter request: %w", recvErr)
	}
	return nil
}

func (a *LogsFilterAggregator) distributeLogs(logs []*remote.SubscribeLogsReply) error {
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()

	filtersToDelete := make(map[uint64]*LogsFilter)
outerLoop:
	for _, log := range logs {
		// Use aggregate filter first
		if a.aggLogsFilter.allAddrs == 0 {
			if _, addrOk := a.aggLogsFilter.addrs[gointerfaces.ConvertH160toAddress(log.Address)]; !addrOk {
				continue
			}
		}
		if a.aggLogsFilter.allTopics == 0 {
			if !a.chooseTopics(a.aggLogsFilter.topics, log.GetTopics()) {
				continue
			}
		}
		for filterId, filter := range a.logsFilters {
			if filter.allAddrs == 0 {
				if _, addrOk := filter.addrs[gointerfaces.ConvertH160toAddress(log.Address)]; !addrOk {
					continue
				}
			}
			if filter.allTopics == 0 {
				if !a.chooseTopics(filter.topics, log.GetTopics()) {
					continue
				}
			}
			if err := filter.sender.Send(log); err != nil {
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

func (a *LogsFilterAggregator) chooseTopics(filterTopics map[libcommon.Hash]int, logTopics []*types.H256) bool {
	for _, logTopic := range logTopics {
		if _, ok := filterTopics[gointerfaces.ConvertH256ToHash(logTopic)]; ok {
			return true
		}
	}
	return false
}
