package privateapi

import (
	"fmt"
	"io"
	"sync"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon/common"
)

type LogsFilterAggregator struct {
	aggLogsFilter  LogsFilter             // Aggregation of all current log filters
	logsFilters    map[uint64]*LogsFilter // Filter for each subscriber, keyed by filterID
	logsFilterLock sync.Mutex
	nextFilterId   uint64
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
	sender    remote.ETHBACKEND_SubscribeLogsServer // nil for aggregate subscriber, for appropriate stream server otherwise
}

func NewLogsFilterAggregator() *LogsFilterAggregator {
	return &LogsFilterAggregator{
		aggLogsFilter: LogsFilter{
			addrs:  map[common.Address]int{},
			topics: map[common.Hash]int{},
		},
		logsFilters:  map[uint64]*LogsFilter{},
		nextFilterId: 0,
	}
}

func (a *LogsFilterAggregator) insertLogsFilter(sender remote.ETHBACKEND_SubscribeLogsServer) (uint64, *LogsFilter) {
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()
	filterId := a.nextFilterId
	a.nextFilterId++
	filter := &LogsFilter{addrs: map[common.Address]int{}, topics: map[common.Hash]int{}, sender: sender}
	a.logsFilters[filterId] = filter
	return filterId, filter
}

func (a *LogsFilterAggregator) removeLogsFilter(filterId uint64, filter *LogsFilter) {
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()
	a.subtractLogFilters(filter)
	delete(a.logsFilters, filterId)
}

func (a *LogsFilterAggregator) updateLogsFilter(filter *LogsFilter, filterReq *remote.LogsFilterRequest) {
	a.logsFilterLock.Lock()
	defer a.logsFilterLock.Unlock()
	a.subtractLogFilters(filter)
	filter.addrs = map[common.Address]int{}
	if filterReq.GetAllAddresses() {
		filter.allAddrs = 1
	} else {
		filter.allAddrs = 0
		for _, addr := range filterReq.GetAddresses() {
			filter.addrs[gointerfaces.ConvertH160toAddress(addr)] = 1
		}
	}
	filter.topics = map[common.Hash]int{}
	if filterReq.GetAllTopics() {
		filter.allTopics = 1
	} else {
		filter.allTopics = 0
		for _, topic := range filterReq.GetTopics() {
			filter.topics[gointerfaces.ConvertH256ToHash(topic)] = 1
		}
	}
	a.addLogsFilters(filter)
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
func (a *LogsFilterAggregator) SubscribeLogs(server remote.ETHBACKEND_SubscribeLogsServer) error {
	filterId, filter := a.insertLogsFilter(server)
	defer a.removeLogsFilter(filterId, filter)
	// Listen to filter updates and modify the filters, until terminated
	var filterReq *remote.LogsFilterRequest
	var recvErr error
	for filterReq, recvErr = server.Recv(); recvErr != nil; filterReq, recvErr = server.Recv() {
		a.updateLogsFilter(filter, filterReq)
	}
	if recvErr != nil && recvErr != io.EOF { // termination
		return fmt.Errorf("receiving log filter request: %w", recvErr)
	}
	return nil
}

func (a *LogsFilterAggregator) distributeLogs(logs []remote.SubscribeLogsReply) error {

	return nil
}
