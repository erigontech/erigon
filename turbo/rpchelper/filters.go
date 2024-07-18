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
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/concurrent"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/grpcutil"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	txpool "github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon-lib/log/v3"
	txpool2 "github.com/erigontech/erigon-lib/txpool"

	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/filters"
	"github.com/erigontech/erigon/rlp"
)

// Filters holds the state for managing subscriptions to various Ethereum events.
// It allows for the subscription and management of events such as new blocks, pending transactions,
// logs, and other Ethereum-related activities.
type Filters struct {
	mu sync.RWMutex

	pendingBlock *types.Block

	headsSubs        *concurrent.SyncMap[HeadsSubID, Sub[*types.Header]]
	pendingLogsSubs  *concurrent.SyncMap[PendingLogsSubID, Sub[types.Logs]]
	pendingBlockSubs *concurrent.SyncMap[PendingBlockSubID, Sub[*types.Block]]
	pendingTxsSubs   *concurrent.SyncMap[PendingTxsSubID, Sub[[]types.Transaction]]
	logsSubs         *LogsFilterAggregator
	logsRequestor    atomic.Value
	onNewSnapshot    func()

	logsStores         *concurrent.SyncMap[LogsSubID, []*types.Log]
	pendingHeadsStores *concurrent.SyncMap[HeadsSubID, []*types.Header]
	pendingTxsStores   *concurrent.SyncMap[PendingTxsSubID, [][]types.Transaction]
	logger             log.Logger

	config FiltersConfig
}

// New creates a new Filters instance, initializes it, and starts subscription goroutines for Ethereum events.
// It requires a context, Ethereum backend, transaction pool client, mining client, snapshot callback function,
// and a logger for logging events.
func New(ctx context.Context, config FiltersConfig, ethBackend ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient, onNewSnapshot func(), logger log.Logger) *Filters {
	logger.Info("rpc filters: subscribing to Erigon events")

	ff := &Filters{
		headsSubs:          concurrent.NewSyncMap[HeadsSubID, Sub[*types.Header]](),
		pendingTxsSubs:     concurrent.NewSyncMap[PendingTxsSubID, Sub[[]types.Transaction]](),
		pendingLogsSubs:    concurrent.NewSyncMap[PendingLogsSubID, Sub[types.Logs]](),
		pendingBlockSubs:   concurrent.NewSyncMap[PendingBlockSubID, Sub[*types.Block]](),
		logsSubs:           NewLogsFilterAggregator(),
		onNewSnapshot:      onNewSnapshot,
		logsStores:         concurrent.NewSyncMap[LogsSubID, []*types.Log](),
		pendingHeadsStores: concurrent.NewSyncMap[HeadsSubID, []*types.Header](),
		pendingTxsStores:   concurrent.NewSyncMap[PendingTxsSubID, [][]types.Transaction](),
		logger:             logger,
		config:             config,
	}

	go func() {
		if ethBackend == nil {
			return
		}
		activeSubscriptionsLogsClientGauge.With(prometheus.Labels{clientLabelName: "ethBackend_Events"}).Inc()
		for {
			select {
			case <-ctx.Done():
				activeSubscriptionsLogsClientGauge.With(prometheus.Labels{clientLabelName: "ethBackend_Events"}).Dec()
				return
			default:
			}

			if err := ethBackend.Subscribe(ctx, ff.OnNewEvent); err != nil {
				select {
				case <-ctx.Done():
					activeSubscriptionsLogsClientGauge.With(prometheus.Labels{clientLabelName: "ethBackend_Events"}).Dec()
					return
				default:
				}
				if grpcutil.IsEndOfStream(err) || grpcutil.IsRetryLater(err) {
					time.Sleep(3 * time.Second)
					continue
				}
				logger.Warn("rpc filters: error subscribing to events", "err", err)
			}
		}
	}()

	go func() {
		if ethBackend == nil {
			return
		}
		activeSubscriptionsLogsClientGauge.With(prometheus.Labels{clientLabelName: "ethBackend_Logs"}).Inc()
		for {
			select {
			case <-ctx.Done():
				activeSubscriptionsLogsClientGauge.With(prometheus.Labels{clientLabelName: "ethBackend_Logs"}).Dec()
				return
			default:
			}
			if err := ethBackend.SubscribeLogs(ctx, ff.OnNewLogs, &ff.logsRequestor); err != nil {
				select {
				case <-ctx.Done():
					activeSubscriptionsLogsClientGauge.With(prometheus.Labels{clientLabelName: "ethBackend_Logs"}).Dec()
					return
				default:
				}
				if grpcutil.IsEndOfStream(err) || grpcutil.IsRetryLater(err) {
					time.Sleep(3 * time.Second)
					continue
				}
				logger.Warn("rpc filters: error subscribing to logs", "err", err)
			}
		}
	}()

	if txPool != nil {
		go func() {
			activeSubscriptionsLogsClientGauge.With(prometheus.Labels{clientLabelName: "txPool_PendingTxs"}).Inc()
			for {
				select {
				case <-ctx.Done():
					activeSubscriptionsLogsClientGauge.With(prometheus.Labels{clientLabelName: "txPool_PendingTxs"}).Dec()
					return
				default:
				}
				if err := ff.subscribeToPendingTransactions(ctx, txPool); err != nil {
					select {
					case <-ctx.Done():
						activeSubscriptionsLogsClientGauge.With(prometheus.Labels{clientLabelName: "txPool_PendingTxs"}).Dec()
						return
					default:
					}
					if grpcutil.IsEndOfStream(err) || grpcutil.IsRetryLater(err) || grpcutil.ErrIs(err, txpool2.ErrPoolDisabled) {
						time.Sleep(3 * time.Second)
						continue
					}
					logger.Warn("rpc filters: error subscribing to pending transactions", "err", err)
				}
			}
		}()

		if !reflect.ValueOf(mining).IsNil() { //https://groups.google.com/g/golang-nuts/c/wnH302gBa4I
			go func() {
				activeSubscriptionsLogsClientGauge.With(prometheus.Labels{clientLabelName: "txPool_PendingBlock"}).Inc()
				for {
					select {
					case <-ctx.Done():
						activeSubscriptionsLogsClientGauge.With(prometheus.Labels{clientLabelName: "txPool_PendingBlock"}).Dec()
						return
					default:
					}
					if err := ff.subscribeToPendingBlocks(ctx, mining); err != nil {
						select {
						case <-ctx.Done():
							activeSubscriptionsLogsClientGauge.With(prometheus.Labels{clientLabelName: "txPool_PendingBlock"}).Dec()
							return
						default:
						}
						if grpcutil.IsEndOfStream(err) || grpcutil.IsRetryLater(err) {
							time.Sleep(3 * time.Second)
							continue
						}
						logger.Warn("rpc filters: error subscribing to pending blocks", "err", err)
					}
				}
			}()
			go func() {
				activeSubscriptionsLogsClientGauge.With(prometheus.Labels{clientLabelName: "txPool_PendingLogs"}).Inc()
				for {
					select {
					case <-ctx.Done():
						activeSubscriptionsLogsClientGauge.With(prometheus.Labels{clientLabelName: "txPool_PendingLogs"}).Dec()
						return
					default:
					}
					if err := ff.subscribeToPendingLogs(ctx, mining); err != nil {
						select {
						case <-ctx.Done():
							activeSubscriptionsLogsClientGauge.With(prometheus.Labels{clientLabelName: "txPool_PendingLogs"}).Dec()
							return
						default:
						}
						if grpcutil.IsEndOfStream(err) || grpcutil.IsRetryLater(err) {
							time.Sleep(3 * time.Second)
							continue
						}
						logger.Warn("rpc filters: error subscribing to pending logs", "err", err)
					}
				}
			}()
		}
	}

	return ff
}

// LastPendingBlock returns the last pending block that was received.
func (ff *Filters) LastPendingBlock() *types.Block {
	ff.mu.RLock()
	defer ff.mu.RUnlock()
	return ff.pendingBlock
}

// subscribeToPendingTransactions subscribes to pending transactions using the given transaction pool client.
// It listens for new transactions and processes them as they arrive.
func (ff *Filters) subscribeToPendingTransactions(ctx context.Context, txPool txpool.TxpoolClient) error {
	subscription, err := txPool.OnAdd(ctx, &txpool.OnAddRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	for {
		event, err := subscription.Recv()
		if errors.Is(err, io.EOF) {
			ff.logger.Debug("rpcdaemon: the subscription to pending transactions channel was closed")
			break
		}
		if err != nil {
			return err
		}

		ff.OnNewTx(event)
	}
	return nil
}

// subscribeToPendingBlocks subscribes to pending blocks using the given mining client.
// It listens for new pending blocks and processes them as they arrive.
func (ff *Filters) subscribeToPendingBlocks(ctx context.Context, mining txpool.MiningClient) error {
	subscription, err := mining.OnPendingBlock(ctx, &txpool.OnPendingBlockRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		event, err := subscription.Recv()
		if errors.Is(err, io.EOF) {
			ff.logger.Debug("rpcdaemon: the subscription to pending blocks channel was closed")
			break
		}
		if err != nil {
			return err
		}

		ff.HandlePendingBlock(event)
	}
	return nil
}

// HandlePendingBlock handles a new pending block received from the mining client.
// It updates the internal state and notifies subscribers about the new block.
func (ff *Filters) HandlePendingBlock(reply *txpool.OnPendingBlockReply) {
	b := &types.Block{}
	if reply == nil || len(reply.RplBlock) == 0 {
		return
	}
	if err := rlp.Decode(bytes.NewReader(reply.RplBlock), b); err != nil {
		ff.logger.Warn("OnNewPendingBlock rpc filters, unprocessable payload", "err", err)
	}

	ff.mu.Lock()
	defer ff.mu.Unlock()
	ff.pendingBlock = b

	ff.pendingBlockSubs.Range(func(k PendingBlockSubID, v Sub[*types.Block]) error {
		v.Send(b)
		return nil
	})
}

// subscribeToPendingLogs subscribes to pending logs using the given mining client.
// It listens for new pending logs and processes them as they arrive.
func (ff *Filters) subscribeToPendingLogs(ctx context.Context, mining txpool.MiningClient) error {
	subscription, err := mining.OnPendingLogs(ctx, &txpool.OnPendingLogsRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		event, err := subscription.Recv()
		if errors.Is(err, io.EOF) {
			ff.logger.Debug("rpcdaemon: the subscription to pending logs channel was closed")
			break
		}
		if err != nil {
			return err
		}

		ff.HandlePendingLogs(event)
	}
	return nil
}

// HandlePendingLogs handles new pending logs received from the mining client.
// It updates the internal state and notifies subscribers about the new logs.
func (ff *Filters) HandlePendingLogs(reply *txpool.OnPendingLogsReply) {
	if len(reply.RplLogs) == 0 {
		return
	}
	l := []*types.Log{}
	if err := rlp.Decode(bytes.NewReader(reply.RplLogs), &l); err != nil {
		ff.logger.Warn("OnNewPendingLogs rpc filters, unprocessable payload", "err", err)
	}
	ff.pendingLogsSubs.Range(func(k PendingLogsSubID, v Sub[types.Logs]) error {
		v.Send(l)
		return nil
	})
}

// SubscribeNewHeads subscribes to new block headers and returns a channel to receive the headers
// and a subscription ID to manage the subscription.
func (ff *Filters) SubscribeNewHeads(size int) (<-chan *types.Header, HeadsSubID) {
	id := HeadsSubID(generateSubscriptionID())
	sub := newChanSub[*types.Header](size)
	ff.headsSubs.Put(id, sub)
	return sub.ch, id
}

// UnsubscribeHeads unsubscribes from new block headers using the given subscription ID.
// It returns true if the unsubscription was successful, otherwise false.
func (ff *Filters) UnsubscribeHeads(id HeadsSubID) bool {
	ch, ok := ff.headsSubs.Get(id)
	if !ok {
		return false
	}
	ch.Close()
	if _, ok = ff.headsSubs.Delete(id); !ok {
		return false
	}
	ff.pendingHeadsStores.Delete(id)
	return true
}

// SubscribePendingLogs subscribes to pending logs and returns a channel to receive the logs
// and a subscription ID to manage the subscription. It uses the specified filter criteria.
func (ff *Filters) SubscribePendingLogs(size int) (<-chan types.Logs, PendingLogsSubID) {
	id := PendingLogsSubID(generateSubscriptionID())
	sub := newChanSub[types.Logs](size)
	ff.pendingLogsSubs.Put(id, sub)
	return sub.ch, id
}

// UnsubscribePendingLogs unsubscribes from pending logs using the given subscription ID.
func (ff *Filters) UnsubscribePendingLogs(id PendingLogsSubID) {
	ch, ok := ff.pendingLogsSubs.Get(id)
	if !ok {
		return
	}
	ch.Close()
	ff.pendingLogsSubs.Delete(id)
}

// SubscribePendingBlock subscribes to pending blocks and returns a channel to receive the blocks
// and a subscription ID to manage the subscription.
func (ff *Filters) SubscribePendingBlock(size int) (<-chan *types.Block, PendingBlockSubID) {
	id := PendingBlockSubID(generateSubscriptionID())
	sub := newChanSub[*types.Block](size)
	ff.pendingBlockSubs.Put(id, sub)
	return sub.ch, id
}

// UnsubscribePendingBlock unsubscribes from pending blocks using the given subscription ID.
func (ff *Filters) UnsubscribePendingBlock(id PendingBlockSubID) {
	ch, ok := ff.pendingBlockSubs.Get(id)
	if !ok {
		return
	}
	ch.Close()
	ff.pendingBlockSubs.Delete(id)
}

// SubscribePendingTxs subscribes to pending transactions and returns a channel to receive the transactions
// and a subscription ID to manage the subscription.
func (ff *Filters) SubscribePendingTxs(size int) (<-chan []types.Transaction, PendingTxsSubID) {
	id := PendingTxsSubID(generateSubscriptionID())
	sub := newChanSub[[]types.Transaction](size)
	ff.pendingTxsSubs.Put(id, sub)
	return sub.ch, id
}

// UnsubscribePendingTxs unsubscribes from pending transactions using the given subscription ID.
// It returns true if the unsubscription was successful, otherwise false.
func (ff *Filters) UnsubscribePendingTxs(id PendingTxsSubID) bool {
	ch, ok := ff.pendingTxsSubs.Get(id)
	if !ok {
		return false
	}
	ch.Close()
	if _, ok = ff.pendingTxsSubs.Delete(id); !ok {
		return false
	}
	ff.pendingTxsStores.Delete(id)
	return true
}

// SubscribeLogs subscribes to logs using the specified filter criteria and returns a channel to receive the logs
// and a subscription ID to manage the subscription.
func (ff *Filters) SubscribeLogs(size int, criteria filters.FilterCriteria) (<-chan *types.Log, LogsSubID) {
	sub := newChanSub[*types.Log](size)
	id, f := ff.logsSubs.insertLogsFilter(sub)

	// Initialize address and topic maps
	f.addrs = concurrent.NewSyncMap[libcommon.Address, int]()
	f.topics = concurrent.NewSyncMap[libcommon.Hash, int]()

	// Handle addresses
	if len(criteria.Addresses) == 0 {
		// If no addresses are specified, it means all addresses should be included
		f.allAddrs = 1
	} else {
		// Limit the number of addresses
		addressCount := 0
		for _, addr := range criteria.Addresses {
			if ff.config.RpcSubscriptionFiltersMaxAddresses == 0 || addressCount < ff.config.RpcSubscriptionFiltersMaxAddresses {
				f.addrs.Put(addr, 1)
				addressCount++
			} else {
				break
			}
		}
	}

	// Handle topics and track the allowed topics
	if len(criteria.Topics) == 0 {
		// If no topics are specified, it means all topics should be included
		f.allTopics = 1
	} else {
		// Limit the number of topics
		topicCount := 0
		allowedTopics := [][]libcommon.Hash{}
		for _, topics := range criteria.Topics {
			allowedTopicsRow := []libcommon.Hash{}
			for _, topic := range topics {
				if ff.config.RpcSubscriptionFiltersMaxTopics == 0 || topicCount < ff.config.RpcSubscriptionFiltersMaxTopics {
					f.topics.Put(topic, 1)
					allowedTopicsRow = append(allowedTopicsRow, topic)
					topicCount++
				} else {
					break
				}
			}
			if len(allowedTopicsRow) > 0 {
				allowedTopics = append(allowedTopics, allowedTopicsRow)
			}
		}
		f.topicsOriginal = allowedTopics
	}

	// Add the filter to the list of log filters
	ff.logsSubs.addLogsFilters(f)

	// Create a filter request based on the aggregated filters
	lfr := ff.logsSubs.createFilterRequest()
	addresses, topics := ff.logsSubs.getAggMaps()
	for addr := range addresses {
		lfr.Addresses = append(lfr.Addresses, gointerfaces.ConvertAddressToH160(addr))
	}
	for topic := range topics {
		lfr.Topics = append(lfr.Topics, gointerfaces.ConvertHashToH256(topic))
	}

	loaded := ff.loadLogsRequester()
	if loaded != nil {
		if err := loaded.(func(*remote.LogsFilterRequest) error)(lfr); err != nil {
			ff.logger.Warn("Could not update remote logs filter", "err", err)
			ff.logsSubs.removeLogsFilter(id)
		}
	}

	return sub.ch, id
}

// loadLogsRequester loads the current logs requester and returns it.
func (ff *Filters) loadLogsRequester() any {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	return ff.logsRequestor.Load()
}

// UnsubscribeLogs unsubscribes from logs using the given subscription ID.
// It returns true if the unsubscription was successful, otherwise false.
func (ff *Filters) UnsubscribeLogs(id LogsSubID) bool {
	isDeleted := ff.logsSubs.removeLogsFilter(id)
	// if any filters in the aggregate need all addresses or all topics then the request to the central
	// log subscription needs to honour this
	lfr := ff.logsSubs.createFilterRequest()

	addresses, topics := ff.logsSubs.getAggMaps()

	for addr := range addresses {
		lfr.Addresses = append(lfr.Addresses, gointerfaces.ConvertAddressToH160(addr))
	}
	for topic := range topics {
		lfr.Topics = append(lfr.Topics, gointerfaces.ConvertHashToH256(topic))
	}
	loaded := ff.loadLogsRequester()
	if loaded != nil {
		if err := loaded.(func(*remote.LogsFilterRequest) error)(lfr); err != nil {
			ff.logger.Warn("Could not update remote logs filter", "err", err)
			return isDeleted || ff.logsSubs.removeLogsFilter(id)
		}
	}

	ff.deleteLogStore(id)

	return isDeleted
}

// deleteLogStore deletes the log store associated with the given subscription ID.
func (ff *Filters) deleteLogStore(id LogsSubID) {
	ff.logsStores.Delete(id)
}

// OnNewEvent is called when there is a new event from the remote and processes it.
func (ff *Filters) OnNewEvent(event *remote.SubscribeReply) {
	err := ff.onNewEvent(event)
	if err != nil {
		ff.logger.Warn("OnNewEvent Filters", "event", event.Type, "err", err)
	}
}

// onNewEvent processes the given event from the remote and updates the internal state.
func (ff *Filters) onNewEvent(event *remote.SubscribeReply) error {
	switch event.Type {
	case remote.Event_HEADER:
		return ff.onNewHeader(event)
	case remote.Event_NEW_SNAPSHOT:
		ff.onNewSnapshot()
		return nil
	case remote.Event_PENDING_LOGS:
		return ff.onPendingLog(event)
	case remote.Event_PENDING_BLOCK:
		return ff.onPendingBlock(event)
	default:
		return fmt.Errorf("unsupported event type")
	}
}

// TODO: implement?
// onPendingLog handles a new pending log event from the remote.
func (ff *Filters) onPendingLog(event *remote.SubscribeReply) error {
	//	payload := event.Data
	//	var logs types.Logs
	//	err := rlp.Decode(bytes.NewReader(payload), &logs)
	//	if err != nil {
	//		// ignoring what we can't unmarshal
	//		log.Warn("OnNewEvent rpc filters (pending logs), unprocessable payload", "err", err)
	//	} else {
	//		for _, v := range ff.pendingLogsSubs {
	//			v <- logs
	//		}
	//	}
	return nil
}

// TODO: implement?
// onPendingBlock handles a new pending block event from the remote.
func (ff *Filters) onPendingBlock(event *remote.SubscribeReply) error {
	//	payload := event.Data
	//	var block types.Block
	//	err := rlp.Decode(bytes.NewReader(payload), &block)
	//	if err != nil {
	//		// ignoring what we can't unmarshal
	//		log.Warn("OnNewEvent rpc filters (pending txs), unprocessable payload", "err", err)
	//	} else {
	//		for _, v := range ff.pendingBlockSubs {
	//			v <- &block
	//		}
	//	}
	return nil
}

// onNewHeader handles a new block header event from the remote and updates the internal state.
func (ff *Filters) onNewHeader(event *remote.SubscribeReply) error {
	payload := event.Data
	var header types.Header
	if len(payload) == 0 {
		return nil
	}
	err := rlp.Decode(bytes.NewReader(payload), &header)
	if err != nil {
		return fmt.Errorf("unprocessable payload: %w", err)
	}
	return ff.headsSubs.Range(func(k HeadsSubID, v Sub[*types.Header]) error {
		v.Send(&header)
		return nil
	})
}

// OnNewTx handles a new transaction event from the transaction pool and processes it.
func (ff *Filters) OnNewTx(reply *txpool.OnAddReply) {
	txs := make([]types.Transaction, len(reply.RplTxs))
	for i, rlpTx := range reply.RplTxs {
		var decodeErr error
		if len(rlpTx) == 0 {
			continue
		}
		txs[i], decodeErr = types.DecodeTransaction(rlpTx)
		if decodeErr != nil {
			// ignoring what we can't unmarshal
			ff.logger.Warn("OnNewTx rpc filters, unprocessable payload", "err", decodeErr, "data", hex.EncodeToString(rlpTx))
			break
		}
	}
	ff.pendingTxsSubs.Range(func(k PendingTxsSubID, v Sub[[]types.Transaction]) error {
		v.Send(txs)
		return nil
	})
}

// OnNewLogs handles a new log event from the remote and processes it.
func (ff *Filters) OnNewLogs(reply *remote.SubscribeLogsReply) {
	ff.logsSubs.distributeLog(reply)
}

// AddLogs adds logs to the store associated with the given subscription ID.
func (ff *Filters) AddLogs(id LogsSubID, log *types.Log) {
	ff.logsStores.DoAndStore(id, func(st []*types.Log, ok bool) []*types.Log {
		if !ok {
			st = make([]*types.Log, 0)
		}

		maxLogs := ff.config.RpcSubscriptionFiltersMaxLogs
		if maxLogs > 0 && len(st)+1 > maxLogs {
			// Calculate the number of logs to remove
			excessLogs := len(st) + 1 - maxLogs
			if excessLogs > 0 {
				if excessLogs >= len(st) {
					// If excessLogs is greater than or equal to the length of st, remove all
					st = []*types.Log{}
				} else {
					// Otherwise, remove the oldest logs
					st = st[excessLogs:]
				}
			}
		}

		// Append the new log
		st = append(st, log)
		return st
	})
}

// ReadLogs reads logs from the store associated with the given subscription ID.
// It returns the logs and a boolean indicating whether the logs were found.
func (ff *Filters) ReadLogs(id LogsSubID) ([]*types.Log, bool) {
	res, ok := ff.logsStores.Delete(id)
	if !ok {
		return res, false
	}
	return res, true
}

// AddPendingBlock adds a pending block header to the store associated with the given subscription ID.
func (ff *Filters) AddPendingBlock(id HeadsSubID, block *types.Header) {
	ff.pendingHeadsStores.DoAndStore(id, func(st []*types.Header, ok bool) []*types.Header {
		if !ok {
			st = make([]*types.Header, 0)
		}

		maxHeaders := ff.config.RpcSubscriptionFiltersMaxHeaders
		if maxHeaders > 0 && len(st)+1 > maxHeaders {
			// Calculate the number of headers to remove
			excessHeaders := len(st) + 1 - maxHeaders
			if excessHeaders > 0 {
				if excessHeaders >= len(st) {
					// If excessHeaders is greater than or equal to the length of st, remove all
					st = []*types.Header{}
				} else {
					// Otherwise, remove the oldest headers
					st = st[excessHeaders:]
				}
			}
		}

		// Append the new header
		st = append(st, block)
		return st
	})
}

// ReadPendingBlocks reads pending block headers from the store associated with the given subscription ID.
// It returns the block headers and a boolean indicating whether the headers were found.
func (ff *Filters) ReadPendingBlocks(id HeadsSubID) ([]*types.Header, bool) {
	res, ok := ff.pendingHeadsStores.Delete(id)
	if !ok {
		return res, false
	}
	return res, true
}

// AddPendingTxs adds pending transactions to the store associated with the given subscription ID.
func (ff *Filters) AddPendingTxs(id PendingTxsSubID, txs []types.Transaction) {
	ff.pendingTxsStores.DoAndStore(id, func(st [][]types.Transaction, ok bool) [][]types.Transaction {
		if !ok {
			st = make([][]types.Transaction, 0)
		}

		// Calculate the total number of transactions in st
		totalTxs := 0
		for _, txBatch := range st {
			totalTxs += len(txBatch)
		}

		maxTxs := ff.config.RpcSubscriptionFiltersMaxTxs
		// If adding the new transactions would exceed maxTxs, remove oldest transactions
		if maxTxs > 0 && totalTxs+len(txs) > maxTxs {
			// Flatten st to a single slice
			flatSt := make([]types.Transaction, 0, totalTxs)
			for _, txBatch := range st {
				flatSt = append(flatSt, txBatch...)
			}

			// Calculate how many transactions need to be removed
			excessTxs := len(flatSt) + len(txs) - maxTxs
			if excessTxs > 0 {
				if excessTxs >= len(flatSt) {
					// If excessTxs is greater than or equal to the length of flatSt, remove all
					flatSt = []types.Transaction{}
				} else {
					// Otherwise, remove the oldest transactions
					flatSt = flatSt[excessTxs:]
				}
			}

			// Convert flatSt back to [][]types.Transaction with a single batch
			st = [][]types.Transaction{flatSt}
		}

		// Append the new transactions as a new batch
		st = append(st, txs)
		return st
	})
}

// ReadPendingTxs reads pending transactions from the store associated with the given subscription ID.
// It returns the transactions and a boolean indicating whether the transactions were found.
func (ff *Filters) ReadPendingTxs(id PendingTxsSubID) ([][]types.Transaction, bool) {
	res, ok := ff.pendingTxsStores.Delete(id)
	if !ok {
		return res, false
	}
	return res, true
}
