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

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	txpool2 "github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/erigon/rlp"
)

type Filters struct {
	mu sync.RWMutex

	pendingBlock *types.Block

	headsSubs        *SyncMap[HeadsSubID, Sub[*types.Header]]
	pendingLogsSubs  *SyncMap[PendingLogsSubID, Sub[types.Logs]]
	pendingBlockSubs *SyncMap[PendingBlockSubID, Sub[*types.Block]]
	pendingTxsSubs   *SyncMap[PendingTxsSubID, Sub[[]types.Transaction]]
	logsSubs         *LogsFilterAggregator
	logsRequestor    atomic.Value
	onNewSnapshot    func()

	storeMu            sync.Mutex
	logsStores         *SyncMap[LogsSubID, []*types.Log]
	pendingHeadsStores *SyncMap[HeadsSubID, []*types.Header]
	pendingTxsStores   *SyncMap[PendingTxsSubID, [][]types.Transaction]
}

func New(ctx context.Context, ethBackend ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient, onNewSnapshot func()) *Filters {
	log.Info("rpc filters: subscribing to Erigon events")

	ff := &Filters{
		headsSubs:          NewSyncMap[HeadsSubID, Sub[*types.Header]](),
		pendingTxsSubs:     NewSyncMap[PendingTxsSubID, Sub[[]types.Transaction]](),
		pendingLogsSubs:    NewSyncMap[PendingLogsSubID, Sub[types.Logs]](),
		pendingBlockSubs:   NewSyncMap[PendingBlockSubID, Sub[*types.Block]](),
		logsSubs:           NewLogsFilterAggregator(),
		onNewSnapshot:      onNewSnapshot,
		logsStores:         NewSyncMap[LogsSubID, []*types.Log](),
		pendingHeadsStores: NewSyncMap[HeadsSubID, []*types.Header](),
		pendingTxsStores:   NewSyncMap[PendingTxsSubID, [][]types.Transaction](),
	}

	go func() {
		if ethBackend == nil {
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if err := ethBackend.Subscribe(ctx, ff.OnNewEvent); err != nil {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if grpcutil.IsEndOfStream(err) || grpcutil.IsRetryLater(err) {
					time.Sleep(3 * time.Second)
					continue
				}
				log.Warn("rpc filters: error subscribing to events", "err", err)
			}
		}
	}()

	go func() {
		if ethBackend == nil {
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if err := ethBackend.SubscribeLogs(ctx, ff.OnNewLogs, &ff.logsRequestor); err != nil {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if grpcutil.IsEndOfStream(err) || grpcutil.IsRetryLater(err) {
					time.Sleep(3 * time.Second)
					continue
				}
				log.Warn("rpc filters: error subscribing to logs", "err", err)
			}
		}
	}()

	if txPool != nil {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				if err := ff.subscribeToPendingTransactions(ctx, txPool); err != nil {
					select {
					case <-ctx.Done():
						return
					default:
					}
					if grpcutil.IsEndOfStream(err) || grpcutil.IsRetryLater(err) || grpcutil.ErrIs(err, txpool2.ErrPoolDisabled) {
						time.Sleep(3 * time.Second)
						continue
					}
					log.Warn("rpc filters: error subscribing to pending transactions", "err", err)
				}
			}
		}()

		if !reflect.ValueOf(mining).IsNil() { //https://groups.google.com/g/golang-nuts/c/wnH302gBa4I
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					if err := ff.subscribeToPendingBlocks(ctx, mining); err != nil {
						select {
						case <-ctx.Done():
							return
						default:
						}
						if grpcutil.IsEndOfStream(err) || grpcutil.IsRetryLater(err) {
							time.Sleep(3 * time.Second)
							continue
						}
						log.Warn("rpc filters: error subscribing to pending blocks", "err", err)
					}
				}
			}()
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					default:
					}
					if err := ff.subscribeToPendingLogs(ctx, mining); err != nil {
						select {
						case <-ctx.Done():
							return
						default:
						}
						if grpcutil.IsEndOfStream(err) || grpcutil.IsRetryLater(err) {
							time.Sleep(3 * time.Second)
							continue
						}
						log.Warn("rpc filters: error subscribing to pending logs", "err", err)
					}
				}
			}()
		}
	}

	return ff
}

func (ff *Filters) LastPendingBlock() *types.Block {
	ff.mu.RLock()
	defer ff.mu.RUnlock()
	return ff.pendingBlock
}

func (ff *Filters) subscribeToPendingTransactions(ctx context.Context, txPool txpool.TxpoolClient) error {
	subscription, err := txPool.OnAdd(ctx, &txpool.OnAddRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	for {
		event, err := subscription.Recv()
		if errors.Is(err, io.EOF) {
			log.Debug("rpcdaemon: the subscription to pending transactions channel was closed")
			break
		}
		if err != nil {
			return err
		}

		ff.OnNewTx(event)
	}
	return nil
}

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
			log.Debug("rpcdaemon: the subscription to pending blocks channel was closed")
			break
		}
		if err != nil {
			return err
		}

		ff.HandlePendingBlock(event)
	}
	return nil
}

func (ff *Filters) HandlePendingBlock(reply *txpool.OnPendingBlockReply) {
	b := &types.Block{}
	if reply == nil || len(reply.RplBlock) == 0 {
		return
	}
	if err := rlp.Decode(bytes.NewReader(reply.RplBlock), b); err != nil {
		log.Warn("OnNewPendingBlock rpc filters, unprocessable payload", "err", err)
	}

	ff.mu.Lock()
	defer ff.mu.Unlock()
	ff.pendingBlock = b

	ff.pendingBlockSubs.Range(func(k PendingBlockSubID, v Sub[*types.Block]) error {
		v.Send(b)
		return nil
	})
}

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
			log.Debug("rpcdaemon: the subscription to pending logs channel was closed")
			break
		}
		if err != nil {
			return err
		}

		ff.HandlePendingLogs(event)
	}
	return nil
}

func (ff *Filters) HandlePendingLogs(reply *txpool.OnPendingLogsReply) {
	if len(reply.RplLogs) == 0 {
		return
	}
	l := []*types.Log{}
	if err := rlp.Decode(bytes.NewReader(reply.RplLogs), &l); err != nil {
		log.Warn("OnNewPendingLogs rpc filters, unprocessable payload", "err", err)
	}
	ff.pendingLogsSubs.Range(func(k PendingLogsSubID, v Sub[types.Logs]) error {
		v.Send(l)
		return nil
	})
}

func (ff *Filters) SubscribeNewHeads(size int) (<-chan *types.Header, HeadsSubID) {
	id := HeadsSubID(generateSubscriptionID())
	sub := newChanSub[*types.Header](size)
	ff.headsSubs.Put(id, sub)
	return sub.ch, id
}

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

func (ff *Filters) SubscribePendingLogs(size int) (<-chan types.Logs, PendingLogsSubID) {
	id := PendingLogsSubID(generateSubscriptionID())
	sub := newChanSub[types.Logs](size)
	ff.pendingLogsSubs.Put(id, sub)
	return sub.ch, id
}

func (ff *Filters) UnsubscribePendingLogs(id PendingLogsSubID) {
	ch, ok := ff.pendingLogsSubs.Get(id)
	if !ok {
		return
	}
	ch.Close()
	ff.pendingLogsSubs.Delete(id)
}

func (ff *Filters) SubscribePendingBlock(size int) (<-chan *types.Block, PendingBlockSubID) {
	id := PendingBlockSubID(generateSubscriptionID())
	sub := newChanSub[*types.Block](size)
	ff.pendingBlockSubs.Put(id, sub)
	return sub.ch, id
}

func (ff *Filters) UnsubscribePendingBlock(id PendingBlockSubID) {
	ch, ok := ff.pendingBlockSubs.Get(id)
	if !ok {
		return
	}
	ch.Close()
	ff.pendingBlockSubs.Delete(id)
}

func (ff *Filters) SubscribePendingTxs(size int) (<-chan []types.Transaction, PendingTxsSubID) {
	id := PendingTxsSubID(generateSubscriptionID())
	sub := newChanSub[[]types.Transaction](size)
	ff.pendingTxsSubs.Put(id, sub)
	return sub.ch, id
}

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

func (ff *Filters) SubscribeLogs(size int, crit filters.FilterCriteria) (<-chan *types.Log, LogsSubID) {
	sub := newChanSub[*types.Log](size)
	id, f := ff.logsSubs.insertLogsFilter(sub)
	f.addrs = map[libcommon.Address]int{}
	if len(crit.Addresses) == 0 {
		f.allAddrs = 1
	} else {
		for _, addr := range crit.Addresses {
			f.addrs[addr] = 1
		}
	}
	f.topics = map[libcommon.Hash]int{}
	if len(crit.Topics) == 0 {
		f.allTopics = 1
	} else {
		for _, topics := range crit.Topics {
			for _, topic := range topics {
				f.topics[topic] = 1
			}
		}
	}
	f.topicsOriginal = crit.Topics
	ff.logsSubs.addLogsFilters(f)
	// if any filter in the aggregate needs all addresses or all topics then the global log subscription needs to
	// allow all addresses or topics through
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
			log.Warn("Could not update remote logs filter", "err", err)
			ff.logsSubs.removeLogsFilter(id)
		}
	}

	return sub.ch, id
}

func (ff *Filters) loadLogsRequester() any {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	return ff.logsRequestor.Load()
}

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
			log.Warn("Could not update remote logs filter", "err", err)
			return isDeleted || ff.logsSubs.removeLogsFilter(id)
		}
	}

	ff.deleteLogStore(id)

	return isDeleted
}

func (ff *Filters) deleteLogStore(id LogsSubID) {
	ff.logsStores.Delete(id)
}

// OnNewEvent is called when there is a new Event from the remote
func (ff *Filters) OnNewEvent(event *remote.SubscribeReply) {
	err := ff.onNewEvent(event)
	if err != nil {
		log.Warn("OnNewEvent Filters", "event", event.Type, "err", err)
	}
}

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

func (ff *Filters) OnNewTx(reply *txpool.OnAddReply) {
	txs := make([]types.Transaction, len(reply.RplTxs))
	for i, rlpTx := range reply.RplTxs {
		var decodeErr error
		if len(rlpTx) == 0 {
			continue
		}
		txs[i], decodeErr = types.DecodeWrappedTransaction(rlpTx)
		if decodeErr != nil {
			// ignoring what we can't unmarshal
			log.Warn("OnNewTx rpc filters, unprocessable payload", "err", decodeErr, "data", hex.EncodeToString(rlpTx))
			break
		}
	}
	ff.pendingTxsSubs.Range(func(k PendingTxsSubID, v Sub[[]types.Transaction]) error {
		v.Send(txs)
		return nil
	})
}

// OnNewLogs is called when there is a new log
func (ff *Filters) OnNewLogs(reply *remote.SubscribeLogsReply) {
	ff.logsSubs.distributeLog(reply)
}

func (ff *Filters) AddLogs(id LogsSubID, logs *types.Log) {
	ff.logsStores.DoAndStore(id, func(st []*types.Log, ok bool) []*types.Log {
		if !ok {
			st = make([]*types.Log, 0)
		}
		st = append(st, logs)
		return st
	})
}

func (ff *Filters) ReadLogs(id LogsSubID) ([]*types.Log, bool) {
	res, ok := ff.logsStores.Delete(id)
	if !ok {
		return res, false
	}
	return res, true
}

func (ff *Filters) AddPendingBlock(id HeadsSubID, block *types.Header) {
	ff.pendingHeadsStores.DoAndStore(id, func(st []*types.Header, ok bool) []*types.Header {
		if !ok {
			st = make([]*types.Header, 0)
		}
		st = append(st, block)
		return st
	})
}

func (ff *Filters) ReadPendingBlocks(id HeadsSubID) ([]*types.Header, bool) {
	res, ok := ff.pendingHeadsStores.Delete(id)
	if !ok {
		return res, false
	}
	return res, true
}

func (ff *Filters) AddPendingTxs(id PendingTxsSubID, txs []types.Transaction) {
	ff.pendingTxsStores.DoAndStore(id, func(st [][]types.Transaction, ok bool) [][]types.Transaction {
		if !ok {
			st = make([][]types.Transaction, 0)
		}
		st = append(st, txs)
		return st
	})
}

func (ff *Filters) ReadPendingTxs(id PendingTxsSubID) ([][]types.Transaction, bool) {
	res, ok := ff.pendingTxsStores.Delete(id)
	if !ok {
		return res, false
	}
	return res, true
}
