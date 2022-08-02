package rpchelper

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	txpool2 "github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/erigon/rlp"
)

type (
	SubscriptionID    string
	HeadsSubID        SubscriptionID
	PendingLogsSubID  SubscriptionID
	PendingBlockSubID SubscriptionID
	PendingTxsSubID   SubscriptionID
	LogsSubID         uint64
)

type Filters struct {
	mu sync.RWMutex

	pendingBlock *types.Block

	headsSubs        map[HeadsSubID]chan *types.Header
	pendingLogsSubs  map[PendingLogsSubID]chan types.Logs
	pendingBlockSubs map[PendingBlockSubID]chan *types.Block
	pendingTxsSubs   map[PendingTxsSubID]chan []types.Transaction
	logsSubs         *LogsFilterAggregator
	logsRequestor    atomic.Value
	onNewSnapshot    func()

	storeMu            sync.Mutex
	logsStores         map[LogsSubID][]*types.Log
	pendingHeadsStores map[HeadsSubID][]*types.Header
	pendingTxsStores   map[PendingTxsSubID][][]types.Transaction
}

func New(ctx context.Context, ethBackend ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient, onNewSnapshot func()) *Filters {
	log.Info("rpc filters: subscribing to Erigon events")

	ff := &Filters{
		headsSubs:          make(map[HeadsSubID]chan *types.Header),
		pendingTxsSubs:     make(map[PendingTxsSubID]chan []types.Transaction),
		pendingLogsSubs:    make(map[PendingLogsSubID]chan types.Logs),
		pendingBlockSubs:   make(map[PendingBlockSubID]chan *types.Block),
		logsSubs:           NewLogsFilterAggregator(),
		onNewSnapshot:      onNewSnapshot,
		logsStores:         make(map[LogsSubID][]*types.Log),
		pendingHeadsStores: make(map[HeadsSubID][]*types.Header),
		pendingTxsStores:   make(map[PendingTxsSubID][][]types.Transaction),
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

	for _, v := range ff.pendingBlockSubs {
		v <- b
	}
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

	ff.mu.RLock()
	defer ff.mu.RUnlock()
	for _, v := range ff.pendingLogsSubs {
		v <- l
	}
}

func (ff *Filters) SubscribeNewHeads(out chan *types.Header) HeadsSubID {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	id := HeadsSubID(generateSubscriptionID())
	ff.headsSubs[id] = out
	return id
}

func (ff *Filters) UnsubscribeHeads(id HeadsSubID) bool {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	if ch, ok := ff.headsSubs[id]; ok {
		close(ch)
		delete(ff.headsSubs, id)
		ff.storeMu.Lock()
		defer ff.storeMu.Unlock()
		delete(ff.pendingHeadsStores, id)
		return true
	}
	return false
}

func (ff *Filters) SubscribePendingLogs(c chan types.Logs) PendingLogsSubID {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	id := PendingLogsSubID(generateSubscriptionID())
	ff.pendingLogsSubs[id] = c
	return id
}

func (ff *Filters) UnsubscribePendingLogs(id PendingLogsSubID) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	delete(ff.pendingLogsSubs, id)
}

func (ff *Filters) SubscribePendingBlock(f chan *types.Block) PendingBlockSubID {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	id := PendingBlockSubID(generateSubscriptionID())
	ff.pendingBlockSubs[id] = f
	return id
}

func (ff *Filters) UnsubscribePendingBlock(id PendingBlockSubID) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	delete(ff.pendingBlockSubs, id)
}

func (ff *Filters) SubscribePendingTxs(out chan []types.Transaction) PendingTxsSubID {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	id := PendingTxsSubID(generateSubscriptionID())
	ff.pendingTxsSubs[id] = out
	return id
}

func (ff *Filters) UnsubscribePendingTxs(id PendingTxsSubID) bool {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	if ch, ok := ff.pendingTxsSubs[id]; ok {
		close(ch)
		delete(ff.pendingTxsSubs, id)
		ff.storeMu.Lock()
		defer ff.storeMu.Unlock()
		delete(ff.pendingTxsStores, id)
		return true
	}
	return false
}

func (ff *Filters) SubscribeLogs(out chan *types.Log, crit filters.FilterCriteria) LogsSubID {
	id, f := ff.logsSubs.insertLogsFilter(out)
	f.addrs = map[common.Address]int{}
	if len(crit.Addresses) == 0 {
		f.allAddrs = 1
	} else {
		for _, addr := range crit.Addresses {
			f.addrs[addr] = 1
		}
	}
	f.topics = map[common.Hash]int{}
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
	lfr := &remote.LogsFilterRequest{
		AllAddresses: ff.logsSubs.aggLogsFilter.allAddrs == 1,
		AllTopics:    ff.logsSubs.aggLogsFilter.allTopics == 1,
	}
	ff.mu.Lock()
	defer ff.mu.Unlock()
	for addr := range ff.logsSubs.aggLogsFilter.addrs {
		lfr.Addresses = append(lfr.Addresses, gointerfaces.ConvertAddressToH160(addr))
	}
	for topic := range ff.logsSubs.aggLogsFilter.topics {
		lfr.Topics = append(lfr.Topics, gointerfaces.ConvertHashToH256(topic))
	}
	loaded := ff.logsRequestor.Load()
	if loaded != nil {
		if err := loaded.(func(*remote.LogsFilterRequest) error)(lfr); err != nil {
			log.Warn("Could not update remote logs filter", "err", err)
			ff.logsSubs.removeLogsFilter(id)
		}
	}
	return id
}

func (ff *Filters) UnsubscribeLogs(id LogsSubID) bool {
	isDeleted := ff.logsSubs.removeLogsFilter(id)
	lfr := &remote.LogsFilterRequest{
		AllAddresses: ff.logsSubs.aggLogsFilter.allAddrs == 1,
		AllTopics:    ff.logsSubs.aggLogsFilter.allTopics == 1,
	}
	ff.mu.Lock()
	defer ff.mu.Unlock()
	for addr := range ff.logsSubs.aggLogsFilter.addrs {
		lfr.Addresses = append(lfr.Addresses, gointerfaces.ConvertAddressToH160(addr))
	}
	for topic := range ff.logsSubs.aggLogsFilter.topics {
		lfr.Topics = append(lfr.Topics, gointerfaces.ConvertHashToH256(topic))
	}
	loaded := ff.logsRequestor.Load()
	if loaded != nil {
		if err := loaded.(func(*remote.LogsFilterRequest) error)(lfr); err != nil {
			log.Warn("Could not update remote logs filter", "err", err)
			return isDeleted || ff.logsSubs.removeLogsFilter(id)
		}
	}
	ff.storeMu.Lock()
	defer ff.storeMu.Unlock()
	delete(ff.logsStores, id)
	return isDeleted
}

func (ff *Filters) OnNewEvent(event *remote.SubscribeReply) {
	ff.mu.RLock()
	defer ff.mu.RUnlock()

	switch event.Type {
	case remote.Event_HEADER:
		payload := event.Data
		var header types.Header
		if len(payload) == 0 {
			return

		}
		err := rlp.Decode(bytes.NewReader(payload), &header)
		if err != nil {
			// ignoring what we can't unmarshal
			log.Warn("OnNewEvent rpc filters (header), unprocessable payload", "err", err)
		} else {
			for _, v := range ff.headsSubs {
				v <- &header
			}
		}
	case remote.Event_NEW_SNAPSHOT:
		ff.onNewSnapshot()
	//case remote.Event_PENDING_LOGS:
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
	//case remote.Event_PENDING_BLOCK:
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
	default:
		log.Warn("OnNewEvent rpc filters: unsupported event type", "type", event.Type)
		return
	}
}

func (ff *Filters) OnNewTx(reply *txpool.OnAddReply) {
	ff.mu.RLock()
	defer ff.mu.RUnlock()

	txs := make([]types.Transaction, len(reply.RplTxs))
	for i, rlpTx := range reply.RplTxs {
		var decodeErr error
		if len(rlpTx) == 0 {
			continue
		}
		s := rlp.NewStream(bytes.NewReader(rlpTx), uint64(len(rlpTx)))
		txs[i], decodeErr = types.DecodeTransaction(s)
		if decodeErr != nil {
			// ignoring what we can't unmarshal
			log.Warn("OnNewTx rpc filters, unprocessable payload", "err", decodeErr, "data", fmt.Sprintf("%x", rlpTx))
			break
		}
	}
	for _, v := range ff.pendingTxsSubs {
		v <- txs
	}
}

func (ff *Filters) OnNewLogs(reply *remote.SubscribeLogsReply) {
	lg := &types.Log{
		Address:     gointerfaces.ConvertH160toAddress(reply.Address),
		Data:        reply.Data,
		BlockNumber: reply.BlockNumber,
		TxHash:      gointerfaces.ConvertH256ToHash(reply.TransactionHash),
		TxIndex:     uint(reply.TransactionIndex),
		BlockHash:   gointerfaces.ConvertH256ToHash(reply.BlockHash),
		Index:       uint(reply.LogIndex),
		Removed:     reply.Removed,
	}
	t := make([]common.Hash, 0)
	for _, v := range reply.Topics {
		t = append(t, gointerfaces.ConvertH256ToHash(v))
	}
	lg.Topics = t
	ff.logsSubs.distributeLog(reply)
}

func generateSubscriptionID() SubscriptionID {
	var id [32]byte

	_, err := rand.Read(id[:])
	if err != nil {
		log.Crit("rpc filters: error creating random id", "err", err)
	}

	return SubscriptionID(fmt.Sprintf("%x", id))
}

func (ff *Filters) AddLogs(id LogsSubID, logs *types.Log) {
	ff.storeMu.Lock()
	defer ff.storeMu.Unlock()
	st, ok := ff.logsStores[id]
	if !ok {
		st = make([]*types.Log, 0)
	}
	st = append(st, logs)
	ff.logsStores[id] = st
}

func (ff *Filters) ReadLogs(id LogsSubID) ([]*types.Log, bool) {
	ff.storeMu.Lock()
	defer ff.storeMu.Unlock()
	res := make([]*types.Log, 0)
	st, ok := ff.logsStores[id]
	if !ok {
		return res, false
	}
	res = append(res, st...)
	st = make([]*types.Log, 0)
	ff.logsStores[id] = st
	return res, true
}

func (ff *Filters) AddPendingBlock(id HeadsSubID, block *types.Header) {
	ff.storeMu.Lock()
	defer ff.storeMu.Unlock()
	st, ok := ff.pendingHeadsStores[id]
	if !ok {
		st = make([]*types.Header, 0)
	}
	st = append(st, block)
	ff.pendingHeadsStores[id] = st
}

func (ff *Filters) ReadPendingBlocks(id HeadsSubID) ([]*types.Header, bool) {
	ff.storeMu.Lock()
	defer ff.storeMu.Unlock()
	res := make([]*types.Header, 0)
	st, ok := ff.pendingHeadsStores[id]
	if !ok {
		return res, false
	}
	res = append(res, st...)
	st = make([]*types.Header, 0)
	ff.pendingHeadsStores[id] = st
	return res, true
}

func (ff *Filters) AddPendingTxs(id PendingTxsSubID, txs []types.Transaction) {
	ff.storeMu.Lock()
	defer ff.storeMu.Unlock()
	st, ok := ff.pendingTxsStores[id]
	if !ok {
		st = make([][]types.Transaction, 0)
	}
	st = append(st, txs)
	ff.pendingTxsStores[id] = st
}

func (ff *Filters) ReadPendingTxs(id PendingTxsSubID) ([][]types.Transaction, bool) {
	ff.storeMu.Lock()
	defer ff.storeMu.Unlock()
	res := make([][]types.Transaction, 0)
	st, ok := ff.pendingTxsStores[id]
	if !ok {
		return res, false
	}
	res = append(res, st...)
	st = make([][]types.Transaction, 0)
	ff.pendingTxsStores[id] = st
	return res, true
}
