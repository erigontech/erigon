package filters

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/gointerfaces/remote"
	"github.com/ledgerwatch/turbo-geth/gointerfaces/txpool"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type (
	SubscriptionID    string
	HeadsSubID        SubscriptionID
	PendingLogsSubID  SubscriptionID
	PendingBlockSubID SubscriptionID
	PendingTxsSubID   SubscriptionID
)

type Filters struct {
	mu sync.RWMutex

	headsSubs        map[HeadsSubID]chan *types.Header
	pendingLogsSubs  map[PendingLogsSubID]func(types.Logs)
	pendingBlockSubs map[PendingBlockSubID]func(*types.Block)
	pendingTxsSubs   map[PendingTxsSubID]chan []types.Transaction
}

func New(ctx context.Context, ethBackend core.ApiBackend, txPool txpool.TxpoolClient) *Filters {
	log.Info("rpc filters: subscribing to tg events")

	ff := &Filters{
		headsSubs:        make(map[HeadsSubID]chan *types.Header),
		pendingTxsSubs:   make(map[PendingTxsSubID]chan []types.Transaction),
		pendingLogsSubs:  make(map[PendingLogsSubID]func(types.Logs)),
		pendingBlockSubs: make(map[PendingBlockSubID]func(*types.Block)),
	}

	go func() {
		if ethBackend == nil {
			return
		}
		if err := ethBackend.Subscribe(ctx, ff.OnNewEvent); err != nil {
			log.Warn("rpc filters: error subscribing to events", "err", err)
			time.Sleep(time.Second)
		}
	}()

	go func() {
		if txPool == nil {
			return
		}
		if err := ff.subscribeToPendingTransactions(ctx, txPool); err != nil {
			log.Warn("rpc filters: error subscribing to pending transactions", "err", err)
			time.Sleep(time.Second)
		}
		if err := ff.subscribeToPendingBlocks(ctx, txPool); err != nil {
			log.Warn("rpc filters: error subscribing to pending transactions", "err", err)
			time.Sleep(time.Second)
		}
	}()

	return ff
}

func (ff *Filters) subscribeToPendingTransactions(ctx context.Context, txPool txpool.TxpoolClient) error {
	subscription, err := txPool.OnAdd(ctx, &txpool.OnAddRequest{}, grpc.WaitForReady(true))
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return errors.New(s.Message())
		}
		return err
	}
	for {
		event, err := subscription.Recv()
		if err == io.EOF {
			log.Info("rpcdaemon: the subscription channel was closed")
			break
		}
		if err != nil {
			return err
		}

		ff.OnNewTx(event)
	}
	return nil
}

func (ff *Filters) subscribeToPendingBlocks(ctx context.Context, txPool txpool.TxpoolClient) error {
	subscription, err := txPool.OnPendingBlock(ctx, &txpool.OnPendingBlockRequest{}, grpc.WaitForReady(true))
	if err != nil {
		if s, ok := status.FromError(err); ok {
			return errors.New(s.Message())
		}
		return err
	}
	for {
		event, err := subscription.Recv()
		if err == io.EOF {
			log.Info("rpcdaemon: the subscription channel was closed")
			break
		}
		if err != nil {
			return err
		}

		ff.OnPendingBlock(event)
	}
	return nil
}

func (ff *Filters) SubscribeNewHeads(out chan *types.Header) HeadsSubID {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	id := HeadsSubID(generateSubscriptionID())
	ff.headsSubs[id] = out
	return id
}

func (ff *Filters) UnsubscribeHeads(id HeadsSubID) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	delete(ff.headsSubs, id)
}

func (ff *Filters) SubscribePendingLogs(ctx context.Context, f func(types.Logs)) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	id := PendingLogsSubID(generateSubscriptionID())
	ff.pendingLogsSubs[id] = f
	go func() {
		<-ctx.Done()
		ff.mu.Lock()
		defer ff.mu.Unlock()
		delete(ff.pendingLogsSubs, id)
	}()
}

func (ff *Filters) SubscribePendingBlock(ctx context.Context, f func(*types.Block)) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	id := PendingBlockSubID(generateSubscriptionID())
	ff.pendingBlockSubs[id] = f
	go func() {
		<-ctx.Done()
		ff.mu.Lock()
		defer ff.mu.Unlock()
		delete(ff.pendingBlockSubs, id)
	}()
}

func (ff *Filters) SubscribePendingTxs(out chan []types.Transaction) PendingTxsSubID {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	id := PendingTxsSubID(generateSubscriptionID())
	ff.pendingTxsSubs[id] = out
	return id
}

func (ff *Filters) UnsubscribePendingTxs(id PendingTxsSubID) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	delete(ff.pendingTxsSubs, id)
}

func (ff *Filters) OnNewEvent(event *remote.SubscribeReply) {
	ff.mu.RLock()
	defer ff.mu.RUnlock()

	switch event.Type {
	case remote.Event_HEADER:
		payload := event.Data
		var header types.Header

		err := rlp.Decode(bytes.NewReader(payload), &header)
		if err != nil {
			// ignoring what we can't unmarshal
			log.Warn("OnNewEvent rpc filters (header), unprocessable payload", "err", err)
		} else {
			for _, v := range ff.headsSubs {
				v <- &header
			}
		}
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
	for i, rplTx := range reply.RplTxs {
		var decodeErr error
		txs[i], decodeErr = types.UnmarshalTransactionFromBinary(rplTx)
		if decodeErr != nil {
			// ignoring what we can't unmarshal
			log.Warn("OnNewTx rpc filters, unprocessable payload", "err", decodeErr)
			break
		}
	}
	for _, v := range ff.pendingTxsSubs {
		v <- txs
	}
}

func (ff *Filters) OnPendingBlock(reply *txpool.OnPendingBlockReply) {
	var b *types.Block
	if err := rlp.Decode(bytes.NewReader(reply.RplBlock), b); err != nil {
		log.Warn("OnNewTx rpc filters, unprocessable payload", "err", err)
	}
	for _, v := range ff.pendingBlockSubs {
		v(b)
	}
}

func generateSubscriptionID() SubscriptionID {
	var id [32]byte

	_, err := rand.Read(id[:])
	if err != nil {
		log.Crit("rpc filters: error creating random id", "err", err)
	}

	return SubscriptionID(fmt.Sprintf("%x", id))
}
