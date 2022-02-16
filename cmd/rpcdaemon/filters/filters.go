package filters

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	txpool2 "github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/services"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

	pendingBlock *types.Block

	headsSubs        map[HeadsSubID]chan *types.Header
	pendingLogsSubs  map[PendingLogsSubID]chan types.Logs
	pendingBlockSubs map[PendingBlockSubID]chan *types.Block
	pendingTxsSubs   map[PendingTxsSubID]chan []types.Transaction
}

func New(ctx context.Context, ethBackend services.ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient) *Filters {
	log.Info("rpc filters: subscribing to Erigon events")

	ff := &Filters{
		headsSubs:        make(map[HeadsSubID]chan *types.Header),
		pendingTxsSubs:   make(map[PendingTxsSubID]chan []types.Transaction),
		pendingLogsSubs:  make(map[PendingLogsSubID]chan types.Logs),
		pendingBlockSubs: make(map[PendingBlockSubID]chan *types.Block),
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
				if s, ok := status.FromError(err); ok && s.Code() == codes.Canceled {
					time.Sleep(3 * time.Second)
					continue
				}
				if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
					time.Sleep(3 * time.Second)
					continue
				}

				log.Warn("rpc filters: error subscribing to events", "err", err)
				time.Sleep(3 * time.Second)
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
					if s, ok := status.FromError(err); ok && s.Code() == codes.Canceled {
						time.Sleep(3 * time.Second)
						continue
					}
					if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, txpool2.ErrPoolDisabled) {
						time.Sleep(3 * time.Second)
						continue
					}
					log.Warn("rpc filters: error subscribing to pending transactions", "err", err)
					time.Sleep(3 * time.Second)
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
						if s, ok := status.FromError(err); ok && s.Code() == codes.Canceled {
							time.Sleep(3 * time.Second)
							continue
						}
						if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
							time.Sleep(3 * time.Second)
							continue
						}
						log.Warn("rpc filters: error subscribing to pending blocks", "err", err)
						time.Sleep(3 * time.Second)
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
						if s, ok := status.FromError(err); ok && s.Code() == codes.Canceled {
							time.Sleep(3 * time.Second)
							continue
						}
						if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
							time.Sleep(3 * time.Second)
							continue
						}
						log.Warn("rpc filters: error subscribing to pending logs", "err", err)
						time.Sleep(3 * time.Second)
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
		if err == io.EOF {
			log.Info("rpcdaemon: the subscription channel was closed")
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
	if len(reply.RplBlock) == 0 {
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
		if err == io.EOF {
			log.Info("rpcdaemon: the subscription channel was closed")
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

func (ff *Filters) UnsubscribeHeads(id HeadsSubID) {
	ff.mu.Lock()
	defer ff.mu.Unlock()
	delete(ff.headsSubs, id)
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

func generateSubscriptionID() SubscriptionID {
	var id [32]byte

	_, err := rand.Read(id[:])
	if err != nil {
		log.Crit("rpc filters: error creating random id", "err", err)
	}

	return SubscriptionID(fmt.Sprintf("%x", id))
}
