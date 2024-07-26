// Copyright 2021 The Erigon Authors
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

package txpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/holiman/uint256"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/direct"
	"github.com/erigontech/erigon-lib/gointerfaces/grpcutil"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	types2 "github.com/erigontech/erigon-lib/types"
)

// Fetch connects to sentry and implements eth/66 protocol regarding the transaction
// messages. It tries to "prime" the sentry with StatusData message containing given
// genesis hash and list of forks, but with zero max block and total difficulty
// Sentry should have a logic not to overwrite statusData with messages from txn pool
type Fetch struct {
	ctx                      context.Context // Context used for cancellation and closing of the fetcher
	pool                     Pool            // Transaction pool implementation
	coreDB                   kv.RoDB
	db                       kv.RwDB
	stateChangesClient       StateChangesClient
	wg                       *sync.WaitGroup // used for synchronisation in the tests (nil when not in tests)
	stateChangesParseCtx     *types2.TxParseContext
	pooledTxsParseCtx        *types2.TxParseContext
	sentryClients            []direct.SentryClient // sentry clients that will be used for accessing the network
	stateChangesParseCtxLock sync.Mutex
	pooledTxsParseCtxLock    sync.Mutex
	logger                   log.Logger
}

type StateChangesClient interface {
	StateChanges(ctx context.Context, in *remote.StateChangeRequest, opts ...grpc.CallOption) (remote.KV_StateChangesClient, error)
}

// NewFetch creates a new fetch object that will work with given sentry clients. Since the
// SentryClient here is an interface, it is suitable for mocking in tests (mock will need
// to implement all the functions of the SentryClient interface).
func NewFetch(ctx context.Context, sentryClients []direct.SentryClient, pool Pool, stateChangesClient StateChangesClient, coreDB kv.RoDB, db kv.RwDB,
	chainID uint256.Int, logger log.Logger) *Fetch {
	f := &Fetch{
		ctx:                  ctx,
		sentryClients:        sentryClients,
		pool:                 pool,
		coreDB:               coreDB,
		db:                   db,
		stateChangesClient:   stateChangesClient,
		stateChangesParseCtx: types2.NewTxParseContext(chainID).ChainIDRequired(), //TODO: change ctx if rules changed
		pooledTxsParseCtx:    types2.NewTxParseContext(chainID).ChainIDRequired(),
		logger:               logger,
	}
	f.pooledTxsParseCtx.ValidateRLP(f.pool.ValidateSerializedTxn)
	f.stateChangesParseCtx.ValidateRLP(f.pool.ValidateSerializedTxn)

	return f
}

func (f *Fetch) SetWaitGroup(wg *sync.WaitGroup) {
	f.wg = wg
}

func (f *Fetch) threadSafeParsePooledTxn(cb func(*types2.TxParseContext) error) error {
	f.pooledTxsParseCtxLock.Lock()
	defer f.pooledTxsParseCtxLock.Unlock()
	return cb(f.pooledTxsParseCtx)
}

func (f *Fetch) threadSafeParseStateChangeTxn(cb func(*types2.TxParseContext) error) error {
	f.stateChangesParseCtxLock.Lock()
	defer f.stateChangesParseCtxLock.Unlock()
	return cb(f.stateChangesParseCtx)
}

// ConnectSentries initialises connection to the sentry
func (f *Fetch) ConnectSentries() {
	for i := range f.sentryClients {
		go func(i int) {
			f.receiveMessageLoop(f.sentryClients[i])
		}(i)
		go func(i int) {
			f.receivePeerLoop(f.sentryClients[i])
		}(i)
	}
}

func (f *Fetch) ConnectCore() {
	go func() {
		for {
			select {
			case <-f.ctx.Done():
				return
			default:
			}
			if err := f.handleStateChanges(f.ctx, f.stateChangesClient); err != nil {
				if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
					time.Sleep(3 * time.Second)
					continue
				}
				f.logger.Warn("[txpool.handleStateChanges]", "err", err)
			}
		}
	}()
}

func (f *Fetch) receiveMessageLoop(sentryClient sentry.SentryClient) {
	for {
		select {
		case <-f.ctx.Done():
			return
		default:
		}
		if _, err := sentryClient.HandShake(f.ctx, &emptypb.Empty{}, grpc.WaitForReady(true)); err != nil {
			if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
				time.Sleep(3 * time.Second)
				continue
			}
			// Report error and wait more
			f.logger.Warn("[txpool.recvMessage] sentry not ready yet", "err", err)
			continue
		}
		if err := f.receiveMessage(f.ctx, sentryClient); err != nil {
			if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
				time.Sleep(3 * time.Second)
				continue
			}
			f.logger.Warn("[txpool.recvMessage]", "err", err)
		}
	}
}

func (f *Fetch) receiveMessage(ctx context.Context, sentryClient sentry.SentryClient) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := sentryClient.Messages(streamCtx, &sentry.MessagesRequest{Ids: []sentry.MessageId{
		sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
		sentry.MessageId_GET_POOLED_TRANSACTIONS_66,
		sentry.MessageId_TRANSACTIONS_66,
		sentry.MessageId_POOLED_TRANSACTIONS_66,
		sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_68,
	}}, grpc.WaitForReady(true))
	if err != nil {
		select {
		case <-f.ctx.Done():
			return ctx.Err()
		default:
		}
		return err
	}
	var req *sentry.InboundMessage
	for req, err = stream.Recv(); ; req, err = stream.Recv() {
		if err != nil {
			select {
			case <-f.ctx.Done():
				return ctx.Err()
			default:
			}
			return fmt.Errorf("txpool.receiveMessage: %w", err)
		}
		if req == nil {
			return nil
		}
		if err = f.handleInboundMessage(streamCtx, req, sentryClient); err != nil {
			if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
				time.Sleep(3 * time.Second)
				continue
			}
			f.logger.Debug("[txpool.fetch] Handling incoming message", "reqID", req.Id.String(), "err", err)
		}
		if f.wg != nil {
			f.wg.Done()
		}
	}
}

func (f *Fetch) handleInboundMessage(ctx context.Context, req *sentry.InboundMessage, sentryClient sentry.SentryClient) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s, rlp: %x", rec, dbg.Stack(), req.Data)
		}
	}()

	if !f.pool.Started() {
		return nil
	}
	tx, err := f.db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	switch req.Id {
	case sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66:
		hashCount, pos, err := types2.ParseHashesCount(req.Data, 0)
		if err != nil {
			return fmt.Errorf("parsing NewPooledTransactionHashes: %w", err)
		}
		hashes := make([]byte, 32*hashCount)
		for i := 0; i < len(hashes); i += 32 {
			if _, pos, err = types2.ParseHash(req.Data, pos, hashes[i:]); err != nil {
				return err
			}
		}
		unknownHashes, err := f.pool.FilterKnownIdHashes(tx, hashes)
		if err != nil {
			return err
		}
		if len(unknownHashes) > 0 {
			var encodedRequest []byte
			var messageID sentry.MessageId
			if encodedRequest, err = types2.EncodeGetPooledTransactions66(unknownHashes, uint64(1), nil); err != nil {
				return err
			}
			messageID = sentry.MessageId_GET_POOLED_TRANSACTIONS_66
			if _, err = sentryClient.SendMessageById(f.ctx, &sentry.SendMessageByIdRequest{
				Data:   &sentry.OutboundMessageData{Id: messageID, Data: encodedRequest},
				PeerId: req.PeerId,
			}, &grpc.EmptyCallOption{}); err != nil {
				return err
			}
		}
	case sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_68:
		_, _, hashes, _, err := rlp.ParseAnnouncements(req.Data, 0)
		if err != nil {
			return fmt.Errorf("parsing NewPooledTransactionHashes88: %w", err)
		}
		unknownHashes, err := f.pool.FilterKnownIdHashes(tx, hashes)
		if err != nil {
			return err
		}

		if len(unknownHashes) > 0 {
			var encodedRequest []byte
			var messageID sentry.MessageId
			if encodedRequest, err = types2.EncodeGetPooledTransactions66(unknownHashes, uint64(1), nil); err != nil {
				return err
			}
			messageID = sentry.MessageId_GET_POOLED_TRANSACTIONS_66
			if _, err = sentryClient.SendMessageById(f.ctx, &sentry.SendMessageByIdRequest{
				Data:   &sentry.OutboundMessageData{Id: messageID, Data: encodedRequest},
				PeerId: req.PeerId,
			}, &grpc.EmptyCallOption{}); err != nil {
				return err
			}
		}
	case sentry.MessageId_GET_POOLED_TRANSACTIONS_66:
		//TODO: handleInboundMessage is single-threaded - means it can accept as argument couple buffers (or analog of txParseContext). Protobuf encoding will copy data anyway, but DirectClient doesn't
		var encodedRequest []byte
		var messageID sentry.MessageId
		messageID = sentry.MessageId_POOLED_TRANSACTIONS_66
		requestID, hashes, _, err := types2.ParseGetPooledTransactions66(req.Data, 0, nil)
		if err != nil {
			return err
		}

		// limit to max 256 transactions in a reply
		const hashSize = 32
		hashes = hashes[:min(len(hashes), 256*hashSize)]

		var txs [][]byte
		responseSize := 0
		processed := len(hashes)

		for i := 0; i < len(hashes); i += hashSize {
			if responseSize >= p2pTxPacketLimit {
				processed = i
				log.Debug("txpool.Fetch.handleInboundMessage PooledTransactions reply truncated to fit p2pTxPacketLimit", "requested", len(hashes), "processed", processed)
				break
			}

			txnHash := hashes[i:min(i+hashSize, len(hashes))]
			txn, err := f.pool.GetRlp(tx, txnHash)
			if err != nil {
				return err
			}
			if txn == nil {
				continue
			}

			txs = append(txs, txn)
			responseSize += len(txn)
		}

		encodedRequest = types2.EncodePooledTransactions66(txs, requestID, nil)
		if len(encodedRequest) > p2pTxPacketLimit {
			log.Debug("txpool.Fetch.handleInboundMessage PooledTransactions reply exceeds p2pTxPacketLimit", "requested", len(hashes), "processed", processed)
		}

		if _, err := sentryClient.SendMessageById(f.ctx, &sentry.SendMessageByIdRequest{
			Data:   &sentry.OutboundMessageData{Id: messageID, Data: encodedRequest},
			PeerId: req.PeerId,
		}, &grpc.EmptyCallOption{}); err != nil {
			return err
		}
	case sentry.MessageId_POOLED_TRANSACTIONS_66, sentry.MessageId_TRANSACTIONS_66:
		txs := types2.TxSlots{}
		if err := f.threadSafeParsePooledTxn(func(parseContext *types2.TxParseContext) error {
			return nil
		}); err != nil {
			return err
		}

		switch req.Id {
		case sentry.MessageId_TRANSACTIONS_66:
			if err := f.threadSafeParsePooledTxn(func(parseContext *types2.TxParseContext) error {
				if _, err := types2.ParseTransactions(req.Data, 0, parseContext, &txs, func(hash []byte) error {
					known, err := f.pool.IdHashKnown(tx, hash)
					if err != nil {
						return err
					}
					if known {
						return types2.ErrRejected
					}
					return nil
				}); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
		case sentry.MessageId_POOLED_TRANSACTIONS_66:
			if err := f.threadSafeParsePooledTxn(func(parseContext *types2.TxParseContext) error {
				if _, _, err := types2.ParsePooledTransactions66(req.Data, 0, parseContext, &txs, func(hash []byte) error {
					known, err := f.pool.IdHashKnown(tx, hash)
					if err != nil {
						return err
					}
					if known {
						return types2.ErrRejected
					}
					return nil
				}); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected message: %s", req.Id.String())
		}
		if len(txs.Txs) == 0 {
			return nil
		}
		f.pool.AddRemoteTxs(ctx, txs)
	default:
		defer f.logger.Trace("[txpool] dropped p2p message", "id", req.Id)
	}

	return nil
}

func (f *Fetch) receivePeerLoop(sentryClient sentry.SentryClient) {
	for {
		select {
		case <-f.ctx.Done():
			return
		default:
		}
		if _, err := sentryClient.HandShake(f.ctx, &emptypb.Empty{}, grpc.WaitForReady(true)); err != nil {
			if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
				time.Sleep(3 * time.Second)
				continue
			}
			// Report error and wait more
			f.logger.Warn("[txpool.recvPeers] sentry not ready yet", "err", err)
			time.Sleep(time.Second)
			continue
		}
		if err := f.receivePeer(sentryClient); err != nil {
			if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
				time.Sleep(3 * time.Second)
				continue
			}

			f.logger.Warn("[txpool.recvPeers]", "err", err)
		}
	}
}

func (f *Fetch) receivePeer(sentryClient sentry.SentryClient) error {
	streamCtx, cancel := context.WithCancel(f.ctx)
	defer cancel()

	stream, err := sentryClient.PeerEvents(streamCtx, &sentry.PeerEventsRequest{})
	if err != nil {
		select {
		case <-f.ctx.Done():
			return f.ctx.Err()
		default:
		}
		return err
	}

	var req *sentry.PeerEvent
	for req, err = stream.Recv(); ; req, err = stream.Recv() {
		if err != nil {
			return err
		}
		if req == nil {
			return nil
		}
		if err = f.handleNewPeer(req); err != nil {
			return err
		}
		if f.wg != nil {
			f.wg.Done()
		}
	}
}

func (f *Fetch) handleNewPeer(req *sentry.PeerEvent) error {
	if req == nil {
		return nil
	}
	switch req.EventId {
	case sentry.PeerEvent_Connect:
		f.pool.AddNewGoodPeer(req.PeerId)
	}

	return nil
}

func (f *Fetch) handleStateChanges(ctx context.Context, client StateChangesClient) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.StateChanges(streamCtx, &remote.StateChangeRequest{WithStorage: false, WithTransactions: true}, grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	for req, err := stream.Recv(); ; req, err = stream.Recv() {
		if err != nil {
			return err
		}
		if req == nil {
			return nil
		}
		if err := f.handleStateChangesRequest(ctx, req); err != nil {
			f.logger.Warn("[fetch] onNewBlock", "err", err)
		}

		if f.wg != nil { // to help tests
			f.wg.Done()
		}
	}
}

func (f *Fetch) handleStateChangesRequest(ctx context.Context, req *remote.StateChangeBatch) error {
	var unwindTxs, unwindBlobTxs, minedTxs types2.TxSlots
	for _, change := range req.ChangeBatch {
		if change.Direction == remote.Direction_FORWARD {
			minedTxs.Resize(uint(len(change.Txs)))
			for i := range change.Txs {
				minedTxs.Txs[i] = &types2.TxSlot{}
				if err := f.threadSafeParseStateChangeTxn(func(parseContext *types2.TxParseContext) error {
					_, err := parseContext.ParseTransaction(change.Txs[i], 0, minedTxs.Txs[i], minedTxs.Senders.At(i), false /* hasEnvelope */, false /* wrappedWithBlobs */, nil)
					return err
				}); err != nil && !errors.Is(err, context.Canceled) {
					f.logger.Warn("[txpool.fetch] stream.Recv", "err", err)
					continue // 1 txn handling error must not stop batch processing
				}
			}
		} else if change.Direction == remote.Direction_UNWIND {
			for i := range change.Txs {
				if err := f.threadSafeParseStateChangeTxn(func(parseContext *types2.TxParseContext) error {
					utx := &types2.TxSlot{}
					sender := make([]byte, 20)
					_, err := parseContext.ParseTransaction(change.Txs[i], 0, utx, sender, false /* hasEnvelope */, false /* wrappedWithBlobs */, nil)
					if err != nil {
						return err
					}
					if utx.Type == types2.BlobTxType {
						unwindBlobTxs.Append(utx, sender, false)
					} else {
						unwindTxs.Append(utx, sender, false)
					}
					return nil
				}); err != nil && !errors.Is(err, context.Canceled) {
					f.logger.Warn("[txpool.fetch] stream.Recv", "err", err)
					continue // 1 txn handling error must not stop batch processing
				}
			}
		}
	}

	if err := f.db.View(ctx, func(tx kv.Tx) error {
		return f.pool.OnNewBlock(ctx, req, unwindTxs, unwindBlobTxs, minedTxs, tx)
	}); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
