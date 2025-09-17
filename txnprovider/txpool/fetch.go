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
	"github.com/erigontech/erigon-lib/gointerfaces/grpcutil"
	"github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/rlp"
)

// Fetch connects to sentry and implements eth/66 protocol regarding the transaction
// messages. It tries to "prime" the sentry with StatusData message containing given
// genesis hash and list of forks, but with zero max block and total difficulty
// Sentry should have a logic not to overwrite statusData with messages from txn pool
type Fetch struct {
	ctx                      context.Context // Context used for cancellation and closing of the fetcher
	pool                     Pool            // Transaction pool implementation
	db                       kv.RwDB
	stateChangesClient       StateChangesClient
	wg                       *sync.WaitGroup // used for synchronisation in the tests (nil when not in tests)
	stateChangesParseCtx     *TxnParseContext
	pooledTxnsParseCtx       *TxnParseContext
	sentryClients            []sentryproto.SentryClient // sentry clients that will be used for accessing the network
	stateChangesParseCtxLock sync.Mutex
	pooledTxnsParseCtxLock   sync.Mutex
	logger                   log.Logger
}

type StateChangesClient interface {
	StateChanges(ctx context.Context, in *remoteproto.StateChangeRequest, opts ...grpc.CallOption) (remoteproto.KV_StateChangesClient, error)
}

// NewFetch creates a new fetch object that will work with given sentry clients. Since the
// SentryClient here is an interface, it is suitable for mocking in tests (mock will need
// to implement all the functions of the SentryClient interface).
func NewFetch(
	ctx context.Context,
	sentryClients []sentryproto.SentryClient,
	pool Pool,
	stateChangesClient StateChangesClient,
	db kv.RwDB,
	chainID uint256.Int,
	logger log.Logger,
	opts ...Option,
) *Fetch {
	options := applyOpts(opts...)
	f := &Fetch{
		ctx:                  ctx,
		sentryClients:        sentryClients,
		pool:                 pool,
		db:                   db,
		stateChangesClient:   stateChangesClient,
		stateChangesParseCtx: NewTxnParseContext(chainID).ChainIDRequired(), //TODO: change ctx if rules changed
		pooledTxnsParseCtx:   NewTxnParseContext(chainID).ChainIDRequired(),
		wg:                   options.p2pFetcherWg,
		logger:               logger,
	}
	f.pooledTxnsParseCtx.ValidateRLP(f.pool.ValidateSerializedTxn)
	f.stateChangesParseCtx.ValidateRLP(f.pool.ValidateSerializedTxn)

	return f
}

func (f *Fetch) threadSafeParsePooledTxn(cb func(*TxnParseContext) error) error {
	f.pooledTxnsParseCtxLock.Lock()
	defer f.pooledTxnsParseCtxLock.Unlock()
	return cb(f.pooledTxnsParseCtx)
}

func (f *Fetch) threadSafeParseStateChangeTxn(cb func(*TxnParseContext) error) error {
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

func (f *Fetch) receiveMessageLoop(sentryClient sentryproto.SentryClient) {
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

func (f *Fetch) receiveMessage(ctx context.Context, sentryClient sentryproto.SentryClient) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := sentryClient.Messages(streamCtx, &sentryproto.MessagesRequest{Ids: []sentryproto.MessageId{
		sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
		sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66,
		sentryproto.MessageId_TRANSACTIONS_66,
		sentryproto.MessageId_POOLED_TRANSACTIONS_66,
		sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68,
	}}, grpc.WaitForReady(true))
	if err != nil {
		select {
		case <-f.ctx.Done():
			return ctx.Err()
		default:
		}
		return err
	}
	var req *sentryproto.InboundMessage
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

func (f *Fetch) handleInboundMessage(ctx context.Context, req *sentryproto.InboundMessage, sentryClient sentryproto.SentryClient) (err error) {
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
	case sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_66:
		hashCount, pos, err := ParseHashesCount(req.Data, 0)
		if err != nil {
			return fmt.Errorf("parsing NewPooledTransactionHashes: %w", err)
		}

		const maxHashesPerMsg = 4096 // See https://github.com/ethereum/devp2p/blob/master/caps/eth.md#newpooledtransactionhashes-0x08
		if hashCount > maxHashesPerMsg {
			f.logger.Warn("Oversized hash announcement",
				"peer", req.PeerId, "count", hashCount)
			sentryClient.PenalizePeer(ctx, &sentryproto.PenalizePeerRequest{PeerId: req.PeerId, Penalty: sentryproto.PenaltyKind_Kick}) // Disconnect peer
			return nil
		}

		hashes := make([]byte, 32*hashCount)
		for i := 0; i < len(hashes); i += 32 {
			if _, pos, err = ParseHash(req.Data, pos, hashes[i:]); err != nil {
				return err
			}
		}
		unknownHashes, err := f.pool.FilterKnownIdHashes(tx, hashes)
		if err != nil {
			return err
		}
		if len(unknownHashes) > 0 {
			var encodedRequest []byte
			var messageID sentryproto.MessageId
			if encodedRequest, err = EncodeGetPooledTransactions66(unknownHashes, uint64(1), nil); err != nil {
				return err
			}
			messageID = sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66
			if _, err = sentryClient.SendMessageById(f.ctx, &sentryproto.SendMessageByIdRequest{
				Data:   &sentryproto.OutboundMessageData{Id: messageID, Data: encodedRequest},
				PeerId: req.PeerId,
			}, &grpc.EmptyCallOption{}); err != nil {
				return err
			}
		}
	case sentryproto.MessageId_NEW_POOLED_TRANSACTION_HASHES_68:
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
			var messageID sentryproto.MessageId
			if encodedRequest, err = EncodeGetPooledTransactions66(unknownHashes, uint64(1), nil); err != nil {
				return err
			}
			messageID = sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66
			if _, err = sentryClient.SendMessageById(f.ctx, &sentryproto.SendMessageByIdRequest{
				Data:   &sentryproto.OutboundMessageData{Id: messageID, Data: encodedRequest},
				PeerId: req.PeerId,
			}, &grpc.EmptyCallOption{}); err != nil {
				return err
			}
		}
	case sentryproto.MessageId_GET_POOLED_TRANSACTIONS_66:
		//TODO: handleInboundMessage is single-threaded - means it can accept as argument couple buffers (or analog of txParseContext). Protobuf encoding will copy data anyway, but DirectClient doesn't
		var encodedRequest []byte
		var messageID sentryproto.MessageId
		messageID = sentryproto.MessageId_POOLED_TRANSACTIONS_66
		requestID, hashes, _, err := ParseGetPooledTransactions66(req.Data, 0, nil)
		if err != nil {
			return err
		}

		// limit to max 256 transactions in a reply
		const hashSize = 32
		hashes = hashes[:min(len(hashes), 256*hashSize)]

		var txns [][]byte
		responseSize := 0
		processed := len(hashes)

		for i := 0; i < len(hashes); i += hashSize {
			if responseSize >= p2pTxPacketLimit {
				processed = i
				log.Trace("txpool.Fetch.handleInboundMessage PooledTransactions reply truncated to fit p2pTxPacketLimit", "requested", len(hashes), "processed", processed)
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

			txns = append(txns, txn)
			responseSize += len(txn)
		}

		encodedRequest = EncodePooledTransactions66(txns, requestID, nil)
		if len(encodedRequest) > p2pTxPacketLimit {
			log.Trace("txpool.Fetch.handleInboundMessage PooledTransactions reply exceeds p2pTxPacketLimit", "requested", len(hashes), "processed", processed)
		}

		if _, err := sentryClient.SendMessageById(f.ctx, &sentryproto.SendMessageByIdRequest{
			Data:   &sentryproto.OutboundMessageData{Id: messageID, Data: encodedRequest},
			PeerId: req.PeerId,
		}, &grpc.EmptyCallOption{}); err != nil {
			return err
		}
	case sentryproto.MessageId_POOLED_TRANSACTIONS_66, sentryproto.MessageId_TRANSACTIONS_66:
		txns := TxnSlots{}
		if err := f.threadSafeParsePooledTxn(func(parseContext *TxnParseContext) error {
			return nil
		}); err != nil {
			return err
		}

		switch req.Id {
		case sentryproto.MessageId_TRANSACTIONS_66:
			if err := f.threadSafeParsePooledTxn(func(parseContext *TxnParseContext) error {
				if _, err := ParseTransactions(req.Data, 0, parseContext, &txns, func(hash []byte) error {
					known, err := f.pool.IdHashKnown(tx, hash)
					if err != nil {
						return err
					}
					if known {
						return ErrRejected
					}
					return nil
				}); err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
		case sentryproto.MessageId_POOLED_TRANSACTIONS_66:
			if err := f.threadSafeParsePooledTxn(func(parseContext *TxnParseContext) error {
				if _, _, err := ParsePooledTransactions66(req.Data, 0, parseContext, &txns, func(hash []byte) error {
					known, err := f.pool.IdHashKnown(tx, hash)
					if err != nil {
						return err
					}
					if known {
						return ErrRejected
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
		if len(txns.Txns) == 0 {
			return nil
		}

		f.pool.AddRemoteTxns(ctx, txns)
	default:
		defer f.logger.Trace("[txpool] dropped p2p message", "id", req.Id)
	}

	return nil
}

func (f *Fetch) receivePeerLoop(sentryClient sentryproto.SentryClient) {
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

func (f *Fetch) receivePeer(sentryClient sentryproto.SentryClient) error {
	streamCtx, cancel := context.WithCancel(f.ctx)
	defer cancel()

	stream, err := sentryClient.PeerEvents(streamCtx, &sentryproto.PeerEventsRequest{})
	if err != nil {
		select {
		case <-f.ctx.Done():
			return f.ctx.Err()
		default:
		}
		return err
	}

	var req *sentryproto.PeerEvent
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

func (f *Fetch) handleNewPeer(req *sentryproto.PeerEvent) error {
	if req == nil {
		return nil
	}
	switch req.EventId {
	case sentryproto.PeerEvent_Connect:
		f.pool.AddNewGoodPeer(req.PeerId)
	}

	return nil
}

func (f *Fetch) handleStateChanges(ctx context.Context, client StateChangesClient) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.StateChanges(streamCtx, &remoteproto.StateChangeRequest{WithStorage: false, WithTransactions: true}, grpc.WaitForReady(true))
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

func (f *Fetch) handleStateChangesRequest(ctx context.Context, req *remoteproto.StateChangeBatch) error {
	var unwindTxns, unwindBlobTxns, minedTxns TxnSlots
	for _, change := range req.ChangeBatch {
		if change.Direction == remoteproto.Direction_FORWARD {
			minedTxns.Resize(uint(len(change.Txs)))
			for i := range change.Txs {
				minedTxns.Txns[i] = &TxnSlot{}
				if err := f.threadSafeParseStateChangeTxn(func(parseContext *TxnParseContext) error {
					_, err := parseContext.ParseTransaction(change.Txs[i], 0, minedTxns.Txns[i], minedTxns.Senders.At(i), false /* hasEnvelope */, false /* wrappedWithBlobs */, nil)
					return err
				}); err != nil && !errors.Is(err, context.Canceled) {
					f.logger.Warn("[txpool.fetch] stream.Recv", "err", err)
					continue // 1 txn handling error must not stop batch processing
				}
			}
		} else if change.Direction == remoteproto.Direction_UNWIND {
			for i := range change.Txs {
				if err := f.threadSafeParseStateChangeTxn(func(parseContext *TxnParseContext) error {
					utx := &TxnSlot{}
					sender := make([]byte, 20)
					_, err := parseContext.ParseTransaction(change.Txs[i], 0, utx, sender, false /* hasEnvelope */, false /* wrappedWithBlobs */, nil)
					if err != nil {
						return err
					}
					if utx.Type == BlobTxnType {
						unwindBlobTxns.Append(utx, sender, false)
					} else {
						unwindTxns.Append(utx, sender, false)
					}
					return nil
				}); err != nil && !errors.Is(err, context.Canceled) {
					f.logger.Warn("[txpool.fetch] stream.Recv", "err", err)
					continue // 1 txn handling error must not stop batch processing
				}
			}
		}
	}

	if err := f.pool.OnNewBlock(ctx, req, unwindTxns, unwindBlobTxns, minedTxns); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
