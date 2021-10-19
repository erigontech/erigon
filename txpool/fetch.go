/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package txpool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// Fetch connects to sentry and implements eth/65 or eth/66 protocol regarding the transaction
// messages. It tries to "prime" the sentry with StatusData message containing given
// genesis hash and list of forks, but with zero max block and total difficulty
// Sentry should have a logic not to overwrite statusData with messages from tx pool
type Fetch struct {
	ctx                  context.Context       // Context used for cancellation and closing of the fetcher
	sentryClients        []direct.SentryClient // sentry clients that will be used for accessing the network
	pool                 Pool                  // Transaction pool implementation
	coreDB               kv.RoDB
	db                   kv.RwDB
	wg                   *sync.WaitGroup // used for synchronisation in the tests (nil when not in tests)
	stateChangesClient   StateChangesClient
	stateChangesParseCtx *TxParseContext
	pooledTxsParseCtx    *TxParseContext
}

type StateChangesClient interface {
	StateChanges(ctx context.Context, in *remote.StateChangeRequest, opts ...grpc.CallOption) (remote.KV_StateChangesClient, error)
}

// NewFetch creates a new fetch object that will work with given sentry clients. Since the
// SentryClient here is an interface, it is suitable for mocking in tests (mock will need
// to implement all the functions of the SentryClient interface).
func NewFetch(ctx context.Context, sentryClients []direct.SentryClient, pool Pool, stateChangesClient StateChangesClient, coreDB kv.RoDB, db kv.RwDB, rules chain.Rules, chainID uint256.Int) *Fetch {
	return &Fetch{
		ctx:                  ctx,
		sentryClients:        sentryClients,
		pool:                 pool,
		coreDB:               coreDB,
		db:                   db,
		stateChangesClient:   stateChangesClient,
		stateChangesParseCtx: NewTxParseContext(chainID), //TODO: change ctx if rules changed
		pooledTxsParseCtx:    NewTxParseContext(chainID),
	}
}

func (f *Fetch) SetWaitGroup(wg *sync.WaitGroup) {
	f.wg = wg
}

// ConnectSentries initialises connection to the sentry
func (f *Fetch) ConnectSentries() {
	//TODO: fix race in parse ctx - 2 sentries causing it
	go func(i int) {
		f.receiveMessageLoop(f.sentryClients[i])
	}(0)
	go func(i int) {
		f.receivePeerLoop(f.sentryClients[i])
	}(0)
	/*
		for i := range f.sentryClients {
			go func(i int) {
				f.receiveMessageLoop(f.sentryClients[i])
			}(i)
			go func(i int) {
				f.receivePeerLoop(f.sentryClients[i])
			}(i)
		}
	*/
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
				if s, ok := status.FromError(err); ok && retryLater(s.Code()) {
					time.Sleep(time.Second)
					continue
				}
				if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
					continue
				}
				log.Warn("[txpool.handleStateChanges]", "err", err)
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
			if s, ok := status.FromError(err); ok && retryLater(s.Code()) {
				time.Sleep(time.Second)
				continue
			}
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				continue
			}
			// Report error and wait more
			log.Warn("[txpool.recvMessage] sentry not ready yet", "err", err)
			continue
		}

		if err := f.receiveMessage(f.ctx, sentryClient); err != nil {
			if s, ok := status.FromError(err); ok && retryLater(s.Code()) {
				time.Sleep(time.Second)
				continue
			}
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				continue
			}
			log.Warn("[txpool.recvMessage]", "err", err)
		}
	}
}

func (f *Fetch) receiveMessage(ctx context.Context, sentryClient sentry.SentryClient) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := sentryClient.Messages(streamCtx, &sentry.MessagesRequest{Ids: []sentry.MessageId{
		sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_65,
		sentry.MessageId_GET_POOLED_TRANSACTIONS_65,
		sentry.MessageId_TRANSACTIONS_65,
		sentry.MessageId_POOLED_TRANSACTIONS_65,
		sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
		sentry.MessageId_GET_POOLED_TRANSACTIONS_66,
		sentry.MessageId_TRANSACTIONS_66,
		sentry.MessageId_POOLED_TRANSACTIONS_66,
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
			return err
		}
		if req == nil {
			return nil
		}
		if err := f.handleInboundMessage(streamCtx, req, sentryClient); err != nil {
			if s, ok := status.FromError(err); ok && retryLater(s.Code()) {
				time.Sleep(time.Second)
				continue
			}
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				continue
			}
			log.Warn("[txpool.fetch] Handling incoming message", "msg", req.Id.String(), "err", err, "rlp", fmt.Sprintf("%x", req.Data))
		}
		if f.wg != nil {
			f.wg.Done()
		}
	}
}

func (f *Fetch) handleInboundMessage(ctx context.Context, req *sentry.InboundMessage, sentryClient sentry.SentryClient) error {
	if !f.pool.Started() {
		return nil
	}
	tx, err := f.db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	switch req.Id {
	case sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66, sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_65:
		hashCount, pos, err := ParseHashesCount(req.Data, 0)
		if err != nil {
			return fmt.Errorf("parsing NewPooledTransactionHashes: %w", err)
		}
		var hashbuf [32]byte
		var unknownHashes Hashes
		for i := 0; i < hashCount; i++ {
			_, pos, err = ParseHash(req.Data, pos, hashbuf[:0])
			if err != nil {
				return fmt.Errorf("parsing NewPooledTransactionHashes: %w", err)
			}
			known, err := f.pool.IdHashKnown(tx, hashbuf[:])
			if err != nil {
				return err
			}
			if !known {
				unknownHashes = append(unknownHashes, hashbuf[:]...)
			}
		}
		if len(unknownHashes) > 0 {
			var encodedRequest []byte
			var messageId sentry.MessageId
			switch req.Id {
			case sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66:
				if encodedRequest, err = EncodeGetPooledTransactions66(unknownHashes, uint64(1), nil); err != nil {
					return err
				}
				messageId = sentry.MessageId_GET_POOLED_TRANSACTIONS_66
			case sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_65:
				encodedRequest = EncodeHashes(unknownHashes, nil)
				messageId = sentry.MessageId_GET_POOLED_TRANSACTIONS_65
			default:
				return fmt.Errorf("unexpected message: %s", req.Id.String())
			}
			if _, err = sentryClient.SendMessageById(f.ctx, &sentry.SendMessageByIdRequest{
				Data:   &sentry.OutboundMessageData{Id: messageId, Data: encodedRequest},
				PeerId: req.PeerId,
			}, &grpc.EmptyCallOption{}); err != nil {
				return err
			}
		}
	case sentry.MessageId_GET_POOLED_TRANSACTIONS_66, sentry.MessageId_GET_POOLED_TRANSACTIONS_65:
		//TODO: handleInboundMessage is single-threaded - means it can accept as argument couple buffers (or analog of txParseContext). Protobuf encoding will copy data anyway, but DirectClient doesn't
		var encodedRequest []byte
		var messageId sentry.MessageId
		switch req.Id {
		case sentry.MessageId_GET_POOLED_TRANSACTIONS_66:
			messageId = sentry.MessageId_POOLED_TRANSACTIONS_66
			requestID, hashes, _, err := ParseGetPooledTransactions66(req.Data, 0, nil)
			if err != nil {
				return err
			}
			_ = requestID
			var txs [][]byte
			for i := 0; i < len(hashes); i += 32 {
				txn, err := f.pool.GetRlp(tx, hashes[i:i+32])
				if err != nil {
					return err
				}
				if txn == nil {
					continue
				}
				txs = append(txs, txn)
			}

			encodedRequest = EncodePooledTransactions66(txs, requestID, nil)
		case sentry.MessageId_GET_POOLED_TRANSACTIONS_65:
			messageId = sentry.MessageId_POOLED_TRANSACTIONS_65
			hashes, _, err := ParseGetPooledTransactions65(req.Data, 0, nil)
			if err != nil {
				return err
			}
			var txs [][]byte
			for i := 0; i < len(hashes); i += 32 {
				txn, err := f.pool.GetRlp(tx, hashes[i:i+32])
				if err != nil {
					return err
				}
				if txn == nil {
					continue
				}
				txs = append(txs, txn)
			}
			encodedRequest = EncodePooledTransactions65(txs, nil)
		default:
			return fmt.Errorf("unexpected message: %s", req.Id.String())
		}

		if _, err := sentryClient.SendMessageById(f.ctx, &sentry.SendMessageByIdRequest{
			Data:   &sentry.OutboundMessageData{Id: messageId, Data: encodedRequest},
			PeerId: req.PeerId,
		}, &grpc.EmptyCallOption{}); err != nil {
			return err
		}
	case sentry.MessageId_POOLED_TRANSACTIONS_65, sentry.MessageId_POOLED_TRANSACTIONS_66, sentry.MessageId_TRANSACTIONS_65, sentry.MessageId_TRANSACTIONS_66:
		txs := TxSlots{}
		f.pooledTxsParseCtx.Reject(func(hash []byte) error {
			known, err := f.pool.IdHashKnown(tx, hash)
			if err != nil {
				return err
			}
			if known {
				return ErrRejected
			}
			return nil
		})
		switch req.Id {
		case sentry.MessageId_POOLED_TRANSACTIONS_65, sentry.MessageId_TRANSACTIONS_65, sentry.MessageId_TRANSACTIONS_66:
			if _, err := ParsePooledTransactions65(req.Data, 0, f.pooledTxsParseCtx, &txs); err != nil {
				return err
			}
		case sentry.MessageId_POOLED_TRANSACTIONS_66:
			if _, _, err := ParsePooledTransactions66(req.Data, 0, f.pooledTxsParseCtx, &txs); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unexpected message: %s", req.Id.String())
		}
		if len(txs.txs) == 0 {
			return nil
		}
		f.pool.AddRemoteTxs(ctx, txs)
	default:
		defer log.Trace("[txpool] dropped p2p message", "id", req.Id)
	}

	return nil
}

func retryLater(code codes.Code) bool {
	return code == codes.Unavailable || code == codes.Canceled || code == codes.ResourceExhausted
}

func (f *Fetch) receivePeerLoop(sentryClient sentry.SentryClient) {
	for {
		select {
		case <-f.ctx.Done():
			return
		default:
		}
		if _, err := sentryClient.HandShake(f.ctx, &emptypb.Empty{}, grpc.WaitForReady(true)); err != nil {
			if s, ok := status.FromError(err); ok && retryLater(s.Code()) {
				time.Sleep(time.Second)
				continue
			}
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				continue
			}
			// Report error and wait more
			log.Warn("[txpool.recvPeers] sentry not ready yet", "err", err)
			time.Sleep(time.Second)
			continue
		}
		if err := f.receivePeer(sentryClient); err != nil {
			if s, ok := status.FromError(err); ok && retryLater(s.Code()) {
				time.Sleep(time.Second)
				continue
			}
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				continue
			}

			log.Warn("[txpool.recvPeers]", "err", err)
		}
	}
}

func (f *Fetch) receivePeer(sentryClient sentry.SentryClient) error {
	streamCtx, cancel := context.WithCancel(f.ctx)
	defer cancel()

	stream, err := sentryClient.Peers(streamCtx, &sentry.PeersRequest{})
	if err != nil {
		select {
		case <-f.ctx.Done():
			return f.ctx.Err()
		default:
		}
		return err
	}

	var req *sentry.PeersReply
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

func (f *Fetch) handleNewPeer(req *sentry.PeersReply) error {
	if req == nil {
		return nil
	}
	switch req.Event {
	case sentry.PeersReply_Connect:
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

		var unwindTxs, minedTxs TxSlots
		for _, change := range req.ChangeBatch {
			if change.Direction == remote.Direction_FORWARD {
				minedTxs.Resize(uint(len(change.Txs)))
				for i := range change.Txs {
					minedTxs.txs[i] = &TxSlot{}
					if _, err := f.stateChangesParseCtx.ParseTransaction(change.Txs[i], 0, minedTxs.txs[i], minedTxs.senders.At(i)); err != nil {
						log.Warn("stream.Recv", "err", err)
						continue
					}
				}
			}
			if change.Direction == remote.Direction_UNWIND {
				unwindTxs.Resize(uint(len(change.Txs)))
				for i := range change.Txs {
					unwindTxs.txs[i] = &TxSlot{}
					if _, err := f.stateChangesParseCtx.ParseTransaction(change.Txs[i], 0, unwindTxs.txs[i], unwindTxs.senders.At(i)); err != nil {
						log.Warn("stream.Recv", "err", err)
						continue
					}
				}
			}
		}
		if err := f.db.View(ctx, func(tx kv.Tx) error {
			return f.pool.OnNewBlock(ctx, req, unwindTxs, minedTxs, tx)
		}); err != nil {
			log.Warn("onNewBlock", "err", err)
		}
		if f.wg != nil {
			f.wg.Done()
		}
	}
}
