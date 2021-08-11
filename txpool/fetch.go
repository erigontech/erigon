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
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Fetch connects to sentry and implements eth/65 or eth/66 protocol regarding the transaction
// messages. It tries to "prime" the sentry with StatusData message containing given
// genesis hash and list of forks, but with zero max block and total difficulty
// Sentry should have a logic not to overwrite statusData with messages from tx pool
type Fetch struct {
	ctx                context.Context       // Context used for cancellation and closing of the fetcher
	sentryClients      []sentry.SentryClient // sentry clients that will be used for accessing the network
	statusData         *sentry.StatusData    // Status data used for "handshaking" with sentries
	pool               Pool                  // Transaction pool implementation
	coreDB             kv.RoDB
	wg                 *sync.WaitGroup // used for synchronisation in the tests (nil when not in tests)
	stateChangesClient remote.KVClient
}

type Timings struct {
	propagateAllNewTxsEvery         time.Duration
	syncToNewPeersEvery             time.Duration
	broadcastLocalTransactionsEvery time.Duration
}

var DefaultTimings = Timings{
	propagateAllNewTxsEvery:         5 * time.Second,
	broadcastLocalTransactionsEvery: 2 * time.Minute,
	syncToNewPeersEvery:             2 * time.Minute,
}

// NewFetch creates a new fetch object that will work with given sentry clients. Since the
// SentryClient here is an interface, it is suitable for mocking in tests (mock will need
// to implement all the functions of the SentryClient interface).
func NewFetch(ctx context.Context, sentryClients []sentry.SentryClient, genesisHash [32]byte, networkId uint64, forks []uint64, pool Pool, stateChangesClient remote.KVClient, db kv.RoDB) *Fetch {
	statusData := &sentry.StatusData{
		NetworkId:       networkId,
		TotalDifficulty: gointerfaces.ConvertUint256IntToH256(uint256.NewInt(0)),
		BestHash:        gointerfaces.ConvertHashToH256(genesisHash),
		MaxBlock:        0,
		ForkData: &sentry.Forks{
			Genesis: gointerfaces.ConvertHashToH256(genesisHash),
			Forks:   forks,
		},
	}
	return &Fetch{
		ctx:                ctx,
		sentryClients:      sentryClients,
		statusData:         statusData,
		pool:               pool,
		coreDB:             db,
		stateChangesClient: stateChangesClient,
	}
}

func (f *Fetch) SetWaitGroup(wg *sync.WaitGroup) {
	f.wg = wg
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
			f.handleStateChanges(f.ctx, f.stateChangesClient)
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
		_, err := sentryClient.SetStatus(f.ctx, f.statusData, grpc.WaitForReady(true))
		if err != nil {
			if s, ok := status.FromError(err); ok && s.Code() == codes.Canceled {
				return
			}
			// Report error and wait more
			log.Warn("sentry not ready yet", "err", err)
			time.Sleep(time.Second)
			continue
		}
		streamCtx, cancel := context.WithCancel(f.ctx)
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
				return
			default:
			}
			if s, ok := status.FromError(err); ok && s.Code() == codes.Canceled {
				return
			}
			if errors.Is(err, io.EOF) {
				return
			}
			log.Warn("messages", "err", err)
			return
		}

		var req *sentry.InboundMessage
		for req, err = stream.Recv(); ; req, err = stream.Recv() {
			if err != nil {
				select {
				case <-f.ctx.Done():
					return
				default:
				}
				if s, ok := status.FromError(err); ok && s.Code() == codes.Canceled {
					return
				}
				if errors.Is(err, io.EOF) {
					return
				}
				log.Warn("stream.Recv", "err", err)
				return
			}
			if req == nil {
				return
			}
			if err = f.handleInboundMessage(streamCtx, req, sentryClient); err != nil {
				log.Warn("Handling incoming message: %s", "err", err)
			}
			if f.wg != nil {
				f.wg.Done()
			}
		}
	}
}

func (f *Fetch) handleInboundMessage(ctx context.Context, req *sentry.InboundMessage, sentryClient sentry.SentryClient) error {
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
			if !f.pool.IdHashKnown(hashbuf[:]) {
				unknownHashes = append(unknownHashes, hashbuf[:]...)
			}
		}
		if len(unknownHashes) > 0 {
			var encodedRequest []byte
			var messageId sentry.MessageId
			if req.Id == sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66 {
				if encodedRequest, err = EncodeGetPooledTransactions66(unknownHashes, uint64(1), nil); err != nil {
					return err
				}
				messageId = sentry.MessageId_GET_POOLED_TRANSACTIONS_66
			} else {
				encodedRequest = EncodeHashes(unknownHashes, nil)
				messageId = sentry.MessageId_GET_POOLED_TRANSACTIONS_65
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
		messageId := sentry.MessageId_POOLED_TRANSACTIONS_66
		if req.Id == sentry.MessageId_GET_POOLED_TRANSACTIONS_65 {
			messageId = sentry.MessageId_POOLED_TRANSACTIONS_65
		}
		if req.Id == sentry.MessageId_GET_POOLED_TRANSACTIONS_66 {
			requestID, hashes, _, err := ParseGetPooledTransactions66(req.Data, 0, nil)
			if err != nil {
				return err
			}
			_ = requestID
			var txs [][]byte
			for i := 0; i < len(hashes); i += 32 {
				txn := f.pool.GetRlp(hashes[i : i+32])
				if txn == nil {
					continue
				}
				txs = append(txs, txn)
			}

			encodedRequest = EncodePooledTransactions66(txs, requestID, nil)
		} else {
			hashes, _, err := ParseGetPooledTransactions65(req.Data, 0, nil)
			if err != nil {
				return err
			}
			var txs [][]byte
			for i := 0; i < len(hashes); i += 32 {
				txn := f.pool.GetRlp(hashes[i : i+32])
				if txn == nil {
					continue
				}
				txs = append(txs, txn)
			}
			encodedRequest = EncodePooledTransactions65(txs, nil)
		}
		if _, err := sentryClient.SendMessageById(f.ctx, &sentry.SendMessageByIdRequest{
			Data:   &sentry.OutboundMessageData{Id: messageId, Data: encodedRequest},
			PeerId: req.PeerId,
		}, &grpc.EmptyCallOption{}); err != nil {
			return err
		}
	case sentry.MessageId_POOLED_TRANSACTIONS_65, sentry.MessageId_POOLED_TRANSACTIONS_66:
		parseCtx := NewTxParseContext()
		txs := TxSlots{}
		if req.Id == sentry.MessageId_GET_POOLED_TRANSACTIONS_66 {
			if _, err := ParsePooledTransactions65(req.Data, 0, parseCtx, &txs); err != nil {
				return err
			}
		} else {
			if _, _, err := ParsePooledTransactions66(req.Data, 0, parseCtx, &txs); err != nil {
				return err
			}
		}
		if err := f.coreDB.View(ctx, func(tx kv.Tx) error {
			return f.pool.Add(tx, txs)
		}); err != nil {
			return err
		}
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
		_, err := sentryClient.SetStatus(f.ctx, f.statusData, grpc.WaitForReady(true))
		if err != nil {
			if s, ok := status.FromError(err); ok && s.Code() == codes.Canceled {
				return
			}
			// Report error and wait more
			log.Warn("sentry not ready yet", "err", err)
			time.Sleep(time.Second)
			continue
		}
		streamCtx, cancel := context.WithCancel(f.ctx)
		defer cancel()

		stream, err := sentryClient.Peers(streamCtx, &sentry.PeersRequest{})
		if err != nil {
			select {
			case <-f.ctx.Done():
				return
			default:
			}
			if s, ok := status.FromError(err); ok && s.Code() == codes.Canceled {
				return
			}
			if errors.Is(err, io.EOF) {
				return
			}
			log.Warn("peers", "err", err)
			return
		}

		var req *sentry.PeersReply
		for req, err = stream.Recv(); ; req, err = stream.Recv() {
			if err != nil {
				select {
				case <-f.ctx.Done():
					return
				default:
				}
				if s, ok := status.FromError(err); ok && s.Code() == codes.Canceled {
					return
				}
				if errors.Is(err, io.EOF) {
					return
				}
				log.Warn("stream.Recv", "err", err)
				return
			}
			if req == nil {
				return
			}
			if err = f.handleNewPeer(req); err != nil {
				log.Warn("Handling new peer", "err", err)
			}
			if f.wg != nil {
				f.wg.Done()
			}
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

func (f *Fetch) handleStateChanges(ctx context.Context, client remote.KVClient) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.StateChanges(streamCtx, &remote.StateChangeRequest{WithStorage: false, WithTransactions: true}, grpc.WaitForReady(true))
	if err != nil {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if s, ok := status.FromError(err); ok && s.Code() == codes.Canceled {
			return
		}
		if errors.Is(err, io.EOF) {
			return
		}
		time.Sleep(time.Second)
		log.Warn("state changes", "err", err)
	}
	for req, err := stream.Recv(); ; req, err = stream.Recv() {
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if s, ok := status.FromError(err); ok && s.Code() == codes.Canceled {
				return
			}
			if errors.Is(err, io.EOF) {
				return
			}
			log.Warn("stream.Recv", "err", err)
			return
		}
		if req == nil {
			return
		}

		parseCtx := NewTxParseContext()
		var unwindTxs, minedTxs TxSlots
		if req.Direction == remote.Direction_FORWARD {
			minedTxs.Growth(len(req.Txs))
			for i := range req.Txs {
				minedTxs.txs[i] = &TxSlot{}
				if _, err := parseCtx.ParseTransaction(req.Txs[i], 0, minedTxs.txs[i], minedTxs.senders.At(i)); err != nil {
					log.Warn("stream.Recv", "err", err)
					continue
				}
			}
		}
		if req.Direction == remote.Direction_UNWIND {
			unwindTxs.Growth(len(req.Txs))
			for i := range req.Txs {
				unwindTxs.txs[i] = &TxSlot{}
				if _, err := parseCtx.ParseTransaction(req.Txs[i], 0, unwindTxs.txs[i], unwindTxs.senders.At(i)); err != nil {
					log.Warn("stream.Recv", "err", err)
					continue
				}
			}
		}
		diff := map[string]senderInfo{}
		for _, change := range req.Changes {
			nonce, balance, err := DecodeSender(change.Data)
			if err != nil {
				log.Warn("stateChanges.decodeSender", "err", err)
				continue
			}
			addr := gointerfaces.ConvertH160toAddress(change.Address)
			diff[string(addr[:])] = senderInfo{nonce: nonce, balance: balance}
		}

		if err := f.coreDB.View(ctx, func(tx kv.Tx) error {
			return f.pool.OnNewBlock(tx, diff, unwindTxs, minedTxs, req.ProtocolBaseFee, req.BlockBaseFee, req.BlockHeight)
		}); err != nil {
			log.Warn("onNewBlock", "err", err)
		}
		if f.wg != nil {
			f.wg.Done()
		}
	}
}
