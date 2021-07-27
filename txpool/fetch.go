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
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/log"
	"github.com/ledgerwatch/erigon-lib/rlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Fetch connects to sentry and impements eth/65 or eth/66 protocol regarding the transaction
// messages. It tries to "prime" the sentry with StatusData message containing given
// genesis hash and list of forks, but with zero max block and total difficulty
// Sentry should have a logic not to overwrite statusData with messages from tx pool
type Fetch struct {
	ctx           context.Context       // Context used for cancellation and closing of the fetcher
	sentryClients []sentry.SentryClient // sentry clients that will be used for accessing the network
	statusData    *sentry.StatusData    // Status data used for "handshaking" with sentries
	pool          Pool                  // Transaction pool implementation
	wg            *sync.WaitGroup       // Waitgroup used for synchronisation in the tests (nil when not in tests)
	logger        log.Logger
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
func NewFetch(ctx context.Context,
	sentryClients []sentry.SentryClient,
	genesisHash [32]byte,
	networkId uint64,
	forks []uint64,
	pool Pool,
	logger log.Logger,
) *Fetch {
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
		ctx:           ctx,
		sentryClients: sentryClients,
		statusData:    statusData,
		pool:          pool,
		logger:        logger.Named("[TxPool.Fetch]"),
	}
}

func (f *Fetch) SetWaitGroup(wg *sync.WaitGroup) {
	f.wg = wg
}

// Start initialises connection to the sentry
func (f *Fetch) Start() {
	for i := range f.sentryClients {
		go func(i int) {
			f.receiveMessageLoop(f.sentryClients[i])
		}(i)
		go func(i int) {
			f.receivePeerLoop(f.sentryClients[i])
		}(i)
	}
}

func (f *Fetch) receiveMessageLoop(sentryClient sentry.SentryClient) {
	logger := f.logger.Named("receiveMessageLoop")
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
			logger.Warnf("sentry not ready yet: %s", err)
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
			logger.Warnf("messages: %s", err)
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
				logger.Warnf("stream.Recv: %s", err)
				return
			}
			if req == nil {
				return
			}
			if err = f.handleInboundMessage(req, sentryClient); err != nil {
				logger.Warnf("Handling incoming message: %s", err)
			}
			if f.wg != nil {
				f.wg.Done()
			}
		}
	}
}

func (f *Fetch) handleInboundMessage(req *sentry.InboundMessage, sentryClient sentry.SentryClient) error {
	switch req.Id {
	case sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66, sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_65:
		hashCount, pos, err := ParseHashesCount(req.Data, 0)
		if err != nil {
			return fmt.Errorf("parsing NewPooledTransactionHashes: %w", err)
		}
		var hashbuf [32]byte
		var unknownHashes Hashes
		for i := 0; i < hashCount; i++ {
			_, pos, err = rlp.ParseHash(req.Data, pos, hashbuf[:0])
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
				if encodedRequest, err = EncodeHashes(unknownHashes, nil); err != nil {
					return err
				}
				messageId = sentry.MessageId_GET_POOLED_TRANSACTIONS_65
			}
			if _, err = sentryClient.SendMessageById(f.ctx, &sentry.SendMessageByIdRequest{
				Data:   &sentry.OutboundMessageData{Id: messageId, Data: encodedRequest},
				PeerId: req.PeerId,
			}, &grpc.EmptyCallOption{}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (f *Fetch) receivePeerLoop(sentryClient sentry.SentryClient) {
	logger := f.logger.Named("receivePeerLoop")
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
			logger.Warnf("sentry not ready yet: %s", err)
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
			logger.Warnf("peers: %s", err)
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
				logger.Warnf("stream.Recv: %s", err)
				return
			}
			if req == nil {
				return
			}
			if err = f.handleNewPeer(req); err != nil {
				logger.Warnf("Handling new peer: %s", err)
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
		f.pool.NotifyNewPeer(req.PeerId)
	}

	return nil
}
