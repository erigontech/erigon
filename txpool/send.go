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
	"sync"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
)

type SentryClient interface {
	sentry.SentryClient
	Protocol() uint
}

// Send - does send concrete P2P messages to Sentry. Same as Fetch but for outbound traffic
// does not initiate any messages by self
type Send struct {
	ctx           context.Context
	sentryClients []direct.SentryClient // sentry clients that will be used for accessing the network
	pool          Pool

	wg *sync.WaitGroup
}

func NewSend(ctx context.Context, sentryClients []direct.SentryClient, pool Pool) *Send {
	return &Send{
		ctx:           ctx,
		pool:          pool,
		sentryClients: sentryClients,
	}
}

func (f *Send) SetWaitGroup(wg *sync.WaitGroup) {
	f.wg = wg
}

const (
	// This is the target size for the packs of transactions or announcements. A
	// pack can get larger than this if a single transactions exceeds this size.
	p2pTxPacketLimit = 100 * 1024
)

func (f *Send) notifyTests() {
	if f.wg != nil {
		f.wg.Done()
	}
}

func (f *Send) BroadcastLocalPooledTxs(txs Hashes) (sentToPeers int) {
	defer f.notifyTests()
	if len(txs) == 0 {
		return
	}

	avgPeersPerSent65 := 0
	avgPeersPerSent66 := 0
	for len(txs) > 0 {
		var pending Hashes
		if len(txs) > p2pTxPacketLimit {
			pending = txs[:p2pTxPacketLimit]
			txs = txs[p2pTxPacketLimit:]
		} else {
			pending = txs[:]
			txs = txs[:0]
		}

		data := EncodeHashes(pending, nil)
		var req66, req65 *sentry.OutboundMessageData
		for _, sentryClient := range f.sentryClients {
			if !sentryClient.Ready() {
				continue
			}
			switch sentryClient.Protocol() {
			case direct.ETH65:
				if req65 == nil {
					req65 = &sentry.OutboundMessageData{
						Id:   sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_65,
						Data: data,
					}
				}

				peers, err := sentryClient.SendMessageToAll(f.ctx, req65, &grpc.EmptyCallOption{})
				if err != nil {
					log.Warn("[txpool.send] BroadcastLocalPooledTxs", "err", err)
				} else {
					avgPeersPerSent65 += len(peers.Peers)
				}
			case direct.ETH66:
				if req66 == nil {
					req66 = &sentry.OutboundMessageData{
						Id:   sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
						Data: data,
					}
				}
				peers, err := sentryClient.SendMessageToAll(f.ctx, req66, &grpc.EmptyCallOption{})
				if err != nil {
					log.Warn("[txpool.send] BroadcastLocalPooledTxs", "err", err)
				} else {
					avgPeersPerSent66 += len(peers.Peers)
				}
			}
		}
	}
	return avgPeersPerSent65 + avgPeersPerSent66
}

func (f *Send) BroadcastRemotePooledTxs(txs Hashes) {
	defer f.notifyTests()

	if len(txs) == 0 {
		return
	}

	for len(txs) > 0 {
		var pending Hashes
		if len(txs) > p2pTxPacketLimit {
			pending = txs[:p2pTxPacketLimit]
			txs = txs[p2pTxPacketLimit:]
		} else {
			pending = txs[:]
			txs = txs[:0]
		}

		data := EncodeHashes(pending, nil)
		var req66, req65 *sentry.SendMessageToRandomPeersRequest
		for _, sentryClient := range f.sentryClients {
			if !sentryClient.Ready() {
				continue
			}

			switch sentryClient.Protocol() {
			case direct.ETH65:
				if req65 == nil {
					req65 = &sentry.SendMessageToRandomPeersRequest{
						MaxPeers: 1024,
						Data: &sentry.OutboundMessageData{
							Id:   sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_65,
							Data: data,
						},
					}
				}

				if _, err := sentryClient.SendMessageToRandomPeers(f.ctx, req65, &grpc.EmptyCallOption{}); err != nil {
					log.Warn("[txpool.send] BroadcastRemotePooledTxs", "err", err)
				}

			case direct.ETH66:
				if req66 == nil {
					req66 = &sentry.SendMessageToRandomPeersRequest{
						MaxPeers: 1024,
						Data: &sentry.OutboundMessageData{
							Id:   sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
							Data: data,
						},
					}
				}
				if _, err := sentryClient.SendMessageToRandomPeers(f.ctx, req66, &grpc.EmptyCallOption{}); err != nil {
					log.Warn("[txpool.send] BroadcastRemotePooledTxs", "err", err)
				}
			}
		}
	}
}

func (f *Send) PropagatePooledTxsToPeersList(peers []PeerID, txs []byte) {
	defer f.notifyTests()

	if len(txs) == 0 {
		return
	}

	for len(txs) > 0 {
		var pending Hashes
		if len(txs) > p2pTxPacketLimit {
			pending = txs[:p2pTxPacketLimit]
			txs = txs[p2pTxPacketLimit:]
		} else {
			pending = txs
			txs = txs[:0]
		}

		data := EncodeHashes(pending, nil)
		for _, sentryClient := range f.sentryClients {
			if !sentryClient.Ready() {
				continue
			}

			for _, peer := range peers {
				switch sentryClient.Protocol() {
				case direct.ETH65:
					req65 := &sentry.SendMessageByIdRequest{
						PeerId: peer,
						Data: &sentry.OutboundMessageData{
							Id:   sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_65,
							Data: data,
						},
					}

					if _, err := sentryClient.SendMessageById(f.ctx, req65, &grpc.EmptyCallOption{}); err != nil {
						log.Warn("[txpool.send] PropagatePooledTxsToPeersList", "err", err)
					}

				case direct.ETH66:
					req66 := &sentry.SendMessageByIdRequest{
						PeerId: peer,
						Data: &sentry.OutboundMessageData{
							Id:   sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
							Data: data,
						},
					}
					if _, err := sentryClient.SendMessageById(f.ctx, req66, &grpc.EmptyCallOption{}); err != nil {
						log.Warn("[txpool.send] PropagatePooledTxsToPeersList", "err", err)
					}
				}
			}
		}
	}
}
