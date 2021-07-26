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
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Send - does send concrete P2P messages to Sentry. Same as Fetch but for outbound traffic
// does not initiate any messages by self
type Send struct {
	ctx           context.Context
	sentryClients []sentry.SentryClient // sentry clients that will be used for accessing the network
	pool          Pool

	logger *zap.SugaredLogger
	wg     *sync.WaitGroup
}

func NewSend(ctx context.Context, sentryClients []sentry.SentryClient, pool Pool, logger *zap.SugaredLogger) *Send {
	return &Send{
		ctx:           ctx,
		pool:          pool,
		sentryClients: sentryClients,
		logger:        logger.Named("txpool.send"),
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

func (f *Send) BroadcastLocalPooledTxs(txs Hashes) {
	defer f.notifyTests()
	if len(txs) == 0 {
		return
	}

	initialAmount := len(txs)
	avgPeersPerSent65 := 0
	avgPeersPerSent66 := 0
	initialTxs := txs
	for len(txs) > 0 {
		var pending Hashes
		if len(txs) > p2pTxPacketLimit {
			pending = txs[:p2pTxPacketLimit]
			txs = txs[p2pTxPacketLimit:]
		} else {
			pending = txs[:]
			txs = txs[:0]
		}

		data, err := EncodeHashes(pending, len(pending)/32, nil)
		if err != nil {
			f.logger.Warn(err)
			return
		}
		var req66, req65 *sentry.OutboundMessageData
		for _, sentryClient := range f.sentryClients {
			//if !sentryClient.Ready() {
			//	continue
			//}
			//protocol:=sentryClient.Protocol()
			protocol := direct.ETH66
			switch protocol {
			case direct.ETH65:
				if req65 == nil {
					req65 = &sentry.OutboundMessageData{
						Id:   sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_65,
						Data: data,
					}
				}

				peers, err := sentryClient.SendMessageToAll(f.ctx, req65, &grpc.EmptyCallOption{})
				if err != nil {
					f.logger.Warn(err)
				}
				avgPeersPerSent65 += len(peers.Peers)

			case direct.ETH66:
				if req66 == nil {
					req66 = &sentry.OutboundMessageData{
						Id:   sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
						Data: data,
					}
				}
				peers, err := sentryClient.SendMessageToAll(f.ctx, req66, &grpc.EmptyCallOption{})
				if err != nil {
					f.logger.Warn(err)
					return
				}
				avgPeersPerSent66 += len(peers.Peers)
			}
		}
	}
	if initialAmount == 1 {
		f.logger.Infof("local tx %x, propageted to %d peers", initialTxs, avgPeersPerSent65+avgPeersPerSent66)
	} else {
		f.logger.Infof("%d local txs propagated to %d peers", initialAmount, avgPeersPerSent65+avgPeersPerSent66)
	}
	return
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

		data, err := EncodeHashes(pending, len(pending)/32, nil)
		if err != nil {
			f.logger.Warn(err)
			return
		}
		var req66, req65 *sentry.SendMessageToRandomPeersRequest
		for _, sentryClient := range f.sentryClients {
			//if !sentryClient.Ready() {
			//	continue
			//}

			//protocol:=sentryClient.Protocol()
			protocol := direct.ETH66
			switch protocol {
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

				if _, err = sentryClient.SendMessageToRandomPeers(f.ctx, req65, &grpc.EmptyCallOption{}); err != nil {
					f.logger.Warn(err)
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
				if _, err = sentryClient.SendMessageToRandomPeers(f.ctx, req66, &grpc.EmptyCallOption{}); err != nil {
					f.logger.Warn(err)
				}
			}
		}
	}
	return
}

func (f *Send) PropagatePooledTxsToPeersList(peers []*types.H512, txs []byte) {
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

		data, err := EncodeHashes(pending, len(pending)/32, nil)
		if err != nil {
			f.logger.Warn(err)
			return
		}
		for _, sentryClient := range f.sentryClients {
			//if !sentryClient.Ready() {
			//	continue
			//}

			for _, peer := range peers {
				//protocol:=sentryClient.Protocol()
				protocol := direct.ETH66
				switch protocol {
				case direct.ETH65:
					req65 := &sentry.SendMessageByIdRequest{
						PeerId: peer,
						Data: &sentry.OutboundMessageData{
							Id:   sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_65,
							Data: data,
						},
					}

					if _, err = sentryClient.SendMessageById(f.ctx, req65, &grpc.EmptyCallOption{}); err != nil {
						f.logger.Warn(err)
					}

				case direct.ETH66:
					req66 := &sentry.SendMessageByIdRequest{
						PeerId: peer,
						Data: &sentry.OutboundMessageData{
							Id:   sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
							Data: data,
						},
					}
					if _, err = sentryClient.SendMessageById(f.ctx, req66, &grpc.EmptyCallOption{}); err != nil {
						f.logger.Warn(err)
					}
				}
			}
		}
	}
	return
}
