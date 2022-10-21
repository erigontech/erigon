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
	types2 "github.com/ledgerwatch/erigon-lib/types"
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
	pool          Pool
	wg            *sync.WaitGroup
	sentryClients []direct.SentryClient // sentry clients that will be used for accessing the network
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

func (f *Send) BroadcastPooledTxs(rlps [][]byte) (txSentTo []int) {
	defer f.notifyTests()
	if len(rlps) == 0 {
		return
	}
	txSentTo = make([]int, len(rlps))
	var prev, size int
	for i, l := 0, len(rlps); i < len(rlps); i++ {
		size += len(rlps[i])
		if i == l-1 || size >= p2pTxPacketLimit {
			txsData := types2.EncodeTransactions(rlps[prev:i+1], nil)
			var txs66 *sentry.SendMessageToRandomPeersRequest
			for _, sentryClient := range f.sentryClients {
				if !sentryClient.Ready() {
					continue
				}
				switch sentryClient.Protocol() {
				case direct.ETH66, direct.ETH67:
					if txs66 == nil {
						txs66 = &sentry.SendMessageToRandomPeersRequest{
							Data: &sentry.OutboundMessageData{
								Id:   sentry.MessageId_TRANSACTIONS_66,
								Data: txsData,
							},
							MaxPeers: 100,
						}
					}
					peers, err := sentryClient.SendMessageToRandomPeers(f.ctx, txs66)
					if err != nil {
						log.Debug("[txpool.send] BroadcastPooledTxs", "err", err)
					}
					if peers != nil {
						for j := prev; j <= i; j++ {
							txSentTo[j] = len(peers.Peers)
						}
					}
				}
			}
			prev = i + 1
			size = 0
		}
	}
	return
}

func (f *Send) AnnouncePooledTxs(hashes types2.Hashes) (hashSentTo []int) {
	defer f.notifyTests()
	hashSentTo = make([]int, len(hashes)/32)
	prev := 0
	for len(hashes) > 0 {
		var pending types2.Hashes
		if len(hashes) > p2pTxPacketLimit {
			pending = hashes[:p2pTxPacketLimit]
			hashes = hashes[p2pTxPacketLimit:]
		} else {
			pending = hashes[:]
			hashes = hashes[:0]
		}

		hashesData := types2.EncodeHashes(pending, nil)
		var hashes66 *sentry.OutboundMessageData
		for _, sentryClient := range f.sentryClients {
			if !sentryClient.Ready() {
				continue
			}
			switch sentryClient.Protocol() {
			case direct.ETH66, direct.ETH67:
				if hashes66 == nil {
					hashes66 = &sentry.OutboundMessageData{
						Id:   sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
						Data: hashesData,
					}
				}
				peers, err := sentryClient.SendMessageToAll(f.ctx, hashes66, &grpc.EmptyCallOption{})
				if err != nil {
					log.Debug("[txpool.send] AnnouncePooledTxs", "err", err)
				}
				if peers != nil {
					for j, l := prev, pending.Len(); j < prev+l; j++ {
						hashSentTo[j] = len(peers.Peers)
					}
				}
			}
		}
		prev += pending.Len()
	}
	return
}

func (f *Send) PropagatePooledTxsToPeersList(peers []types2.PeerID, txs []byte) {
	defer f.notifyTests()

	if len(txs) == 0 {
		return
	}

	for len(txs) > 0 {
		var pending types2.Hashes
		if len(txs) > p2pTxPacketLimit {
			pending = txs[:p2pTxPacketLimit]
			txs = txs[p2pTxPacketLimit:]
		} else {
			pending = txs
			txs = txs[:0]
		}

		data := types2.EncodeHashes(pending, nil)
		for _, sentryClient := range f.sentryClients {
			if !sentryClient.Ready() {
				continue
			}

			for _, peer := range peers {
				switch sentryClient.Protocol() {
				case direct.ETH66, direct.ETH67:
					req66 := &sentry.SendMessageByIdRequest{
						PeerId: peer,
						Data: &sentry.OutboundMessageData{
							Id:   sentry.MessageId_NEW_POOLED_TRANSACTION_HASHES_66,
							Data: data,
						},
					}
					if _, err := sentryClient.SendMessageById(f.ctx, req66, &grpc.EmptyCallOption{}); err != nil {
						log.Debug("[txpool.send] PropagatePooledTxsToPeersList", "err", err)
					}
				}
			}
		}
	}
}
