// Copyright 2025 The Erigon Authors
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

package shutter

import (
	"context"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/txnprovider/shutter/proto"
)

const (
	ProtocolVersion     = "/shutter/0.1.0"
	DecryptionKeysTopic = "decryptionKeys"
)

type DecryptionKeysStreamer struct {
	logger log.Logger
	config Config
	keys   chan *proto.DecryptionKeys
}

func NewDecryptionKeysStreamer(logger log.Logger, config Config) DecryptionKeysStreamer {
	return DecryptionKeysStreamer{
		logger: logger,
		config: config,
		keys:   make(chan *proto.DecryptionKeys),
	}
}

// Run the streamer. Blocks until an error occurs or context has been cancelled and performs
// any required cleanup and/or graceful shutdown.
func (s DecryptionKeysStreamer) Run(ctx context.Context) error {
	s.logger.Info("running decryption keys streamer")

	p2pHost, err := s.initP2pHost()
	if err != nil {
		return err
	}

	pubSub, err := s.initGossipSub(ctx, p2pHost)
	if err != nil {
		return err
	}

	err = s.connectBootstrapNodes(ctx, p2pHost)
	if err != nil {
		return err
	}

	//
	// TODO play around with go-libp2p-kad-dht for routing and discovery analogous to rolling-shutter
	//      check if it improves number of peers for topic
	//

	//
	// TODO persist connected nodes to be able to re-use on restart
	//

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return s.listenLoop(ctx, pubSub) })
	eg.Go(func() error { return s.peerInfoLoop(ctx, pubSub) })
	return eg.Wait()
}

// Next returns the next decryption keys message from the streamer. Blocks if no new messages
// until one arrives. It is context aware, meaning it unblocks itself and returns an error when
// the context is cancelled.
//
// The streamer guarantees that all decryption keys are valid after passing validation.
func (s DecryptionKeysStreamer) Next(ctx context.Context) (*proto.DecryptionKeys, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case keys := <-s.keys:
		return keys, nil
	}
}

func (s DecryptionKeysStreamer) initP2pHost() (host.Host, error) {
	listenAddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", s.config.ListenPort))
	if err != nil {
		return nil, err
	}

	privKey, err := libp2pcrypto.UnmarshalSecp256k1PrivateKey(s.config.PrivateKey.D.Bytes())
	if err != nil {
		return nil, err
	}

	p2pHost, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrs(listenAddr),
		libp2p.UserAgent(fmt.Sprintf("erigon/shutter/%s", params.VersionWithCommit(params.GitCommit))),
		libp2p.ProtocolVersion(ProtocolVersion),
	)
	if err != nil {
		return nil, err
	}

	s.logger.Info("shutter libp2p host initialised", "addr", listenAddr, "id", p2pHost.ID())
	return p2pHost, nil
}

func (s DecryptionKeysStreamer) initGossipSub(ctx context.Context, host host.Host) (*pubsub.PubSub, error) {
	// NOTE: gossipSubParams, peerScoreParams, peerScoreThresholds are taken from
	// https://github.com/shutter-network/rolling-shutter/blob/main/rolling-shutter/p2p/params.go#L16
	gossipSubParams := pubsub.DefaultGossipSubParams()
	gossipSubParams.HeartbeatInterval = 700 * time.Millisecond
	gossipSubParams.HistoryLength = 6

	bootstrapNodes, err := s.config.BootstrapNodesAddrInfo()
	if err != nil {
		return nil, err
	}

	bootstrapNodesSet := make(map[peer.ID]bool, len(s.config.BootstrapNodes))
	for _, node := range bootstrapNodes {
		bootstrapNodesSet[node.ID] = true
	}

	// NOTE: loosely from the gossipsub spec:
	// Only the bootstrappers / highly trusted PX'ing nodes
	// should reach the AcceptPXThreshold thus they need
	// to be treated differently in the scoring function.
	appSpecificScoringFn := func(p peer.ID) float64 {
		_, ok := bootstrapNodesSet[p]
		if !ok {
			return 0
		}
		// In order to be able to participate in the gossipsub,
		// a peer has to be PX'ed by a bootstrap node - this is only
		// possible if the AcceptPXThreshold peer-score is reached.

		// NOTE: we have yet to determine a value that is
		// sufficient to reach the AcceptPXThreshold most of the time,
		// but don't overshoot and trust the bootstrap peers
		// unconditionally - they should still be punishable
		// for malicous behavior
		return 200
	}
	peerScoreParams := &pubsub.PeerScoreParams{
		// Topics score-map will be filled later while subscribing to topics.
		Topics:                      make(map[string]*pubsub.TopicScoreParams),
		TopicScoreCap:               32.72,
		AppSpecificScore:            appSpecificScoringFn,
		AppSpecificWeight:           1,
		IPColocationFactorWeight:    -35.11,
		IPColocationFactorThreshold: 10,
		IPColocationFactorWhitelist: nil,
		BehaviourPenaltyWeight:      -15.92,
		BehaviourPenaltyThreshold:   6,
		BehaviourPenaltyDecay:       0.928,
		DecayInterval:               12 * time.Second,
		DecayToZero:                 0.01,
		RetainScore:                 12 * time.Hour,
	}

	peerScoreThresholds := &pubsub.PeerScoreThresholds{
		GossipThreshold:             -4000,
		PublishThreshold:            -8000,
		GraylistThreshold:           -16000,
		AcceptPXThreshold:           100,
		OpportunisticGraftThreshold: 5,
	}

	return pubsub.NewGossipSub(
		ctx,
		host,
		pubsub.WithGossipSubParams(gossipSubParams),
		pubsub.WithPeerScore(peerScoreParams, peerScoreThresholds),
	)
}

func (s DecryptionKeysStreamer) connectBootstrapNodes(ctx context.Context, host host.Host) error {
	nodes, err := s.config.BootstrapNodesAddrInfo()
	if err != nil {
		return err
	}

	wg, ctx := errgroup.WithContext(ctx)
	for _, node := range nodes {
		wg.Go(func() error {
			err := host.Connect(ctx, node)
			if err != nil {
				s.logger.Error("failed to connect to bootstrap node", "node", node, "err", err)
			}
			return nil
		})
	}

	return wg.Wait()
}

func (s DecryptionKeysStreamer) listenLoop(ctx context.Context, pubSub *pubsub.PubSub) error {
	topicValidator := NewDecryptionKeysP2pValidatorEx(s.logger, s.config)
	err := pubSub.RegisterTopicValidator(DecryptionKeysTopic, topicValidator)
	if err != nil {
		return err
	}

	topic, err := pubSub.Join(DecryptionKeysTopic)
	if err != nil {
		return err
	}
	defer func() {
		if err := topic.Close(); err != nil {
			s.logger.Error("failed to close decryption keys topic", "err", err)
		}
	}()

	err = topic.SetScoreParams(decryptionKeysTopicScoreParams())
	if err != nil {
		return err
	}

	sub, err := topic.Subscribe()
	if err != nil {
		return err
	}
	defer sub.Cancel()

	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			return err
		}

		decryptionKeys, err := proto.UnmarshallDecryptionKeys(msg.Data)
		if err != nil {
			s.logger.Debug("failed to unmarshal decryption keys, skipping message", "err", err)
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.keys <- decryptionKeys:
		}
	}
}

func (s DecryptionKeysStreamer) peerInfoLoop(ctx context.Context, pubSub *pubsub.PubSub) error {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			peers := pubSub.ListPeers(DecryptionKeysTopic)
			s.logger.Info("decryption keys peer count", "peers", len(peers))
		}
	}
}

func decryptionKeysTopicScoreParams() *pubsub.TopicScoreParams {
	// NOTE: this is taken from
	// https://github.com/shutter-network/rolling-shutter/blob/main/rolling-shutter/p2p/params.go#L100
	//
	// Based on attestation topic in beacon chain network. The formula uses the number of
	// validators which we set to a fixed number which could be the number of keypers.
	n := float64(200)
	return &pubsub.TopicScoreParams{
		TopicWeight:                     1,
		TimeInMeshWeight:                0.0324,
		TimeInMeshQuantum:               12 * time.Second,
		TimeInMeshCap:                   300,
		FirstMessageDeliveriesWeight:    0.05,
		FirstMessageDeliveriesDecay:     0.631,
		FirstMessageDeliveriesCap:       n / 755.712,
		MeshMessageDeliveriesWeight:     -0.026,
		MeshMessageDeliveriesDecay:      0.631,
		MeshMessageDeliveriesCap:        n / 94.464,
		MeshMessageDeliveriesThreshold:  n / 377.856,
		MeshMessageDeliveriesWindow:     200 * time.Millisecond,
		MeshMessageDeliveriesActivation: 4 * 12 * time.Second,
		MeshFailurePenaltyWeight:        -0.0026,
		MeshFailurePenaltyDecay:         0.631,
		InvalidMessageDeliveriesWeight:  -99,
		InvalidMessageDeliveriesDecay:   0.9994,
	}
}
