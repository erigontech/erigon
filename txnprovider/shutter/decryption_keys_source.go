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
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/txnprovider/shutter/shuttercfg"
)

type DecryptionKeysSource interface {
	Run(ctx context.Context) error
	Subscribe(ctx context.Context) (DecryptionKeysSubscription, error)
}

type DecryptionKeysSubscription interface {
	Next(ctx context.Context) (*pubsub.Message, error)
}

type DecryptionKeysSourceFactory func(validator pubsub.ValidatorEx) DecryptionKeysSource

const (
	ProtocolVersion     = "/shutter/0.1.0"
	DecryptionKeysTopic = "decryptionKeys"
)

func NewPubSubDecryptionKeysSource(logger log.Logger, config shuttercfg.P2pConfig, validator pubsub.ValidatorEx) *PubSubDecryptionKeysSource {
	return &PubSubDecryptionKeysSource{
		logger:            logger,
		config:            config,
		validator:         validator,
		initialisedSignal: make(chan struct{}),
	}
}

type PubSubDecryptionKeysSource struct {
	logger            log.Logger
	config            shuttercfg.P2pConfig
	validator         pubsub.ValidatorEx
	topic             *pubsub.Topic
	running           atomic.Bool
	initialised       atomic.Bool
	initialisedSignal chan struct{}
}

func (dks *PubSubDecryptionKeysSource) Run(ctx context.Context) error {
	if !dks.running.CompareAndSwap(false, true) {
		return errors.New("decryption keys source already running")
	}
	defer dks.running.Store(false)
	p2pHost, err := dks.initP2pHost()
	if err != nil {
		return err
	}
	defer func() {
		err := p2pHost.Close()
		if err != nil {
			dks.logger.Error("failed to close p2p host", "err", err)
		}
	}()

	pubSub, err := dks.initGossipSub(ctx, p2pHost)
	if err != nil {
		return err
	}

	err = dks.connectBootstrapNodes(ctx, p2pHost)
	if err != nil {
		return err
	}

	err = pubSub.RegisterTopicValidator(DecryptionKeysTopic, dks.validator)
	if err != nil {
		return err
	}

	topic, err := pubSub.Join(DecryptionKeysTopic)
	if err != nil {
		return err
	}
	defer func() {
		if err := topic.Close(); err != nil && !errors.Is(err, context.Canceled) {
			dks.logger.Error("failed to close decryption keys topic", "err", err)
		}
	}()

	err = topic.SetScoreParams(decryptionKeysTopicScoreParams())
	if err != nil {
		return err
	}

	dks.topic = topic
	close(dks.initialisedSignal)
	defer func() { dks.initialisedSignal = make(chan struct{}) }()
	dks.initialised.Store(true)
	defer dks.initialised.Store(false)

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		err := dks.peerInfoLoop(ctx, pubSub)
		if err != nil {
			return fmt.Errorf("decryption keys peer info loop failure: %w", err)
		}
		return nil
	})

	eg.Go(func() error {
		select {
		case <-ctx.Done(): // to keep the host and topic alive until Run's ctx is cancelled
			return ctx.Err()
		}
	})

	return eg.Wait()
}

func (dks *PubSubDecryptionKeysSource) Subscribe(ctx context.Context) (DecryptionKeysSubscription, error) {
	if !dks.initialised.Load() {
		dks.logger.Debug("pubsub decryption keys source not yet initialised, waiting for initialisation")
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-dks.initialisedSignal:
		// continue
	}

	sub, err := dks.topic.Subscribe()
	if err != nil {
		return nil, err
	}
	go func() {
		select {
		case <-ctx.Done():
			dks.logger.Debug("cancelling pubsub decryption keys subscription")
			sub.Cancel()
		}
	}()
	return sub, nil
}

func (dks *PubSubDecryptionKeysSource) initP2pHost() (host.Host, error) {
	listenAddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/" + strconv.FormatUint(dks.config.ListenPort, 10))
	if err != nil {
		return nil, err
	}

	privKeyBytes := make([]byte, 32)
	dks.config.PrivateKey.D.FillBytes(privKeyBytes)
	privKey, err := libp2pcrypto.UnmarshalSecp256k1PrivateKey(privKeyBytes)
	if err != nil {
		return nil, err
	}

	p2pHost, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrs(listenAddr),
		libp2p.UserAgent("erigon/shutter/"+version.VersionWithCommit(version.GitCommit)),
		libp2p.ProtocolVersion(ProtocolVersion),
	)
	if err != nil {
		return nil, err
	}

	dks.logger.Info("shutter p2p host initialised", "addr", listenAddr, "id", p2pHost.ID())
	return p2pHost, nil
}

func (dks *PubSubDecryptionKeysSource) initGossipSub(ctx context.Context, host host.Host) (*pubsub.PubSub, error) {
	// NOTE: gossipSubParams, peerScoreParams, peerScoreThresholds are taken from
	// https://github.com/shutter-network/rolling-shutter/blob/main/rolling-shutter/p2p/params.go#L16
	gossipSubParams := pubsub.DefaultGossipSubParams()
	gossipSubParams.HeartbeatInterval = 700 * time.Millisecond
	gossipSubParams.HistoryLength = 6

	bootstrapNodes, err := dks.config.BootstrapNodesAddrInfo()
	if err != nil {
		return nil, err
	}

	bootstrapNodesSet := make(map[peer.ID]bool, len(dks.config.BootstrapNodes))
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

func (dks *PubSubDecryptionKeysSource) connectBootstrapNodes(ctx context.Context, host host.Host) error {
	nodes, err := dks.config.BootstrapNodesAddrInfo()
	if err != nil {
		return err
	}

	if len(nodes) == 0 {
		return errors.New("no shutter bootstrap nodes configured")
	}

	var connected atomic.Int32
	wg, ctx := errgroup.WithContext(ctx)
	for _, node := range nodes {
		wg.Go(func() error {
			connect := func() error {
				dks.logger.Info("connecting to bootstrap node", "node", node)
				err := host.Connect(ctx, node)
				if err != nil {
					dks.logger.Warn("failed to connect to bootstrap node, trying again", "node", node, "err", err)
				}
				return err
			}

			err = backoff.Retry(connect, backoff.WithContext(backoff.NewExponentialBackOff(), ctx))
			if err != nil {
				dks.logger.Error("failed to connect to bootstrap node", "node", node, "err", err)
				return nil
			}

			dks.logger.Info("connected to bootstrap node", "node", node)
			connected.Add(1)
			return nil
		})
	}

	err = wg.Wait()
	if err != nil {
		return err
	}

	if connected.Load() == 0 {
		return errors.New("failed to connect to any bootstrap node")
	}

	return nil
}

func (dks *PubSubDecryptionKeysSource) peerInfoLoop(ctx context.Context, pubSub *pubsub.PubSub) error {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			peers := pubSub.ListPeers(DecryptionKeysTopic)
			dks.logger.Info("decryption keys peer count", "peers", len(peers))
			decryptionKeysTopicPeerCount.Set(float64(len(peers)))
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
