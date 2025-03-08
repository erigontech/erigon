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

package testhelpers

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/txnprovider/shutter"
)

type DecryptionKeysSender struct {
	logger log.Logger
	host   host.Host
	topic  *pubsub.Topic
}

func DialDecryptionKeysSender(ctx context.Context, logger log.Logger, port int, key libp2pcrypto.PrivKey) (DecryptionKeysSender, error) {
	addr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/" + strconv.FormatInt(int64(port), 10))
	if err != nil {
		return DecryptionKeysSender{}, err
	}

	p2pHost, err := libp2p.New(
		libp2p.Identity(key),
		libp2p.ListenAddrs(addr),
		libp2p.UserAgent("test/decryption-key-sender"),
		libp2p.ProtocolVersion(shutter.ProtocolVersion),
	)
	if err != nil {
		return DecryptionKeysSender{}, err
	}

	logger.Debug("decryption key sender p2p host initialised", "addr", addr, "id", p2pHost.ID())
	gossipSub, err := pubsub.NewGossipSub(ctx, p2pHost)
	if err != nil {
		return DecryptionKeysSender{}, err
	}

	topic, err := gossipSub.Join(shutter.DecryptionKeysTopic)
	if err != nil {
		return DecryptionKeysSender{}, err
	}

	return DecryptionKeysSender{logger: logger, host: p2pHost, topic: topic}, nil
}

func (dks DecryptionKeysSender) Connect(ctx context.Context, port int, peerId peer.ID) error {
	receiverAddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d/p2p/%s", port, peerId))
	if err != nil {
		return err
	}

	receiverAddrInfo, err := peer.AddrInfoFromP2pAddr(receiverAddr)
	if err != nil {
		return err
	}

	connect := func() error {
		err := dks.host.Connect(ctx, *receiverAddrInfo)
		if err != nil {
			dks.logger.Warn(
				"decryption key sender failed to connect to receiver, trying again",
				"receiver", receiverAddrInfo,
				"err", err,
			)
		}
		return err
	}

	dks.logger.Debug("decryption key sender connecting to receiver", "receiver", receiverAddrInfo)
	err = backoff.Retry(connect, backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Second), 10))
	if err != nil {
		return err
	}

	dks.logger.Debug("decryption key sender connected to receiver", "receiver", receiverAddrInfo)
	return nil
}

func (dks DecryptionKeysSender) PublishDecryptionKeys(ctx context.Context, msg []byte) error {
	return dks.topic.Publish(ctx, msg)
}

func (dks DecryptionKeysSender) Close() error {
	return dks.host.Close()
}
