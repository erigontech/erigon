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
	"strconv"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/txnprovider/shutter"
	shutterproto "github.com/erigontech/erigon/txnprovider/shutter/internal/proto"
)

type DecryptionKeysSender struct {
	logger log.Logger
	host   host.Host
	topic  *pubsub.Topic
}

func DialDecryptionKeysSender(ctx context.Context, logger log.Logger, port int, key libp2pcrypto.PrivKey) (DecryptionKeysSender, error) {
	logger = logger.New("component", "decryption-key-sender")

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

	logger.Debug("p2p host initialised", "addr", addr, "id", p2pHost.ID())
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

func (dks DecryptionKeysSender) WaitExternalPeerConnection(ctx context.Context, peerId peer.ID) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			peers := dks.host.Network().Peers()
			for _, p := range peers {
				if p == peerId {
					return nil
				}
			}
		}
	}
}

func (dks DecryptionKeysSender) PublishDecryptionKeys(
	ctx context.Context,
	ekg EonKeyGeneration,
	slot uint64,
	txnPointer uint64,
	ips shutter.IdentityPreimages,
	instanceId uint64,
) error {
	signers := ekg.Keypers[:ekg.Threshold]
	signerIndices := make([]uint64, len(signers))
	for i, signer := range signers {
		signerIndices[i] = uint64(signer.Index)
	}

	slotIp, err := MakeSlotIdentityPreimage(slot)
	if err != nil {
		return err
	}

	ipsWithSlot := shutter.IdentityPreimages{slotIp}
	ipsWithSlot = append(ipsWithSlot, ips...)
	keys, err := ekg.DecryptionKeys(signers, ipsWithSlot)
	if err != nil {
		return err
	}

	signatureData := shutter.DecryptionKeysSignatureData{
		InstanceId:        instanceId,
		Eon:               ekg.EonIndex,
		Slot:              slot,
		TxnPointer:        txnPointer,
		IdentityPreimages: ipsWithSlot.ToListSSZ(),
	}

	sigs, err := Signatures(signers, signatureData)
	if err != nil {
		return err
	}

	keysEnvelope, err := MockDecryptionKeysEnvelopeData(MockDecryptionKeysEnvelopeDataOptions{
		EonIndex:      ekg.EonIndex,
		Keys:          keys,
		Slot:          slot,
		TxnPointer:    txnPointer,
		InstanceId:    instanceId,
		SignerIndices: signerIndices,
		Signatures:    sigs,
		Version:       shutterproto.EnvelopeVersion,
	})
	if err != nil {
		return err
	}

	dks.logger.Debug("publishing decryption keys", "slot", slot, "eon", ekg.EonIndex, "txnPointer", txnPointer, "keys", len(keys))
	return dks.topic.Publish(ctx, keysEnvelope)
}

func (dks DecryptionKeysSender) Close() error {
	return dks.host.Close()
}
