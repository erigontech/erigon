// Copyright 2024 The Erigon Authors
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

package sentry

import (
	"context"
	"fmt"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/forkid"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

func readAndValidatePeerStatus[T StatusPacket](
	rw p2p.MsgReadWriter,
	status *sentryproto.StatusData,
	version uint,
	minVersion uint,
	compat func(T, *sentryproto.StatusData, uint, uint) error,
) (T, *p2p.PeerError) {
	var zero T
	msg, err := rw.ReadMsg()
	if err != nil {
		return zero, p2p.NewPeerError(p2p.PeerErrorStatusReceive, p2p.DiscNetworkError, err, "readAndValidatePeerStatus rw.ReadMsg error")
	}
	defer msg.Discard()
	if msg.Code != eth.StatusMsg {
		return zero, p2p.NewPeerError(p2p.PeerErrorStatusDecode, p2p.DiscProtocolError, fmt.Errorf("first msg has code %x (!= %x)", msg.Code, eth.StatusMsg), "readAndValidatePeerStatus wrong code")
	}
	if msg.Size > eth.ProtocolMaxMsgSize {
		return zero, p2p.NewPeerError(p2p.PeerErrorStatusDecode, p2p.DiscProtocolError, fmt.Errorf("message is too large %d, limit %d", msg.Size, eth.ProtocolMaxMsgSize), "readAndValidatePeerStatus too large")
	}

	var reply T
	if err := msg.Decode(&reply); err != nil {
		return zero, p2p.NewPeerError(p2p.PeerErrorStatusDecode, p2p.DiscProtocolError, fmt.Errorf("decode message %v: %w", msg, err), "readAndValidatePeerStatus decode error")
	}

	if err := compat(reply, status, version, minVersion); err != nil {
		return zero, p2p.NewPeerError(p2p.PeerErrorStatusIncompatible, p2p.DiscUselessPeer, err, "readAndValidatePeerStatus incompatible")
	}

	return reply, nil
}

func compatStatusPacket(reply eth.StatusPacket, status *sentryproto.StatusData, version, minVersion uint) error {
	return checkCompatibility(reply.NetworkID, reply.ProtocolVersion, reply.Genesis, reply.ForkID, status, version, minVersion)
}

func compatStatusPacket69(reply eth.StatusPacket69, status *sentryproto.StatusData, version, minVersion uint) error {
	return checkCompatibility(reply.NetworkID, reply.ProtocolVersion, reply.Genesis, reply.ForkID, status, version, minVersion)
}

func checkCompatibility(networkID uint64, protocolVersion uint32, genesis common.Hash, forkID forkid.ID, status *sentryproto.StatusData, version, minVersion uint) error {
	expectedNetworkID := status.NetworkId
	if networkID != expectedNetworkID {
		return fmt.Errorf("network id does not match: theirs %d, ours %d", networkID, expectedNetworkID)
	}
	if uint(protocolVersion) > version {
		return fmt.Errorf("version is more than what this senty supports: theirs %d, max %d", protocolVersion, version)
	}
	if uint(protocolVersion) < minVersion {
		return fmt.Errorf("version is less than allowed minimum: theirs %d, min %d", protocolVersion, minVersion)
	}
	genesisHash := gointerfaces.ConvertH256ToHash(status.ForkData.Genesis)
	if genesis != genesisHash {
		return fmt.Errorf("genesis hash does not match: theirs %x, ours %x", genesis, genesisHash)
	}
	forkFilter := forkid.NewFilterFromForks(status.ForkData.HeightForks, status.ForkData.TimeForks, genesisHash, status.MaxBlockHeight, status.MaxBlockTime)
	return forkFilter(forkID)
}

// StatusPacket is the set of supported ETH status packet value types.
type StatusPacket interface {
	eth.StatusPacket | eth.StatusPacket69
}

func handShake[T StatusPacket](
	ctx context.Context,
	status *sentryproto.StatusData,
	rw p2p.MsgReadWriter,
	version uint,
	minVersion uint,
	encode func(*sentryproto.StatusData, uint) T,
	compat func(T, *sentryproto.StatusData, uint, uint) error,
	timeout time.Duration,
) (*T, *p2p.PeerError) {
	errChan := make(chan *p2p.PeerError, 2)
	resultChan := make(chan T, 1)

	// Send our status
	go func() {
		defer dbg.LogPanic()
		payload := encode(status, version)
		if err := p2p.Send(rw, eth.StatusMsg, payload); err == nil {
			errChan <- nil
		} else {
			errChan <- p2p.NewPeerError(p2p.PeerErrorStatusSend, p2p.DiscNetworkError, err, "sentry.handShake failed to send eth Status")
		}
	}()

	// Read and validate peer status
	go func() {
		defer dbg.LogPanic()
		reply, err := readAndValidatePeerStatus[T](rw, status, version, minVersion, compat)
		if err == nil {
			resultChan <- reply
			errChan <- nil
		} else {
			errChan <- err
		}
	}()

	t := time.NewTimer(timeout)
	defer t.Stop()
	for i := 0; i < 2; i++ {
		select {
		case err := <-errChan:
			if err != nil {
				return nil, err
			}
		case <-t.C:
			return nil, p2p.NewPeerError(p2p.PeerErrorStatusHandshakeTimeout, p2p.DiscReadTimeout, nil, "sentry.handShake timeout")
		case <-ctx.Done():
			return nil, p2p.NewPeerError(p2p.PeerErrorDiscReason, p2p.DiscQuitting, ctx.Err(), "sentry.handShake ctx.Done")
		}
	}
	// Safely wait for the reply with the same guards
	t2 := time.NewTimer(timeout)
	defer t2.Stop()
	select {
	case reply := <-resultChan:
		return &reply, nil
	case <-t2.C:
		return nil, p2p.NewPeerError(p2p.PeerErrorStatusHandshakeTimeout, p2p.DiscReadTimeout, nil, "sentry.handShake timeout (awaiting result)")
	case <-ctx.Done():
		return nil, p2p.NewPeerError(p2p.PeerErrorDiscReason, p2p.DiscQuitting, ctx.Err(), "sentry.handShake ctx.Done (awaiting result)")
	}
}

// Encoders for status messages
func encodeStatusPacket(status *sentryproto.StatusData, version uint) eth.StatusPacket {
	ourTD := gointerfaces.ConvertH256ToUint256Int(status.TotalDifficulty)
	genesisHash := gointerfaces.ConvertH256ToHash(status.ForkData.Genesis)
	return eth.StatusPacket{
		ProtocolVersion: uint32(version),
		NetworkID:       status.NetworkId,
		TD:              ourTD.ToBig(),
		Head:            gointerfaces.ConvertH256ToHash(status.BestHash),
		Genesis:         genesisHash,
		ForkID:          forkid.NewIDFromForks(status.ForkData.HeightForks, status.ForkData.TimeForks, genesisHash, status.MaxBlockHeight, status.MaxBlockTime),
	}
}

func encodeStatusPacket69(status *sentryproto.StatusData, version uint) eth.StatusPacket69 {
	genesisHash := gointerfaces.ConvertH256ToHash(status.ForkData.Genesis)
	return eth.StatusPacket69{
		ProtocolVersion: uint32(version),
		NetworkID:       status.NetworkId,
		Genesis:         genesisHash,
		ForkID:          forkid.NewIDFromForks(status.ForkData.HeightForks, status.ForkData.TimeForks, genesisHash, status.MaxBlockHeight, status.MaxBlockTime),
		MinimumBlock:    status.MinimumBlockHeight,
		LatestBlock:     status.MaxBlockHeight,
		LatestBlockHash: gointerfaces.ConvertH256ToHash(status.BestHash),
	}
}
