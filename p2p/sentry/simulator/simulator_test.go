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

//go:build integration

package simulator_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/direct"
	sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	sentry_if "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/eth/protocols/eth"
	"github.com/erigontech/erigon/p2p/sentry/simulator"
	"github.com/erigontech/erigon/rlp"
)

func TestSimulatorStart(t *testing.T) {
	t.Skip("For now, this test is intended for manual runs only as it downloads snapshots and takes too long")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := log.New()
	// logger.SetHandler(log.StdoutHandler)
	dataDir := t.TempDir()

	sim, err := simulator.NewSentry(ctx, "amoy", dataDir, 1, logger)
	if err != nil {
		t.Fatal(err)
	}

	simClient := direct.NewSentryClientDirect(66, sim)

	peerCount, err := simClient.PeerCount(ctx, &sentry.PeerCountRequest{})
	if err != nil {
		t.Fatal(err)
	}

	if peerCount.Count != 1 {
		t.Fatal("Invalid response count: expected:", 1, "got:", peerCount.Count)
	}

	receiver, err := simClient.Messages(ctx, &sentry.MessagesRequest{
		Ids: []sentry.MessageId{sentry.MessageId_BLOCK_HEADERS_66},
	})

	if err != nil {
		t.Fatal(err)
	}

	getHeaders66 := &eth.GetBlockHeadersPacket66{
		RequestId: 1,
		GetBlockHeadersPacket: &eth.GetBlockHeadersPacket{
			Origin: eth.HashOrNumber{Number: 10},
			Amount: 10,
		},
	}

	var data bytes.Buffer

	err = rlp.Encode(&data, getHeaders66)

	if err != nil {
		t.Fatal(err)
	}

	peers, err := simClient.SendMessageToAll(ctx, &sentry.OutboundMessageData{
		Id:   sentry_if.MessageId_GET_BLOCK_HEADERS_66,
		Data: data.Bytes(),
	})

	if err != nil {
		t.Fatal(err)
	}

	if len(peers.Peers) != int(peerCount.Count) {
		t.Fatal("Unexpected peer count expected:", peerCount.Count, len(peers.Peers))
	}

	message, err := receiver.Recv()

	if err != nil {
		t.Fatal(err)
	}

	if message.Id != sentry_if.MessageId_BLOCK_HEADERS_66 {
		t.Fatal("unexpected message id expected:", sentry_if.MessageId_BLOCK_HEADERS_66, "got:", message.Id)
	}

	var expectedPeer bool

	for _, peer := range peers.Peers {
		if message.PeerId.String() == peer.String() {
			expectedPeer = true
			break
		}
	}

	if !expectedPeer {
		t.Fatal("message received from unexpected peer:", message.PeerId)
	}

	packet := &eth.BlockHeadersPacket66{}

	if err := rlp.DecodeBytes(message.Data, packet); err != nil {
		t.Fatal("failed to decode packet:", err)
	}

	if len(packet.BlockHeadersPacket) != 10 {
		t.Fatal("unexpected header count: expected:", 10, "got:", len(packet.BlockHeadersPacket))
	}

	blockNum := uint64(10)

	for _, header := range packet.BlockHeadersPacket {
		if header.Number.Uint64() != blockNum {
			t.Fatal("unexpected block number: expected:", blockNum, "got:", header.Number)
		}

		blockNum++
	}

	simClient65 := direct.NewSentryClientDirect(65, sim)

	getHeaders65 := &eth.GetBlockHeadersPacket{
		Origin: eth.HashOrNumber{Number: 100},
		Amount: 50,
	}

	data.Reset()

	err = rlp.Encode(&data, getHeaders65)

	if err != nil {
		t.Fatal(err)
	}

	peers65, err := simClient65.SendMessageById(ctx, &sentry_if.SendMessageByIdRequest{
		Data: &sentry.OutboundMessageData{
			Id:   sentry_if.MessageId_GET_BLOCK_HEADERS_65,
			Data: data.Bytes(),
		},
		PeerId: peers.Peers[0],
	})

	if err != nil {
		t.Fatal(err)
	}

	if len(peers65.Peers) != 1 {
		t.Fatal("message sent to unexpected number of peers:", len(peers65.Peers))
	}

	if peers65.Peers[0].String() != peers.Peers[0].String() {
		t.Fatal("message sent to unexpected number of peers", peers65.Peers[0])
	}

	receiver65, err := simClient65.Messages(ctx, &sentry.MessagesRequest{
		Ids: []sentry.MessageId{sentry.MessageId_BLOCK_HEADERS_65},
	})

	if err != nil {
		t.Fatal(err)
	}

	message, err = receiver65.Recv()

	if err != nil {
		t.Fatal(err)
	}

	if message.Id != sentry_if.MessageId_BLOCK_HEADERS_65 {
		t.Fatal("unexpected message id expected:", sentry_if.MessageId_BLOCK_HEADERS_65, "got:", message.Id)
	}

	if message.PeerId.String() != peers.Peers[0].String() {
		t.Fatal("message received from unexpected peer:", message.PeerId)
	}

	packet65 := eth.BlockHeadersPacket{}

	if err := rlp.DecodeBytes(message.Data, &packet65); err != nil {
		t.Fatal("failed to decode packet:", err)
	}

	if len(packet65) != 50 {
		t.Fatal("unexpected header count: expected:", 50, "got:", len(packet.BlockHeadersPacket))
	}

	blockNum = uint64(100)

	for _, header := range packet65 {
		if header.Number.Uint64() != blockNum {
			t.Fatal("unexpected block number: expected:", blockNum, "got:", header.Number)
		}

		blockNum++
	}
}
