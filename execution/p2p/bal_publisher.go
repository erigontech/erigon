// Copyright 2026 The Erigon Authors
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

package p2p

import (
	"context"

	"github.com/erigontech/erigon/p2p/protocols/eth"
)

func NewBALPublisher(messageSender *MessageSender) *BALPublisher {
	return &BALPublisher{messageSender: messageSender}
}

// BALPublisher sends out eth/71 (EIP-8159) block access list messages.
type BALPublisher struct {
	messageSender *MessageSender
}

// PublishBlockAccessLists sends a BlockAccessLists response to the peer that requested them.
func (p *BALPublisher) PublishBlockAccessLists(ctx context.Context, peerId *PeerId, packet eth.BlockAccessListsPacket66) error {
	return p.messageSender.SendBlockAccessLists(ctx, peerId, packet)
}
