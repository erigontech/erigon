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

	"github.com/erigontech/erigon/p2p/protocols/wit"
)

func NewWitnessPublisher(messageSender *MessageSender) *WitnessPublisher {
	return &WitnessPublisher{messageSender: messageSender}
}

// WitnessPublisher sends out wit/0 witness messages.
type WitnessPublisher struct {
	messageSender *MessageSender
}

// PublishWitness sends a Witness response to the peer that requested it.
func (p *WitnessPublisher) PublishWitness(ctx context.Context, peerId *PeerId, packet wit.WitnessPacketRLPPacket) error {
	return p.messageSender.SendWitness(ctx, peerId, packet)
}
