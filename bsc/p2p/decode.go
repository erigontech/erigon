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

package bscp2p

import (
	"fmt"

	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

// MessageListenerOptions makes a message listener accept BSC block bodies and
// NewBlock packets, which may end with an optional blob-sidecars list.
// Execution never reads sidecars, so the decoders skip it.
func MessageListenerOptions() []p2p.MessageListenerOption {
	return []p2p.MessageListenerOption{
		p2p.WithBlockBodiesDecoder(decodeBlockBodies),
		p2p.WithNewBlockDecoder(decodeNewBlock),
	}
}

func decodeBlockBodies(data []byte) (*eth.BlockBodiesPacket66, error) {
	s := rlp.NewBytesStream(data)
	defer rlp.PutStream(s)

	if _, err := s.List(); err != nil {
		return nil, err
	}
	packet := &eth.BlockBodiesPacket66{}
	var err error
	if packet.RequestId, err = s.Uint64(); err != nil {
		return nil, fmt.Errorf("read RequestId: %w", err)
	}
	if _, err := s.List(); err != nil {
		return nil, err
	}
	for s.MoreDataInList() {
		body := &types.Body{}
		if _, err := s.List(); err != nil {
			return nil, err
		}
		if err := body.DecodeFields(s); err != nil {
			return nil, err
		}
		if err := endWithOptionalSidecars(s); err != nil {
			return nil, err
		}
		packet.BlockBodiesPacket = append(packet.BlockBodiesPacket, body)
	}
	if err := s.ListEnd(); err != nil {
		return nil, err
	}
	if err := s.ListEnd(); err != nil {
		return nil, err
	}
	return packet, endOfInput(s)
}

func decodeNewBlock(data []byte) (*eth.NewBlockPacket, error) {
	s := rlp.NewBytesStream(data)
	defer rlp.PutStream(s)

	if _, err := s.List(); err != nil {
		return nil, err
	}
	packet := &eth.NewBlockPacket{Block: &types.Block{}}
	if err := packet.Block.DecodeRLP(s); err != nil {
		return nil, err
	}
	if err := s.ReadUint256(&packet.TD); err != nil {
		return nil, fmt.Errorf("read TD: %w", err)
	}
	if err := endWithOptionalSidecars(s); err != nil {
		return nil, err
	}
	return packet, endOfInput(s)
}

func endWithOptionalSidecars(s *rlp.Stream) error {
	if s.MoreDataInList() {
		if _, err := s.Raw(); err != nil {
			return fmt.Errorf("read Sidecars: %w", err)
		}
	}
	return s.ListEnd()
}

func endOfInput(s *rlp.Stream) error {
	if s.Remaining() > 0 {
		return rlp.ErrMoreThanOneValue
	}
	return nil
}
