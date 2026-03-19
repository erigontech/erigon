// Copyright 2022 The Erigon Authors
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

package sentinel

import (
	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/cl/p2p"
)

type SentinelConfig struct {
	p2p.P2PConfig

	MaxInboundTrafficPerPeer     datasize.ByteSize
	MaxOutboundTrafficPerPeer    datasize.ByteSize
	AdaptableTrafficRequirements bool

	EnableBlocks       bool
	SubscribeAllTopics bool // Capture all topics
	ActiveIndicies     uint64
}
