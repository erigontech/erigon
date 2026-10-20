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
package deriver

import (
	"bytes"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/protocol/da"
	"github.com/erigontech/erigon/execution/types"
)

// HeaderInputs are the header fields of a derived block that its
// transactions do not decide; the rest follows from the parent and the rules.
type HeaderInputs struct {
	Number     uint64
	ParentHash common.Hash
	Time       uint64
	Coinbase   common.Address
	Extra      []byte
	GasLimit   uint64
}

// DeriveInputs returns the inputs of the block after parent that the span
// carries at position i: the next timestamp on the configured interval, the
// beneficiary and extraData the composer chose, the configured gas limit.
func DeriveInputs(l2 *config.L2Config, parent *types.Header, span *da.Span, i int) HeaderInputs {
	return HeaderInputs{
		Number:     parent.Number.Uint64() + 1,
		ParentHash: parent.Hash(),
		Time:       parent.Time + l2.BlockInterval,
		Coinbase:   span.Beneficiaries[i],
		Extra:      bytes.Clone(span.ExtraData[i]),
		GasLimit:   l2.BlockGasLimit,
	}
}

// Matches reports whether h carries these inputs.
func (in *HeaderInputs) Matches(h *types.Header) bool {
	return h.Number.Uint64() == in.Number && h.ParentHash == in.ParentHash && h.Time == in.Time &&
		h.Coinbase == in.Coinbase && bytes.Equal(h.Extra, in.Extra) && h.GasLimit == in.GasLimit
}
