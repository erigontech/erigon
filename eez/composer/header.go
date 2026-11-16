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

package composer

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
)

// SyncHeader returns the header the builder gives the block of inputs in on
// parent, roots and gas used aside: a draft of the block executes under it.
func SyncHeader(config *chain.Config, engine rules.Engine, headers rules.ChainHeaderReader, parent *types.Header, in deriver.HeaderInputs) (*types.Header, error) {
	gasLimit := in.GasLimit
	h := builder.MakeEmptyHeader(parent, config, in.Time, &gasLimit)
	if err := misc.VerifyGaslimit(parent.GasLimit, h.GasLimit); err != nil {
		h.GasLimit = parent.GasLimit
	}
	h.Coinbase = in.Coinbase
	h.Extra = in.Extra
	if h.Extra == nil {
		h.Extra = []byte{}
	}
	if err := engine.Prepare(headers, h); err != nil {
		return nil, err
	}
	h.MixDigest = in.MixDigest
	if config.IsCancun(in.Time) {
		h.ParentBeaconBlockRoot = &common.Hash{}
	}
	return h, nil
}
