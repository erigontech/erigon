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

package cas20

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/vm"
)

// BEP-702 3.13: an immutable currency() and decimals fixed at 6.

const cas20StablecoinNamespace = "bsc.cas20.stablecoin"

const cas20StablecoinSlotCurrency = 0

var (
	cas20StablecoinRoot = erc7201Root(cas20StablecoinNamespace)

	selCurrency = selector("currency()")
)

type stablecoinExt struct{ s cas20Storage }

func newStablecoinExt(ctx *PrecompileContext) stablecoinExt {
	return stablecoinExt{s: newMeteredCAS20Storage(ctx)}
}

func stablecoinSlot(offset uint64) common.Hash {
	return offsetSlot(cas20StablecoinRoot, offset)
}

func (e stablecoinExt) currency() (string, bool) {
	return e.s.getStringAt(stablecoinSlot(cas20StablecoinSlotCurrency))
}

func (e stablecoinExt) setCurrency(v string) bool {
	return e.s.setStringAt(stablecoinSlot(cas20StablecoinSlotCurrency), v)
}

func stablecoinDispatch(tok cas20Token, ext stablecoinExt, input []byte) ([]byte, error) {
	if len(input) >= 4 {
		var sel [4]byte
		copy(sel[:], input[:4])
		if sel == selCurrency {
			v, ok := ext.currency()
			if !ok {
				return nil, vm.ErrOutOfGas
			}
			return encString(v), nil
		}
	}
	return tok.dispatch(input)
}
