// Copyright 2022 The go-ethereum Authors
// (original work)
// Copyright 2025 The Erigon Authors
// (modifications)
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

package native

import (
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
	"github.com/holiman/uint256"
)

type arbitrumTransfer struct {
	Purpose string  `json:"purpose"`
	From    *string `json:"from"`
	To      *string `json:"to"`
	Value   string  `json:"value"`
}

func (t *callTracer) CaptureArbitrumTransfer(from, to *libcommon.Address, value *uint256.Int, before bool, reason string) {
	transfer := arbitrumTransfer{
		Purpose: reason,
		Value:   value.Hex(),
	}
	if from != nil {
		from := from.String()
		transfer.From = &from
	}
	if to != nil {
		to := to.String()
		transfer.To = &to
	}
	if before {
		t.beforeEVMTransfers = append(t.beforeEVMTransfers, transfer)
	} else {
		t.afterEVMTransfers = append(t.afterEVMTransfers, transfer)
	}
}

func (t *prestateTracer) CaptureArbitrumStorageGet(key libcommon.Hash, depth int, before bool) {
	t.lookupAccount(types.ArbosStateAddress)
	t.lookupStorage(types.ArbosStateAddress, key)
}

func (t *prestateTracer) CaptureArbitrumStorageSet(key, value libcommon.Hash, depth int, before bool) {
	t.lookupAccount(types.ArbosStateAddress)
	t.lookupStorage(types.ArbosStateAddress, key)
}
