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

package js

import (
	"github.com/dop251/goja"
	"github.com/holiman/uint256"

	libcommon "github.com/erigontech/erigon-lib/common"
)

func (jst *jsTracer) CaptureArbitrumTransfer(
	from, to *libcommon.Address, value *uint256.Int, before bool, reason string) {
	traceTransfer, ok := goja.AssertFunction(jst.obj.Get("captureArbitrumTransfer"))
	if !ok {
		return
	}

	transfer := jst.vm.NewObject()
	if from != nil {
		transfer.Set("from", from.String())
	} else {
		transfer.Set("from", nil)
	}
	if to != nil {
		transfer.Set("to", to.String())
	} else {
		transfer.Set("to", nil)
	}

	transfer.Set("value", value)
	transfer.Set("before", before)
	transfer.Set("purpose", reason)

	if _, err := traceTransfer(transfer); err != nil {
		jst.err = wrapError("captureArbitrumTransfer", err)
	}
}
