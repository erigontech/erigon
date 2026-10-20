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

package parlia

import (
	"encoding/binary"

	"github.com/erigontech/erigon/execution/types"
)

// MilliTimestamp returns the header time in milliseconds. From Lorentz on, the
// sub-second part is stored in the low bytes of MixDigest.
func MilliTimestamp(h *types.Header) uint64 {
	return h.Time*1000 + binary.BigEndian.Uint64(h.MixDigest[24:])
}
