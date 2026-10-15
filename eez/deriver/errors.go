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
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
)

// ErrDiverged reports a local L2 chain that L1's settlements contradict.
var ErrDiverged = errors.New("local L2 chain diverged from L1")

// DivergenceError locates a divergence: the batch whose evidence the local
// chain contradicts and the L2 block where it shows.
type DivergenceError struct {
	L1Block     uint64
	L1BlockHash common.Hash
	TxHash      common.Hash
	L2Block     uint64
	Reason      string
	cause       error
}

func (e *DivergenceError) Error() string {
	return fmt.Sprintf("%v at L2 block %d, batch %x in L1 block %d (%x): %s", ErrDiverged, e.L2Block, e.TxHash, e.L1Block, e.L1BlockHash, e.Reason)
}

func (e *DivergenceError) Unwrap() []error { return []error{ErrDiverged, e.cause} }
