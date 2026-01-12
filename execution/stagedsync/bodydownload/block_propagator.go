// Copyright 2025 The Erigon Authors
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

<<<<<<<< HEAD:erigon-lib/synctest/synctest_go_1_25_and_beyond.go
//go:build go1.25

package synctest

import "testing/synctest"

var Test testFunc = synctest.Test
========
package bodydownload

import (
	"context"
	"math/big"

	"github.com/erigontech/erigon/execution/types"
)

type BlockPropagator func(ctx context.Context, header *types.Header, body *types.RawBody, td *big.Int)
>>>>>>>> main:execution/stagedsync/bodydownload/block_propagator.go
