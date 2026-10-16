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
package stagedsync

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSingleBlockCycleDone(t *testing.T) {
	one := execRange{blockNum: 10, maxBlockNum: 10}
	many := execRange{blockNum: 10, maxBlockNum: 20}
	exhausted := &ErrLoopExhausted{From: 10, To: 10, Reason: "block limit reached"}

	require.True(t, singleBlockCycleDone(nil, one, 0))
	require.True(t, singleBlockCycleDone(exhausted, many, 1), "chain-tip cycles end with ErrLoopExhausted")
	require.False(t, singleBlockCycleDone(nil, many, 0))
	require.False(t, singleBlockCycleDone(errors.New("boom"), one, 1))
}
