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

package commitmenttest

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCaseRoundTripAndStateOwnership(t *testing.T) {
	c, err := Generate(MathRand(0), SequenceSpec{Kind: "incremental"})
	require.NoError(t, err)
	data, err := json.Marshal(c)
	require.NoError(t, err)
	var decoded Case
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)
	require.Equal(t, c, decoded)
	state := make(State)
	for _, round := range decoded.Rounds {
		state.Apply(round)
	}
	ops := state.Ops()
	require.Len(t, ops, 4)
	require.Equal(t, uint64(4), ops[0].Account.Nonce)
	require.Equal(t, []byte{1}, ops[1].Storage)
	require.Equal(t, []byte{3}, ops[2].Storage)
	require.Equal(t, []byte{4}, ops[3].Storage)
	ops[0].Account.Nonce = 99
	ops[1].Storage[0] = 99
	require.Equal(t, uint64(4), state.Ops()[0].Account.Nonce)
	require.Equal(t, []byte{1}, state.Ops()[1].Storage)
	decoded.Rounds[0][2].Storage[0] = 42
	require.Equal(t, []byte{1}, state.Ops()[1].Storage)
}

func TestInvalidInputs(t *testing.T) {
	_, err := Generate(Seed{Algorithm: "unknown"}, SequenceSpec{Kind: "whale", Count: 1})
	require.ErrorContains(t, err, "unsupported seed")
	_, err = Generate(MathRand(0), SequenceSpec{Kind: "unknown"})
	require.ErrorContains(t, err, "unknown sequence")
	_, err = Keys(MathRand(0), KeySpec{Kind: "random-distinct", Size: 1, Count: 257})
	require.ErrorContains(t, err, "too many distinct keys")
	_, err = Paths(Shape{Prefixes: [][]byte{{16}}})
	require.ErrorContains(t, err, "invalid nibble")
	var malformed Case
	err = json.Unmarshal([]byte("{"), &malformed)
	require.Error(t, err)
}

func TestStateEmptyStorageRequiresExplicitDelete(t *testing.T) {
	state := make(State)
	key := make([]byte, 52)
	state.Apply([]Op{{Key: key, Storage: []byte{1}}})
	state.Apply([]Op{{Key: key, Storage: []byte{}}})
	require.Equal(t, []Op{{Key: key, Storage: []byte{}}}, state.Ops())
	state.Apply([]Op{{Key: key, Delete: true}})
	require.Empty(t, state.Ops())
}
