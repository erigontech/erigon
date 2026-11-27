// Copyright 2024 The Erigon Authors
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

package state_accessors

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/stretchr/testify/require"
)

func TestEpochData(t *testing.T) {
	e := &EpochData{
		TotalActiveBalance:          123,
		JustificationBits:           &cltypes.JustificationBits{true},
		CurrentJustifiedCheckpoint:  solid.Checkpoint{Epoch: 123},
		PreviousJustifiedCheckpoint: solid.Checkpoint{Epoch: 123},
		FinalizedCheckpoint:         solid.Checkpoint{Epoch: 123},
		HistoricalSummariesLength:   235,
		HistoricalRootsLength:       345,
	}
	var b bytes.Buffer
	if err := e.Serialize(&b); err != nil {
		t.Fatal(err)
	}

	e2 := &EpochData{}
	if err := e2.Deserialize(&b); err != nil {
		t.Fatal(err)
	}

	require.Equal(t, e, e2)
}

func TestEpochDataFromBeaconStateTotalActiveBalance(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	s := state.New(cfg)
	require.NoError(t, s.SetSlot(5*cfg.SlotsPerEpoch))
	epoch := state.Epoch(s)

	const never = ^uint64(0)
	type validatorSpec struct{ balance, activation, exit uint64 }
	specs := []validatorSpec{
		{32_000_000_000, 0, never},
		{31_000_000_000, 0, never},
		{17_000_000_000, epoch + 1, never}, // not activated yet
		{30_000_000_000, 0, epoch},         // already exited
		{29_000_000_000, 1, epoch + 3},
		{28_000_000_000, never, never},
		{27_000_000_000, epoch, never},
	}
	var want uint64
	for i, spec := range specs {
		var pk [48]byte
		binary.BigEndian.PutUint64(pk[:], uint64(i))
		v := solid.NewValidator()
		v.SetPublicKey(pk)
		v.SetEffectiveBalance(spec.balance)
		v.SetActivationEpoch(spec.activation)
		v.SetExitEpoch(spec.exit)
		require.NoError(t, s.AddValidator(v, spec.balance))
		if v.Active(epoch) {
			want += v.EffectiveBalance()
		}
	}

	got := EpochDataFromBeaconState(s)
	require.NotNil(t, got)
	require.Equal(t, want, got.TotalActiveBalance)

	ref := *got
	ref.TotalActiveBalance = want
	var gotBytes, refBytes bytes.Buffer
	require.NoError(t, got.Serialize(&gotBytes))
	require.NoError(t, ref.Serialize(&refBytes))
	require.Equal(t, refBytes.Bytes(), gotBytes.Bytes())
}
