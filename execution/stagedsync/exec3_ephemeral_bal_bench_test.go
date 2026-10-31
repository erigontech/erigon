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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/blockreplay"
	"github.com/erigontech/erigon/execution/types"
)

func BenchmarkEphemeralBALReplay(b *testing.B) {
	fx, err := blockreplay.Load(fixturePath(b))
	require.NoError(b, err)
	require.NotNil(b, fx.Outputs, "fixture missing captured outputs; recapture with `integration capture_block`")
	expected := fx.Outputs

	r, closeFn := setupEphemeralReplay(b, fx)
	defer closeFn()

	r.cfg.experimentalBAL = true
	var captured types.BlockAccessList
	r.cfg.SetBALSink(func(_ uint64, bal types.BlockAccessList) { captured = bal })

	run := func(seed types.BlockAccessList) error {
		captured = nil
		tx, doms := r.newDomains(b, fx)
		b.StartTimer()
		err := r.execSeeded(tx, doms, seed)
		b.StopTimer()
		if err == nil {
			err = r.verify(tx, doms, expected)
		}
		doms.Close()
		tx.Rollback()
		return err
	}

	require.NoError(b, run(nil))
	require.NotNil(b, captured, "no BAL derived (experimentalBAL not honored?)")
	seed := captured
	want := seed.Hash()

	b.ResetTimer()
	for range b.N {
		require.NoError(b, run(seed))
		require.NotNil(b, captured)
		require.Equal(b, want, captured.Hash(), "seeded run derived a different BAL")
	}
}
