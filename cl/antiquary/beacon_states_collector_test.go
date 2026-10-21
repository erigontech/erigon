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

package antiquary

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/common/log/v3"
)

func newBuildersList(builders ...cltypes.Builder) *solid.ListSSZ[*cltypes.Builder] {
	l := solid.NewStaticListSSZ[*cltypes.Builder](1024, new(cltypes.Builder).EncodingSizeSSZ())
	for i := range builders {
		l.Append(&builders[i])
	}
	return l
}

// Builders is a registry, so a builder that is not the last one can change in place.
func TestBuildersDiffKeepsInPlaceChange(t *testing.T) {
	collector := newBeaconStatesCollector(&clparams.MainnetBeaconConfig, t.TempDir(), log.New())
	defer collector.close()

	pre := newBuildersList(
		cltypes.Builder{Balance: 100, WithdrawableEpoch: clparams.MainnetBeaconConfig.FarFutureEpoch},
		cltypes.Builder{Balance: 200, WithdrawableEpoch: clparams.MainnetBeaconConfig.FarFutureEpoch},
	)
	post := newBuildersList(
		cltypes.Builder{Balance: 40, WithdrawableEpoch: 10},
		cltypes.Builder{Balance: 200, WithdrawableEpoch: clparams.MainnetBeaconConfig.FarFutureEpoch},
	)

	collector.buildersWriter.Initialize(pre)
	var diff bytes.Buffer
	require.NoError(t, collector.buildersWriter.WriteDiff(&diff, post))

	reconstructed := newBuildersList(*pre.Get(0), *pre.Get(1))
	require.NoError(t, base_encoding.ApplySSZQueueDiff(&diff, reconstructed, 0))

	require.Equal(t, post.Len(), reconstructed.Len())
	for i := 0; i < post.Len(); i++ {
		require.Equal(t, *post.Get(i), *reconstructed.Get(i), "builder %d", i)
	}
}

func TestBuilderPendingWithdrawalsDiffKeepsDuplicateAppend(t *testing.T) {
	collector := newBeaconStatesCollector(&clparams.MainnetBeaconConfig, t.TempDir(), log.New())
	defer collector.close()

	w := cltypes.BuilderPendingWithdrawal{Amount: 7, BuilderIndex: 1}
	newList := func(n int) *solid.ListSSZ[*cltypes.BuilderPendingWithdrawal] {
		l := solid.NewStaticListSSZ[*cltypes.BuilderPendingWithdrawal](1024, new(cltypes.BuilderPendingWithdrawal).EncodingSizeSSZ())
		for range n {
			c := w
			l.Append(&c)
		}
		return l
	}
	pre, post := newList(1), newList(2)

	collector.builderPendingWithdrawalsWriter.Initialize(pre)
	var diff bytes.Buffer
	require.NoError(t, collector.builderPendingWithdrawalsWriter.WriteDiff(&diff, post))

	reconstructed := newList(1)
	require.NoError(t, base_encoding.ApplySSZQueueDiff(&diff, reconstructed, 0))
	require.Equal(t, post.Len(), reconstructed.Len())
}
