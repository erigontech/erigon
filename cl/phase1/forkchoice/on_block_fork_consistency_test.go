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

package forkchoice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/utils"
)

// A response's decoded schema comes from the peer-chosen fork digest, so it is
// independent of the slot the block claims. Gloas removed ExecutionPayload and
// BlobKzgCommitments from BeaconBody, so a Gloas-decoded block whose slot maps
// to a pre-Gloas fork must be rejected before the pre-Gloas branch reads them.
func TestOnBlockRejectsForkSchemaSlotMismatch(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	store := buildExAnteStore(t)

	// Borrow slot/parent from a block the store already accepted, so the block
	// clears the early ancestry and timing checks and reaches version dispatch.
	ref := cltypes.NewSignedBeaconBlock(cfg, clparams.DenebVersion)
	require.NoError(t, utils.DecodeSSZSnappy(ref, diffBlock3aEnc, int(clparams.AltairVersion)))

	mismatched := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	mismatched.Block.Slot = ref.Block.Slot
	mismatched.Block.ProposerIndex = ref.Block.ProposerIndex
	mismatched.Block.ParentRoot = ref.Block.ParentRoot
	mismatched.Block.StateRoot = ref.Block.StateRoot

	require.Equal(t, clparams.GloasVersion, mismatched.Version(), "decoded schema must be Gloas")
	require.Nil(t, mismatched.Block.Body.BlobKzgCommitments, "Gloas schema leaves BlobKzgCommitments unset")
	require.Nil(t, mismatched.Block.Body.ExecutionPayload, "Gloas schema leaves ExecutionPayload unset")
	require.Less(t, cfg.GetCurrentStateVersion(mismatched.Block.Slot/cfg.SlotsPerEpoch), clparams.GloasVersion,
		"slot must map to a pre-Gloas fork for the mismatch to exist")

	err := store.OnBlock(context.Background(), mismatched, false, true, true)
	require.ErrorIs(t, err, ErrForkSchemaSlotMismatch)
}
