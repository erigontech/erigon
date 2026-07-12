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

package snapshot_format_test

import (
	"bytes"
	_ "embed"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/format/snapshot_format"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/stretchr/testify/require"
)

//go:embed test_data/phase0.ssz_snappy
var phase0BlockSSZSnappy []byte

//go:embed test_data/altair.ssz_snappy
var altairBlockSSZSnappy []byte

//go:embed test_data/bellatrix.ssz_snappy
var bellatrixBlockSSZSnappy []byte

//go:embed test_data/capella.ssz_snappy
var capellaBlockSSZSnappy []byte

//go:embed test_data/deneb.ssz_snappy
var denebBlockSSZSnappy []byte

var emptyBlock = cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)

// obtain the test blocks
func getTestBlocks(t *testing.T) []*cltypes.SignedBeaconBlock {

	emptyBlock.EncodingSizeSSZ()

	denebBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	capellaBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.CapellaVersion)
	bellatrixBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.BellatrixVersion)
	altairBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.AltairVersion)
	phase0Block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)

	require.NoError(t, utils.DecodeSSZSnappy(denebBlock, denebBlockSSZSnappy, int(clparams.DenebVersion)))
	require.NoError(t, utils.DecodeSSZSnappy(capellaBlock, capellaBlockSSZSnappy, int(clparams.CapellaVersion)))
	require.NoError(t, utils.DecodeSSZSnappy(bellatrixBlock, bellatrixBlockSSZSnappy, int(clparams.BellatrixVersion)))
	require.NoError(t, utils.DecodeSSZSnappy(altairBlock, altairBlockSSZSnappy, int(clparams.AltairVersion)))
	require.NoError(t, utils.DecodeSSZSnappy(phase0Block, phase0BlockSSZSnappy, int(clparams.Phase0Version)))
	return []*cltypes.SignedBeaconBlock{phase0Block, altairBlock, bellatrixBlock, capellaBlock, denebBlock, emptyBlock}
}

func TestBlockSnapshotEncoding(t *testing.T) {
	for _, blk := range getTestBlocks(t) {
		var br snapshot_format.MockBlockReader
		if blk.Version() >= clparams.BellatrixVersion {
			br = snapshot_format.MockBlockReader{Block: blk.Block.Body.ExecutionPayload}
		}
		var b bytes.Buffer
		_, err := snapshot_format.WriteBlockForSnapshot(&b, blk, nil)
		require.NoError(t, err)
		blk2, err := snapshot_format.ReadBlockFromSnapshot(&b, &br, &clparams.MainnetBeaconConfig)
		require.NoError(t, err)

		hash1, err := blk.HashSSZ()
		require.NoError(t, err)
		hash2, err := blk2.HashSSZ()
		require.NoError(t, err)
		// Rewrite for header test
		b.Reset()
		_, err = snapshot_format.WriteBlockForSnapshot(&b, blk, nil)
		require.NoError(t, err)
		header, bn, bHash, err := snapshot_format.ReadBlockHeaderFromSnapshotWithExecutionData(&b, &clparams.MainnetBeaconConfig)
		require.NoError(t, err)
		hash3, err := header.HashSSZ()
		require.NoError(t, err)
		// now do it with blinded
		require.Equal(t, hash1, hash2)

		require.Equal(t, header.Signature, blk.Signature)
		require.Equal(t, header.Header.Slot, blk.Block.Slot)

		b.Reset()
		_, err = snapshot_format.WriteBlockForSnapshot(&b, blk, nil)
		require.NoError(t, err)
		blk4, err := snapshot_format.ReadBeaconBlockBodyFromSnapshot(&b, &clparams.MainnetBeaconConfig)
		require.NoError(t, err)

		hash4, err := blk4.HashSSZ()
		require.NoError(t, err)
		// ReadBeaconBlockBodyFromSnapshot does not reconstruct execution data,
		// so hash equality only holds for pre-Bellatrix blocks.
		if blk.Version() < clparams.BellatrixVersion {
			require.Equal(t, hash1, hash4)
		}

		if blk.Version() >= clparams.BellatrixVersion {
			require.Equal(t, bn, blk.Block.Body.ExecutionPayload.BlockNumber)
			require.Equal(t, bHash, blk.Block.Body.ExecutionPayload.BlockHash)
		}
		require.Equal(t, hash3, hash2)
	}
}

// TestPreMergeBellatrixBlockSkipsExecutionLookup is a regression test for the
// Caplin archive infinite retry loop at the Bellatrix transition.
//
// Pre-Merge Bellatrix blocks carry a default (all-zero) execution payload
// header, including BlockNumber=0 and BlockHash=0x0000...0000. Before the fix,
// ReadBlockFromSnapshot unconditionally tried to fetch EL transactions for any
// post-Altair block, causing "transactions not found for block 0" errors that
// looped forever during historical state reconstruction.
//
// The fix checks for a zero BlockHash (matching the consensus spec's
// is_execution_enabled semantics) and skips the EL lookup entirely.
func TestPreMergeBellatrixBlockSkipsExecutionLookup(t *testing.T) {
	// Create a Bellatrix block with a default (all-zero) execution payload.
	// This represents a pre-Merge Bellatrix block where execution is not yet enabled.
	preMergeBlock := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.BellatrixVersion)
	preMergeBlock.Block.Slot = 4636672 // First Bellatrix slot on mainnet
	// ExecutionPayload fields are already zero-valued (BlockNumber=0, BlockHash=0x0)

	// Write the block to snapshot format
	var b bytes.Buffer
	_, err := snapshot_format.WriteBlockForSnapshot(&b, preMergeBlock, nil)
	require.NoError(t, err)

	// Use a mock execution reader that panics if called.
	// If the fix is correct, ReadBlockFromSnapshot should detect the zero BlockHash
	// and skip the execution lookup entirely.
	panicReader := &panicOnCallBlockReader{}

	// Read it back — this must NOT attempt to fetch EL transactions
	readBack, err := snapshot_format.ReadBlockFromSnapshot(&b, panicReader, &clparams.MainnetBeaconConfig)
	require.NoError(t, err)
	require.NotNil(t, readBack)

	// The block should have the correct version and slot
	require.Equal(t, clparams.BellatrixVersion, readBack.Version())
	require.Equal(t, uint64(4636672), readBack.Block.Slot)

	// Execution payload should preserve zero values
	require.Equal(t, uint64(0), readBack.Block.Body.ExecutionPayload.BlockNumber)
	require.Equal(t, common.Hash{}, readBack.Block.Body.ExecutionPayload.BlockHash)
}

// panicOnCallBlockReader is a mock ExecutionBlockReaderByNumber that panics
// if Transactions or Withdrawals is called. Used to verify that the code
// correctly skips EL lookups for pre-Merge blocks.
type panicOnCallBlockReader struct{}

func (p *panicOnCallBlockReader) Transactions(number uint64, hash common.Hash) (*solid.TransactionsSSZ, error) {
	panic("Transactions should not be called for pre-Merge Bellatrix blocks with zero BlockHash")
}

func (p *panicOnCallBlockReader) Withdrawals(number uint64, hash common.Hash) (*solid.ListSSZ[*cltypes.Withdrawal], error) {
	panic("Withdrawals should not be called for pre-Merge Bellatrix blocks with zero BlockHash")
}

func (p *panicOnCallBlockReader) SetBeaconChainConfig(*clparams.BeaconChainConfig) {}

func (p *panicOnCallBlockReader) CacheBody(blockNumber uint64, transactions [][]byte, withdrawals []*types.Withdrawal) {
}
