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

package consensus_tests

import (
	"context"
	"fmt"
	"io/fs"
	"math"
	"testing"

	"github.com/erigontech/erigon/spectest"

	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon/cl/cltypes"
)

func (f *ForkChoiceStep) StepType() string {
	if f.PayloadStatus != nil {
		return "on_payload_info"
	}
	if f.AttesterSlashing != nil {
		return "on_attester_slashing"
	}
	if f.PowBlock != nil {
		return "on_merge_block"
	}
	if f.Block != nil {
		return "on_block"
	}
	if f.Attestation != nil {
		return "attestation"
	}
	if f.Tick != nil {
		return "on_tick"
	}
	if f.Checks != nil {
		return "checks"
	}
	return "unknown"
}

type ForkChoiceStep struct {
	Tick             *int                     `yaml:"tick,omitempty"`
	Valid            *bool                    `yaml:"valid,omitempty"`
	Attestation      *string                  `yaml:"attestation,omitempty"`
	Block            *string                  `yaml:"block,omitempty"`
	Blobs            *string                  `yaml:"blobs,omitempty"`
	Proofs           []string                 `yaml:"proofs,omitempty"`
	PowBlock         *string                  `yaml:"pow_block,omitempty"`
	AttesterSlashing *string                  `yaml:"attester_slashing,omitempty"`
	BlockHash        *string                  `yaml:"block_hash,omitempty"`
	PayloadStatus    *ForkChoicePayloadStatus `yaml:"payload_status,omitempty"`
	Checks           *ForkChoiceChecks        `yaml:"checks,omitempty"`
}

func (f *ForkChoiceStep) GetTick() int {
	if f.Tick == nil {
		return 0
	}
	return *f.Tick
}
func (f *ForkChoiceStep) GetValid() bool {
	if f.Valid == nil {
		return true
	}
	return *f.Valid
}
func (f *ForkChoiceStep) GetAttestation() string {
	if f.Attestation == nil {
		return ""
	}
	return *f.Attestation
}
func (f *ForkChoiceStep) GetBlock() string {
	if f.Block == nil {
		return ""
	}
	return *f.Block
}

func (f *ForkChoiceStep) GetBlobs() string {
	if f.Blobs == nil {
		return ""
	}
	return *f.Blobs
}

func (f *ForkChoiceStep) GetPowBlock() string {
	if f.PowBlock == nil {
		return ""
	}
	return *f.PowBlock
}
func (f *ForkChoiceStep) GetAttesterSlashing() string {
	if f.AttesterSlashing == nil {
		return ""
	}
	return *f.AttesterSlashing
}
func (f *ForkChoiceStep) GetBlockHash() string {
	if f.BlockHash == nil {
		return ""
	}
	return *f.BlockHash
}
func (f *ForkChoiceStep) GetPayloadStatus() *ForkChoicePayloadStatus {
	if f.PayloadStatus == nil {
		return nil
	}
	return f.PayloadStatus
}
func (f *ForkChoiceStep) GetChecks() *ForkChoiceChecks {
	if f.Checks == nil {
		return nil
	}
	return f.Checks
}

type ForkChoiceChecks struct {
	Head *struct {
		Slot *int         `yaml:"slot,omitempty"`
		Root *common.Hash `yaml:"root,omitempty"`
	} `yaml:"head,omitempty"`
	Time                *int `yaml:"time,omitempty"`
	GenesisTime         *int `yaml:"genesis_time,omitempty"`
	JustifiedCheckpoint *struct {
		Epoch *int         `yaml:"epoch,omitempty"`
		Root  *common.Hash `yaml:"root,omitempty"`
	} `yaml:"justified_checkpoint,omitempty"`

	FinalizedCheckpoint *struct {
		Epoch *int         `yaml:"epoch,omitempty"`
		Root  *common.Hash `yaml:"root,omitempty"`
	} `yaml:"finalized_checkpoint,omitempty"`
	ProposerBoostRoot *common.Hash `yaml:"proposer_boost_root,omitempty"`
}

type ForkChoicePayloadStatus struct {
	Status          string `yaml:"status"`
	LatestValidHash string `yaml:"latest_valid_hash"`
	ValidationError string `yaml:"validation_error"`
}

type ForkChoice struct {
}

func NewForkChoice(fn func(s abstract.BeaconState) error) *ForkChoice {
	return &ForkChoice{}
}

func (b *ForkChoice) Run(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {
	ctx := context.Background()

	anchorBlock, err := spectest.ReadAnchorBlock(root, c.Version(), "anchor_block.ssz_snappy")
	require.NoError(t, err)

	// TODO: what to do with anchor block ?
	_ = anchorBlock

	anchorState, err := spectest.ReadBeaconState(root, c.Version(), "anchor_state.ssz_snappy")
	require.NoError(t, err)

	genesisState, err := initial_state.GetGenesisState(clparams.MainnetNetwork)
	require.NoError(t, err)

	emitters := beaconevents.NewEventEmitter()
	_, beaconConfig := clparams.GetConfigsByNetwork(clparams.MainnetNetwork)
	ethClock := eth_clock.NewEthereumClock(genesisState.GenesisTime(), genesisState.GenesisValidatorsRoot(), beaconConfig)
	blobStorage := blob_storage.NewBlobStore(memdb.New("/tmp"), afero.NewMemMapFs(), math.MaxUint64, &clparams.MainnetBeaconConfig, ethClock)

	validatorMonitor := monitor.NewValidatorMonitor(false, nil, nil, nil)
	forkStore, err := forkchoice.NewForkChoiceStore(
		ethClock, anchorState, nil, pool.NewOperationsPool(&clparams.MainnetBeaconConfig),
		fork_graph.NewForkGraphDisk(anchorState, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{}, emitters),
		emitters, synced_data.NewSyncedDataManager(true, &clparams.MainnetBeaconConfig), blobStorage, validatorMonitor)
	require.NoError(t, err)
	forkStore.SetSynced(true)

	var steps []ForkChoiceStep
	err = spectest.ReadYml(root, "steps.yaml", &steps)
	require.NoError(t, err)

	for stepIdx, step := range steps {
		stepstr := fmt.Sprintf("step %d: %s", stepIdx, step.StepType())
		switch step.StepType() {
		case "on_payload_info":
			// we are not doing engine level mocking, so simply return the test when this is received
			return nil
		case "on_attester_slashing":
			data := &cltypes.AttesterSlashing{}
			err := spectest.ReadSsz(root, c.Version(), step.GetAttesterSlashing()+".ssz_snappy", data)
			require.NoError(t, err, stepstr)
			err = forkStore.OnAttesterSlashing(data, false)
			if step.GetValid() {
				require.NoError(t, err, stepstr)
			} else {
				require.Error(t, err, stepstr)
			}
		case "on_merge_block":
			return nil
		case "on_block":
			blk := cltypes.NewSignedBeaconBlock(anchorState.BeaconConfig())
			err := spectest.ReadSsz(root, c.Version(), step.GetBlock()+".ssz_snappy", blk)
			require.NoError(t, err, stepstr)
			blobs := solid.NewStaticListSSZ[*cltypes.Blob](6, len(cltypes.Blob{}))
			if step.GetBlobs() != "" {
				err := spectest.ReadSsz(root, c.Version(), step.GetBlobs()+".ssz_snappy", blobs)
				require.NoError(t, err, stepstr)
				if step.GetValid() {
					require.False(t, len(step.Proofs) != blobs.Len() || len(step.Proofs) != blk.Block.Body.BlobKzgCommitments.Len(), "invalid number of proofs")
				} else {
					if len(step.Proofs) != blobs.Len() || len(step.Proofs) != blk.Block.Body.BlobKzgCommitments.Len() {
						continue
					}
				}
				blobSidecarService := services.NewBlobSidecarService(ctx, &clparams.MainnetBeaconConfig, forkStore, nil, ethClock, emitters, true)

				blobs.Range(func(index int, value *cltypes.Blob, length int) bool {
					var proof libcommon.Bytes48
					proofStr := step.Proofs[index]
					proofBytes := common.Hex2Bytes(proofStr[2:])
					copy(proof[:], proofBytes)
					err = blobSidecarService.ProcessMessage(ctx, nil, &cltypes.BlobSidecar{
						Index:             uint64(index),
						SignedBlockHeader: blk.SignedBeaconBlockHeader(),
						Blob:              *value,
						KzgCommitment:     common.Bytes48(*blk.Block.Body.BlobKzgCommitments.Get(index)),
						KzgProof:          proof,
					})
					return true
				})

			}

			err = forkStore.OnBlock(ctx, blk, true, true, true)
			if step.GetValid() {
				require.NoError(t, err, stepstr)
			} else {
				require.Error(t, err, stepstr)
			}
		case "attestation":
			att := &solid.Attestation{}
			err := spectest.ReadSsz(root, c.Version(), step.GetAttestation()+".ssz_snappy", att)
			require.NoError(t, err, stepstr)
			err = forkStore.OnAttestation(att, false, false)
			if step.GetValid() {
				require.NoError(t, err, stepstr)
			} else {
				require.Error(t, err, stepstr)
			}
		case "on_tick":
			forkStore.OnTick(uint64(step.GetTick()))
			//TODO: onTick needs to be able to return error
		//	if step.GetValid() {
		//		require.NoError(t, err)
		//	} else {
		//		require.Error(t, err)
		//	}
		case "checks":
			chk := step.GetChecks()
			doCheck(t, stepstr, forkStore, chk)
		default:
		}
	}

	return nil
}

func doCheck(t *testing.T, stepstr string, store *forkchoice.ForkChoiceStore, e *ForkChoiceChecks) {
	if e.Head != nil {
		root, v, err := store.GetHead()
		require.NoError(t, err, stepstr)
		if e.Head.Root != nil {
			assert.EqualValues(t, *e.Head.Root, root, stepstr)
		}
		if e.Head.Slot != nil {
			assert.EqualValues(t, *e.Head.Slot, int(v), stepstr)
		}
	}
	if e.Time != nil {
		assert.EqualValues(t, *e.Time, store.Time(), stepstr)
	}
	/*if e.GenesisTime != nil {
		// TODO:  what value to use here??
		//assert.EqualValues(t, e.Time, store.GenesisTime())
	}*/
	if e.ProposerBoostRoot != nil {
		assert.EqualValues(t, *e.ProposerBoostRoot, store.ProposerBoostRoot(), stepstr)
	}

	if e.FinalizedCheckpoint != nil {
		cp := store.FinalizedCheckpoint()
		if e.FinalizedCheckpoint.Root != nil {
			assert.EqualValues(t, *e.FinalizedCheckpoint.Root, cp.BlockRoot(), stepstr)
		}
		if e.FinalizedCheckpoint.Epoch != nil {
			assert.EqualValues(t, *e.FinalizedCheckpoint.Epoch, cp.Epoch(), stepstr)
		}
	}
	if e.JustifiedCheckpoint != nil {
		cp := store.JustifiedCheckpoint()
		if e.JustifiedCheckpoint.Root != nil {
			assert.EqualValues(t, *e.JustifiedCheckpoint.Root, cp.BlockRoot(), stepstr)
		}
		if e.JustifiedCheckpoint.Epoch != nil {
			assert.EqualValues(t, *e.JustifiedCheckpoint.Epoch, cp.Epoch(), stepstr)
		}
	}
}
