package consensus_tests

import (
	"context"
	"fmt"
	"io/fs"
	"testing"

	"github.com/ledgerwatch/erigon/cl/abstract"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/spectest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	return false
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
	anchorBlock, err := spectest.ReadAnchorBlock(root, c.Version(), "anchor_block.ssz_snappy")
	require.NoError(t, err)

	// TODO: what to do with anchor block ?
	_ = anchorBlock

	anchorState, err := spectest.ReadBeaconState(root, c.Version(), "anchor_state.ssz_snappy")
	require.NoError(t, err)

	forkStore, err := forkchoice.NewForkChoiceStore(context.Background(), anchorState, nil, nil, false)
	require.NoError(t, err)

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
			err = forkStore.OnAttesterSlashing(data)
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
			err = forkStore.OnBlock(blk, true, true)
			if step.GetValid() {
				require.NoError(t, err, stepstr)
			} else {
				require.Error(t, err, stepstr)
			}
		case "attestation":
			att := &solid.Attestation{}
			err := spectest.ReadSsz(root, c.Version(), step.GetAttestation()+".ssz_snappy", att)
			require.NoError(t, err, stepstr)
			err = forkStore.OnAttestation(att, false)
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
