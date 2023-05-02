package consensustests

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/transition"
	"gopkg.in/yaml.v2"
)

type transitionMeta struct {
	ForkEpoch uint64 `yaml:"fork_epoch"`
}

func transitionTestFunction(context testContext) error {
	metaBytes, err := os.ReadFile("meta.yaml")
	if err != nil {
		return err
	}
	meta := transitionMeta{}
	if err := yaml.Unmarshal(metaBytes, &meta); err != nil {
		return err
	}
	contextPrev := context
	contextPrev.version--
	testState, err := decodeStateFromFile(contextPrev, "pre.ssz_snappy")
	if err != nil {
		return err
	}

	expectedState, err := decodeStateFromFile(context, "post.ssz_snappy")
	if err != nil {
		return err
	}
	switch context.version {
	case clparams.AltairVersion:
		testState.BeaconConfig().AltairForkEpoch = meta.ForkEpoch
	case clparams.BellatrixVersion:
		testState.BeaconConfig().BellatrixForkEpoch = meta.ForkEpoch
	case clparams.CapellaVersion:
		testState.BeaconConfig().CapellaForkEpoch = meta.ForkEpoch
	}
	startSlot := testState.Slot()
	blockIndex := 0
	for {
		testSlot, err := testBlockSlot(blockIndex)
		if err != nil {
			return err
		}
		var block *cltypes.SignedBeaconBlock
		if testSlot/clparams.MainnetBeaconConfig.SlotsPerEpoch >= meta.ForkEpoch {
			block, err = testBlock(context, blockIndex)
			if err != nil {
				return err
			}
		} else {
			block, err = testBlock(contextPrev, blockIndex)
			if err != nil {
				return err
			}
		}

		if block == nil {
			break
		}

		blockIndex++

		if err := transition.TransitionState(testState, block, true); err != nil {
			return fmt.Errorf("cannot transition state: %s. slot=%d. start_slot=%d", err, block.Block.Slot, startSlot)
		}
	}
	expectedRoot, err := expectedState.HashSSZ()
	if err != nil {
		return err
	}
	haveRoot, err := testState.HashSSZ()
	if err != nil {
		return err
	}
	if haveRoot != expectedRoot {
		return fmt.Errorf("mismatching state roots")
	}
	return nil
}
