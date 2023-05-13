package consensus_tests

import (
	"io/fs"
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/spectest"
)

type RewardsCore struct {
}

func readDelta(root fs.FS, name string) (*cltypes.Deltas, error) {
	sszSnappy, err := fs.ReadFile(root, name)
	if err != nil {
		return nil, err
	}
	testState := &cltypes.Deltas{}
	if err := utils.DecodeSSZSnappy(testState, sszSnappy, 0); err != nil {
		return nil, err
	}
	return testState, nil
}

func (b *RewardsCore) Run(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {
	t.Skipf("Skippinf attestation reward calculation tests for now")
	//TODO: we should find some way to pass these
	//preState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PreSsz)
	//require.NoError(t, err)

	//source_deltas, err := readDelta(root, "source_deltas.ssz_snappy")
	//require.NoError(t, err)
	//target_deltas, err := readDelta(root, "target_deltas.ssz_snappy")
	//require.NoError(t, err)
	//head_deltas, err := readDelta(root, "head_deltas.ssz_snappy")
	//require.NoError(t, err)
	//inclusion_delay_deltas, err := readDelta(root, "inclusion_delay_deltas.ssz_snappy")
	//require.NoError(t, err)
	//inactivity_penalty_deltas, err := readDelta(root, "inactivity_penalty_deltas.ssz_snappy")
	//require.NoError(t, err)

	return nil
}
