package consensus_tests

import (
	"io/fs"
	"testing"

	"github.com/ledgerwatch/erigon/spectest"
)

type RewardsCore struct {
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
