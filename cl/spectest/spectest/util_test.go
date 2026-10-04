package spectest

import (
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
)

func TestReadBeaconStateUsesCaseForkEpoch(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.GloasForkEpoch = 1
	st := state.New(&cfg)
	st.SetVersion(clparams.GloasVersion)
	require.NoError(t, st.SetSlot(cfg.SlotsPerEpoch))
	encoded, err := utils.EncodeSSZSnappy(st)
	require.NoError(t, err)

	decoded, err := ReadBeaconState(fstest.MapFS{
		"pre.ssz_snappy": {Data: encoded},
		"config.yaml":    {Data: []byte("GLOAS_FORK_EPOCH: 1\n")},
	}, clparams.GloasVersion, "pre.ssz_snappy")
	require.NoError(t, err)
	require.Equal(t, uint64(1), decoded.BeaconConfig().GloasForkEpoch)
}
