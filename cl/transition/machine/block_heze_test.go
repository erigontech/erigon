package machine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	corestate "github.com/erigontech/erigon/cl/phase1/core/state"
)

type eth1DataCounter struct {
	parentSlotProcessor
	calls int
}

func (c *eth1DataCounter) ProcessEth1Data(abstract.BeaconState, *cltypes.Eth1Data) error {
	c.calls++
	return nil
}

func TestProcessBlockProcessesEth1DataOnlyBeforeHeze(t *testing.T) {
	for _, test := range []struct {
		version clparams.StateVersion
		calls   int
	}{
		{clparams.GloasVersion, 1},
		{clparams.HezeVersion, 0},
	} {
		t.Run(test.version.String(), func(t *testing.T) {
			s, block := stateAndBlockAfterParentSlot(t, test.version)
			p := &eth1DataCounter{}

			require.NoError(t, ProcessBlock(p, s, block))
			require.Equal(t, test.calls, p.calls)
		})
	}
}

func TestProcessOperationsDepositsFieldOnlyBeforeHeze(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	run := func(version clparams.StateVersion, body *cltypes.BeaconBody) error {
		s := corestate.New(&cfg)
		s.SetVersion(version)
		_, _, _, err := ProcessOperations(noopBlockOperationProcessor{}, s, body, 0)
		return err
	}

	for _, version := range []clparams.StateVersion{clparams.FuluVersion, clparams.GloasVersion} {
		body := cltypes.NewBeaconBody(&cfg, version)
		body.Deposits.Append(&cltypes.Deposit{})
		require.ErrorContains(t, run(version, body), "old-style deposits")
	}

	body := cltypes.NewBeaconBody(&cfg, clparams.HezeVersion)
	body.Deposits = nil
	require.NoError(t, run(clparams.HezeVersion, body))
}
