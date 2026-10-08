package eth_clock

import (
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

// ENR eth2 field: with no future fork scheduled the spec requires
// next_fork_version == current_fork_version, so a fork left at FAR_FUTURE_EPOCH
// must not contribute its version.
func TestForkIdNextForkVersionWithoutScheduledFork(t *testing.T) {
	for _, tc := range []struct {
		name           string
		chainID        clparams.NetworkType
		currentVersion uint32
	}{
		{"chiado", chainspec.ChiadoChainID, 0x0600006f},
		{"gnosis", chainspec.GnosisChainID, 0x06000064},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, beaconCfg := clparams.GetConfigsByNetwork(tc.chainID)
			clock := NewEthereumClock(beaconCfg.MinGenesisTime, common.Hash{}, beaconCfg)

			forkID, err := clock.ForkId()
			require.NoError(t, err)
			require.Len(t, forkID, 16)

			nextForkEpoch := binary.LittleEndian.Uint64(forkID[8:])
			require.Equal(t, beaconCfg.FarFutureEpoch, nextForkEpoch,
				"precondition: no fork is scheduled after the current one")

			require.Equal(t, common.Bytes4(utils.Uint32ToBytes4(tc.currentVersion)), common.Bytes4(forkID[4:8]),
				"next_fork_version must fall back to the current fork version")
		})
	}
}

// ENR eth2 field: ENRForkID is SSZ, so next_fork_epoch is a little-endian uint64.
func TestForkIdNextForkEpochIsLittleEndian(t *testing.T) {
	_, baseCfg := clparams.GetConfigsByNetwork(chainspec.ChiadoChainID)
	beaconCfg := *baseCfg
	beaconCfg.GloasForkEpoch = 0x0102030405
	beaconCfg.InitializeForkSchedule()
	clock := NewEthereumClock(beaconCfg.MinGenesisTime, common.Hash{}, &beaconCfg)

	forkID, err := clock.ForkId()
	require.NoError(t, err)
	require.Len(t, forkID, 16)

	require.Equal(t, common.Bytes4(utils.Uint32ToBytes4(uint32(beaconCfg.GloasForkVersion))), common.Bytes4(forkID[4:8]))
	require.Equal(t, []byte{0x05, 0x04, 0x03, 0x02, 0x01, 0x00, 0x00, 0x00}, forkID[8:16])
}

// ENR eth2 field (Fulu): next_fork_epoch counts BPO forks, but next_fork_version
// only changes at regular forks.
func TestForkIdNextForkVersionWithBPO(t *testing.T) {
	const (
		fuluVersion = 0x0600006f
		earlier     = uint64(1) << 40
		later       = uint64(1) << 41
	)
	for _, tc := range []struct {
		name        string
		bpoEpoch    uint64
		gloasEpoch  uint64
		wantEpoch   uint64
		wantVersion uint32
	}{
		{"bpo before gloas", earlier, later, earlier, fuluVersion},
		{"bpo at gloas", earlier, earlier, earlier, 0x0700006f},
		{"gloas before bpo", later, earlier, earlier, 0x0700006f},
		{"bpo without gloas", earlier, math.MaxUint64, earlier, fuluVersion},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, baseCfg := clparams.GetConfigsByNetwork(chainspec.ChiadoChainID)
			beaconCfg := *baseCfg
			beaconCfg.GloasForkVersion = 0x0700006f
			beaconCfg.GloasForkEpoch = tc.gloasEpoch
			beaconCfg.BlobSchedule = []clparams.BlobParameters{{Epoch: tc.bpoEpoch, MaxBlobsPerBlock: 9}}
			beaconCfg.InitializeForkSchedule()
			clock := NewEthereumClock(genesisAtEpoch(&beaconCfg, beaconCfg.FuluForkEpoch), common.Hash{}, &beaconCfg)

			forkID, err := clock.ForkId()
			require.NoError(t, err)
			require.Len(t, forkID, 16)
			require.Equal(t, common.Bytes4(utils.Uint32ToBytes4(tc.wantVersion)), common.Bytes4(forkID[4:8]))
			require.Equal(t, tc.wantEpoch, binary.LittleEndian.Uint64(forkID[8:16]))
		})
	}
}

// Two regular forks at one epoch: next_fork_version is the later one, as
// compute_fork_version gives at that epoch.
func TestForkIdNextForkVersionWithForksAtSameEpoch(t *testing.T) {
	const epoch = uint64(1) << 40
	_, baseCfg := clparams.GetConfigsByNetwork(chainspec.ChiadoChainID)
	beaconCfg := *baseCfg
	beaconCfg.FuluForkEpoch = epoch
	beaconCfg.GloasForkVersion = 0x0700006f
	beaconCfg.GloasForkEpoch = epoch
	beaconCfg.BlobSchedule = nil
	beaconCfg.InitializeForkSchedule()
	clock := NewEthereumClock(genesisAtEpoch(&beaconCfg, beaconCfg.ElectraForkEpoch), common.Hash{}, &beaconCfg)

	forkID, err := clock.ForkId()
	require.NoError(t, err)
	require.Len(t, forkID, 16)
	require.Equal(t, common.Bytes4(utils.Uint32ToBytes4(0x0700006f)), common.Bytes4(forkID[4:8]))
	require.Equal(t, epoch, binary.LittleEndian.Uint64(forkID[8:16]))
}

func genesisAtEpoch(cfg *clparams.BeaconChainConfig, epoch uint64) uint64 {
	return uint64(time.Now().Unix()) - epoch*cfg.SlotsPerEpoch*cfg.SecondsPerSlot
}
