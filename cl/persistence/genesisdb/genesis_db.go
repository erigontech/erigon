package genesisdb

import (
	"fmt"

	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/raw"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/spf13/afero"
)

const genesisStateFileName = "genesis_state.ssz_snappy"

/*
* GenesisDB only keeps track of one file
* genesis_state.ssz_snappy
* This DB is static and only used to store the genesis state, it is write-once and read-always.
 */
type genesisDB struct {
	fs           afero.Fs // Use afero to make it easier to test.
	beaconConfig *clparams.BeaconChainConfig
}

func NewGenesisDB(beaconConfig *clparams.BeaconChainConfig, genesisDBPath string) GenesisDB {
	return &genesisDB{
		fs:           afero.NewBasePathFs(afero.NewOsFs(), genesisDBPath),
		beaconConfig: beaconConfig,
	}
}

func (g *genesisDB) IsInitialized() (bool, error) {
	return afero.Exists(g.fs, genesisStateFileName)
}

func (g *genesisDB) Initialize(state *state.CachingBeaconState) error {
	initialized, err := g.IsInitialized()
	if err != nil {
		return err
	}
	// No need to initialize.
	if initialized || state == nil {
		return nil
	}
	enc, err := state.EncodeSSZ(nil)
	if err != nil {
		return err
	}
	return afero.WriteFile(g.fs, genesisStateFileName, utils.CompressSnappy(enc), 0644)
}

func (g *genesisDB) ReadGenesisState() (*state.CachingBeaconState, error) {
	enc, err := afero.ReadFile(g.fs, genesisStateFileName)
	if err != nil {
		return nil, err
	}

	decompressedEnc, err := utils.DecompressSnappy(enc, false)
	if err != nil {
		return nil, err
	}

	// Ensure decompressedEnc has enough bytes to safely slice
	if len(decompressedEnc) < raw.SlotOffsetSSZ+8 {
		return nil, fmt.Errorf("decompressedEnc is too short: expected at least %d bytes, got %d", raw.SlotOffsetSSZ+8, len(decompressedEnc))
	}
	// get slot from the state
	slot := ssz.Uint64SSZDecode(decompressedEnc[raw.SlotOffsetSSZ : raw.SlotOffsetSSZ+8])
	version := g.beaconConfig.GetCurrentStateVersion(slot / g.beaconConfig.SlotsPerEpoch)
	st := state.New(g.beaconConfig)
	if err := st.DecodeSSZ(decompressedEnc, int(version)); err != nil {
		return nil, fmt.Errorf("could not deserialize state: %s", err)
	}
	return st, nil
}
