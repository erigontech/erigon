package forkchoice_test

import (
	_ "embed"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/forkchoice"
	"github.com/stretchr/testify/require"
)

//go:embed test_data/anchor_state.ssz_snappy
var anchorStateEncoded []byte

//go:embed test_data/block_0x3af8b5b42ca135c75b32abb32b3d71badb73695d3dc638bacfb6c8b7bcbee1a9.ssz_snappy
var block3aEncoded []byte

//go:embed test_data/block_0xc2788d6005ee2b92c3df2eff0aeab0374d155fa8ca1f874df305fa376ce334cf.ssz_snappy
var blockc2Encoded []byte

//go:embed test_data/block_0xd4503d46e43df56de4e19acb0f93b3b52087e422aace49a7c3816cf59bafb0ad.ssz_snappy
var blockd4Encoded []byte

//go:embed test_data/attestation_0xfb924d35b2888d9cd70e6879c1609e6cad7ea3b028a501967747d96e49068cb6.ssz_snappy
var attestationEncoded []byte

// this is consensus spec test altair/forkchoice/ex_ante/ex_ante_attestations_is_greater_than_proposer_boost_with_boost
func TestForkChoiceBasic(t *testing.T) {
	expectedCheckpoint := &cltypes.Checkpoint{
		Epoch: 0,
		Root:  libcommon.HexToHash("0x564d76d91f66c1fb2977484a6184efda2e1c26dd01992e048353230e10f83201"),
	}
	// Decode test blocks
	block0x3a, block0xc2, block0xd4 := &cltypes.SignedBeaconBlock{}, &cltypes.SignedBeaconBlock{}, &cltypes.SignedBeaconBlock{}
	require.NoError(t, utils.DecodeSSZSnappyWithVersion(block0x3a, block3aEncoded, int(clparams.AltairVersion)))
	require.NoError(t, utils.DecodeSSZSnappyWithVersion(block0xc2, blockc2Encoded, int(clparams.AltairVersion)))
	require.NoError(t, utils.DecodeSSZSnappyWithVersion(block0xd4, blockd4Encoded, int(clparams.AltairVersion)))
	// decode test attestation
	testAttestation := &cltypes.Attestation{}
	require.NoError(t, utils.DecodeSSZSnappyWithVersion(testAttestation, attestationEncoded, int(clparams.AltairVersion)))
	// Initialize forkchoice store
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappyWithVersion(anchorState, anchorStateEncoded, int(clparams.AltairVersion)))
	store, err := forkchoice.NewForkChoiceStore(anchorState, nil)
	require.NoError(t, err)
	// first steps
	store.OnTick(0)
	store.OnTick(12)
	require.NoError(t, store.OnBlock(block0x3a, true))
	// Check if we get correct status (1)
	require.Equal(t, store.Time(), uint64(12))
	require.Equal(t, store.ProposerBoostRoot(), libcommon.HexToHash("0xc9bd7bcb6dfa49dc4e5a67ca75e89062c36b5c300bc25a1b31db4e1a89306071"))
	require.Equal(t, store.JustifiedCheckpoint(), expectedCheckpoint)
	require.Equal(t, store.FinalizedCheckpoint(), expectedCheckpoint)
	headRoot, headSlot, err := store.GetHead()
	require.NoError(t, err)
	require.Equal(t, headSlot, uint64(1))
	require.Equal(t, headRoot, libcommon.HexToHash("0xc9bd7bcb6dfa49dc4e5a67ca75e89062c36b5c300bc25a1b31db4e1a89306071"))
	// process another tick and another block
	store.OnTick(36)
	require.NoError(t, store.OnBlock(block0xc2, true))
	// Check if we get correct status (2)
	require.Equal(t, store.Time(), uint64(36))
	require.Equal(t, store.ProposerBoostRoot(), libcommon.HexToHash("0x744cc484f6503462f0f3a5981d956bf4fcb3e57ab8687ed006467e05049ee033"))
	require.Equal(t, store.JustifiedCheckpoint(), expectedCheckpoint)
	require.Equal(t, store.FinalizedCheckpoint(), expectedCheckpoint)
	headRoot, headSlot, err = store.GetHead()
	require.NoError(t, err)
	require.Equal(t, headSlot, uint64(3))
	require.Equal(t, headRoot, libcommon.HexToHash("0x744cc484f6503462f0f3a5981d956bf4fcb3e57ab8687ed006467e05049ee033"))
	// last block
	require.NoError(t, store.OnBlock(block0xd4, true))
	require.Equal(t, store.Time(), uint64(36))
	require.Equal(t, store.ProposerBoostRoot(), libcommon.HexToHash("0x744cc484f6503462f0f3a5981d956bf4fcb3e57ab8687ed006467e05049ee033"))
	require.Equal(t, store.JustifiedCheckpoint(), expectedCheckpoint)
	require.Equal(t, store.FinalizedCheckpoint(), expectedCheckpoint)
	headRoot, headSlot, err = store.GetHead()
	require.NoError(t, err)
	require.Equal(t, headSlot, uint64(3))
	require.Equal(t, headRoot, libcommon.HexToHash("0x744cc484f6503462f0f3a5981d956bf4fcb3e57ab8687ed006467e05049ee033"))
	// lastly do attestation
	require.NoError(t, store.OnAttestation(testAttestation, false))
}
