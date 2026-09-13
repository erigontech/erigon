package consensus_tests

import (
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/spectest/spectest"
	"github.com/erigontech/erigon/cl/transition/machine"
	"github.com/erigontech/erigon/cl/utils"
)

type attestationOperationMachine struct {
	machine.Interface
	called     bool
	stateSlot  uint64
	headerSlot uint64
	bidSlot    uint64
	parentSlot uint64
}

func (m *attestationOperationMachine) ProcessAttestations(s abstract.BeaconState, _ *solid.ListSSZ[*solid.Attestation], parentSlot uint64) error {
	m.called = true
	m.stateSlot = s.Slot()
	m.headerSlot = s.LatestBlockHeader().Slot
	m.bidSlot = s.GetLatestExecutionPayloadBid().Slot
	m.parentSlot = parentSlot
	return nil
}

func TestOperationAttestationParentSlot(t *testing.T) {
	tests := []struct {
		name       string
		version    clparams.StateVersion
		stateSlot  uint64
		headerSlot uint64
		bidSlot    uint64
		wantSlot   uint64
	}{
		{"gloas different header and bid", clparams.GloasVersion, 65, 63, 0, 63},
		{"fulu", clparams.FuluVersion, 65, 63, 0, 0},
		{"gloas genesis", clparams.GloasVersion, 0, 0, 0, 0},
		{"gloas equal header and bid", clparams.GloasVersion, 65, 63, 63, 63},
		{"gloas zero header with nonzero bid", clparams.GloasVersion, 65, 0, 63, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			preState := state.New(&cfg)
			preState.SetVersion(test.version)
			require.NoError(t, preState.SetSlot(test.stateSlot))
			preState.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{Slot: test.headerSlot})
			preState.SetLatestExecutionPayloadBid(&cltypes.ExecutionPayloadBid{
				Slot:               test.bidSlot,
				BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
			})
			preState.SetLatestExecutionPayloadHeader(cltypes.NewEth1Header(test.version))
			stateBytes, err := utils.EncodeSSZSnappy(preState)
			require.NoError(t, err)
			att := &solid.Attestation{
				AggregationBits: solid.BitlistFromBytes([]byte{1}, int(cfg.MaxCommitteesPerSlot)*2048),
				Data:            &solid.AttestationData{},
				CommitteeBits:   solid.NewBitVector(int(cfg.MaxCommitteesPerSlot)),
			}
			attBytes, err := utils.EncodeSSZSnappy(att)
			require.NoError(t, err)
			root := fstest.MapFS{
				"pre.ssz_snappy":    {Data: stateBytes},
				"post.ssz_snappy":   {Data: stateBytes},
				attestationFileName: {Data: attBytes},
			}
			m := &attestationOperationMachine{}
			require.NoError(t, operationAttestationHandler(t, root, spectest.TestCase{
				ForkPhaseName: test.version.String(),
				Machine:       m,
			}))
			require.True(t, m.called)
			t.Logf("decoded state slot=%d header slot=%d bid slot=%d; callback parent slot=%d", m.stateSlot, m.headerSlot, m.bidSlot, m.parentSlot)
			require.Equal(t, test.stateSlot, m.stateSlot)
			require.Equal(t, test.headerSlot, m.headerSlot)
			if test.version >= clparams.GloasVersion {
				require.Equal(t, test.bidSlot, m.bidSlot)
			}
			require.Equal(t, test.wantSlot, m.parentSlot)
		})
	}
}
