package machine

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	corestate "github.com/erigontech/erigon/cl/phase1/core/state"
)

type noopBlockOperationProcessor struct{}

func (noopBlockOperationProcessor) ProcessProposerSlashing(abstract.BeaconState, *cltypes.ProposerSlashing) error {
	return nil
}
func (noopBlockOperationProcessor) ProcessAttesterSlashing(abstract.BeaconState, *cltypes.AttesterSlashing) error {
	return nil
}
func (noopBlockOperationProcessor) ProcessAttestations(abstract.BeaconState, *solid.ListSSZ[*solid.Attestation]) error {
	return nil
}
func (noopBlockOperationProcessor) ProcessDeposit(abstract.BeaconState, *cltypes.Deposit) error {
	return nil
}
func (noopBlockOperationProcessor) ProcessVoluntaryExit(abstract.BeaconState, *cltypes.SignedVoluntaryExit) error {
	return nil
}
func (noopBlockOperationProcessor) ProcessBlsToExecutionChange(abstract.BeaconState, *cltypes.SignedBLSToExecutionChange) error {
	return nil
}
func (noopBlockOperationProcessor) ProcessDepositRequest(abstract.BeaconState, *solid.DepositRequest) error {
	return nil
}
func (noopBlockOperationProcessor) ProcessWithdrawalRequest(abstract.BeaconState, *solid.WithdrawalRequest) error {
	return nil
}
func (noopBlockOperationProcessor) ProcessConsolidationRequest(abstract.BeaconState, *solid.ConsolidationRequest) error {
	return nil
}
func (noopBlockOperationProcessor) ProcessBuilderDepositRequest(abstract.BeaconState, *solid.BuilderDepositRequest) error {
	return nil
}
func (noopBlockOperationProcessor) ProcessBuilderExitRequest(abstract.BeaconState, *solid.BuilderExitRequest) error {
	return nil
}
func (noopBlockOperationProcessor) ProcessPayloadAttestation(abstract.BeaconState, *cltypes.PayloadAttestation) error {
	return nil
}
func (noopBlockOperationProcessor) FullValidate() bool { return false }

func TestProcessOperationsRejectsOversizedGloasLists(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	tests := []struct {
		name   string
		limit  uint64
		append func(*cltypes.BeaconBody)
	}{
		{"proposer slashings", cfg.MaxProposerSlashings, func(body *cltypes.BeaconBody) {
			body.ProposerSlashings.Append(&cltypes.ProposerSlashing{})
		}},
		{"attester slashings", cfg.MaxAttesterSlashingsElectra, func(body *cltypes.BeaconBody) {
			body.AttesterSlashings.Append(&cltypes.AttesterSlashing{})
		}},
		{"attestations", cfg.MaxAttestationsElectra, func(body *cltypes.BeaconBody) {
			body.Attestations.Append(&solid.Attestation{})
		}},
		{"voluntary exits", cfg.MaxVoluntaryExits, func(body *cltypes.BeaconBody) {
			body.VoluntaryExits.Append(&cltypes.SignedVoluntaryExit{})
		}},
		{"BLS-to-execution changes", cfg.MaxBlsToExecutionChanges, func(body *cltypes.BeaconBody) {
			body.ExecutionChanges.Append(&cltypes.SignedBLSToExecutionChange{})
		}},
		{"payload attestations", cfg.MaxPayloadAttestations, func(body *cltypes.BeaconBody) {
			body.PayloadAttestations.Append(&cltypes.PayloadAttestation{})
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := corestate.New(&cfg)
			s.SetVersion(clparams.GloasVersion)
			body := cltypes.NewBeaconBody(&cfg, clparams.GloasVersion)
			for range test.limit {
				test.append(body)
			}
			require.NoError(t, validateGloasOperationCounts(body, &cfg))
			test.append(body)

			_, _, _, err := ProcessOperations(noopBlockOperationProcessor{}, s, body)
			require.ErrorContains(t, err, "too many "+test.name)
		})
	}
}
