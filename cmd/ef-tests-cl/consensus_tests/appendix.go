package consensus_tests

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/ef-tests-cl/spectest"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

var TestFormats = spectest.Appendix{}

func init() {
	TestFormats.Add("bls").
		With("aggregate_verify", &BlsAggregateVerify{}).
		With("aggregate", spectest.UnimplementedHandler).
		With("eth_aggregate_pubkeys", spectest.UnimplementedHandler).
		With("eth_fast_aggregate_verify", spectest.UnimplementedHandler).
		With("fast_aggregate_verify", spectest.UnimplementedHandler).
		With("sign", spectest.UnimplementedHandler).
		With("verify", spectest.UnimplementedHandler)
	TestFormats.Add("epoch_processing").
		With("effective_balance_updates", effectiveBalancesUpdateTest).
		With("eth1_data_reset", eth1DataResetTest).
		With("historical_roots_update", historicalRootsUpdateTest).
		With("inactivity_updates", inactivityUpdateTest).
		With("justification_and_finalization", justificationFinalizationTest).
		With("participation_flag_updates", participationFlagUpdatesTest).
		With("randao_mixes_reset", randaoMixesTest).
		With("registry_updates", registryUpdatesTest).
		With("rewards_and_penalties", rewardsAndPenaltiesTest).
		With("slashings", slashingsTest).
		With("slashings_reset", slashingsResetTest).
		With("participation_record_updates", participationFlagUpdatesTest)
	TestFormats.Add("finality").
		With("finality", FinalityFinality)
	TestFormats.Add("fork-choice").
		With("", spectest.UnimplementedHandler)
	TestFormats.Add("forks").
		With("fork", ForksFork)
	TestFormats.Add("genesis").
		With("validity", spectest.UnimplementedHandler).
		With("initialization", spectest.UnimplementedHandler)
	TestFormats.Add("kzg").
		With("", spectest.UnimplementedHandler)
	TestFormats.Add("light_client").
		With("", spectest.UnimplementedHandler)
	TestFormats.Add("operations").
		WithFn("attestation", operationAttestationHandler).
		WithFn("attester_slashing", operationAttesterSlashingHandler).
		WithFn("proposer_slashing", operationProposerSlashingHandler).
		WithFn("block_header", operationBlockHeaderHandler).
		WithFn("deposit", operationDepositHandler).
		WithFn("voluntary_exit", operationVoluntaryExitHandler).
		WithFn("sync_aggregate", operationSyncAggregateHandler).
		WithFn("withdrawals", operationWithdrawalHandler).
		WithFn("bls_to_execution-change", operationSignedBlsChangeHandler)
	TestFormats.Add("random").
		With("", spectest.UnimplementedHandler)
	TestFormats.Add("rewards").
		With("", spectest.UnimplementedHandler)
	TestFormats.Add("sanity").
		With("slots", SanitySlots).
		With("blocks", SanityBlocks)
	TestFormats.Add("shuffling").
		With("", spectest.UnimplementedHandler)
	TestFormats.Add("ssz_generic").
		With("", spectest.UnimplementedHandler)
	TestFormats.Add("ssz_static").
		With("Validator", getSSZStaticConsensusTest(&cltypes.Validator{})).
		With("BeaconState", getSSZStaticConsensusTest(state.New(&clparams.MainnetBeaconConfig))).
		With("Checkpoint", getSSZStaticConsensusTest(&cltypes.Checkpoint{})).
		With("Deposit", getSSZStaticConsensusTest(&cltypes.Deposit{})).
		With("DepositData", getSSZStaticConsensusTest(&cltypes.DepositData{})).
		With("SignedBeaconBlock", getSSZStaticConsensusTest(&cltypes.SignedBeaconBlock{})).
		With("BeaconBlock", getSSZStaticConsensusTest(&cltypes.BeaconBlock{})).
		With("BeaconBody", getSSZStaticConsensusTest(&cltypes.BeaconBody{}))
	TestFormats.Add("sync").
		With("", spectest.UnimplementedHandler)
	TestFormats.Add("transition").
		With("core", &TransitionCore{})
}
