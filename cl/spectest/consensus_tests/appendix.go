// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package consensus_tests

import (
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/spectest"
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
		With("participation_record_updates", participationRecordUpdatesTest).
		With("pending_deposits", pendingDepositTest).
		With("pending_consolidations", PendingConsolidationTest).
		With("proposer_lookahead", ProposerLookaheadTest)
	TestFormats.Add("finality").
		With("finality", FinalityFinality)
	TestFormats.Add("fork_choice").
		With("get_head", &ForkChoice{}).
		With("on_block", &ForkChoice{}).
		With("on_merge_block", &ForkChoice{}).
		With("ex_ante", &ForkChoice{})
	TestFormats.Add("fork").
		With("fork", ForksFork)
	TestFormats.Add("genesis").
		With("validity", spectest.UnimplementedHandler).
		With("initialization", spectest.UnimplementedHandler)
	TestFormats.Add("kzg").
		With("", spectest.UnimplementedHandler)
	TestFormats.Add("light_client").
		WithFn("single_merkle_proof", LightClientBeaconBlockBodyExecutionMerkleProof)
	TestFormats.Add("merkle_proof").
		With("single_merkle_proof", Eip4844MerkleProof)
	TestFormats.Add("operations").
		WithFn("attestation", operationAttestationHandler).
		WithFn("attester_slashing", operationAttesterSlashingHandler).
		WithFn("proposer_slashing", operationProposerSlashingHandler).
		WithFn("block_header", operationBlockHeaderHandler).
		WithFn("deposit", operationDepositHandler).
		WithFn("voluntary_exit", operationVoluntaryExitHandler).
		WithFn("sync_aggregate", operationSyncAggregateHandler).
		WithFn("withdrawals", operationWithdrawalHandler).
		WithFn("bls_to_execution_change", operationSignedBlsChangeHandler).
		WithFn("consolidation_request", operationConsolidationRequestHandler).
		WithFn("deposit_request", operationDepositRequstHandler).
		WithFn("withdrawal_request", operationWithdrawalRequstHandler)
	TestFormats.Add("random").
		With("random", SanityBlocks)
	TestFormats.Add("rewards").
		With("basic", &RewardsCore{}).
		With("random", &RewardsCore{}).
		With("leak", &RewardsCore{})
	TestFormats.Add("sanity").
		With("slots", SanitySlots).
		With("blocks", SanityBlocks)
	TestFormats.Add("shuffling").
		With("core", &ShufflingCore{})
	TestFormats.Add("ssz_generic").
		With("", spectest.UnimplementedHandler)
	TestFormats.Add("sync").
		With("", spectest.UnimplementedHandler)
	TestFormats.Add("transition").
		With("core", &TransitionCore{})
	TestFormats.Add("networking").
		WithFn("compute_columns_for_custody_group", TestComputeColumnsForCustodyGroup).
		WithFn("get_custody_groups", TestGetCustodyGroups)

	addSszTests()
}

func addSszTests() {
	TestFormats.Add("ssz_static").
		With("AggregateAndProof", getSSZStaticConsensusTest(&cltypes.AggregateAndProof{})).
		With("AttestationData", getSSZStaticConsensusTest(&solid.AttestationData{})).
		With("BeaconBlockHeader", getSSZStaticConsensusTest(&cltypes.BeaconBlockHeader{})).
		With("BeaconState", getSSZStaticConsensusTest(state.New(&clparams.MainnetBeaconConfig))).
		With("BlobIdentifier", getSSZStaticConsensusTest(&cltypes.BlobIdentifier{})).
		With("BlobSidecar", getSSZStaticConsensusTest(&cltypes.BlobSidecar{})).
		With("BLSToExecutionChange", getSSZStaticConsensusTest(&cltypes.BLSToExecutionChange{})).
		With("Checkpoint", getSSZStaticConsensusTest(&solid.Checkpoint{})).
		With("ContributionAndProof", getSSZStaticConsensusTest(&cltypes.ContributionAndProof{})).
		With("Deposit", getSSZStaticConsensusTest(&cltypes.Deposit{})).
		With("DepositData", getSSZStaticConsensusTest(&cltypes.DepositData{})).
		//	With("DepositMessage", getSSZStaticConsensusTest(&cltypes.DepositMessage{})).
		// With("Eth1Block", getSSZStaticConsensusTest(&cltypes.Eth1Block{})).
		With("Eth1Data", getSSZStaticConsensusTest(&cltypes.Eth1Data{})).
		With("Fork", getSSZStaticConsensusTest(&cltypes.Fork{})).
		//With("ForkData", getSSZStaticConsensusTest(&cltypes.ForkData{})).
		//With("HistoricalBatch", getSSZStaticConsensusTest(&cltypes.HistoricalBatch{})).
		With("HistoricalSummary", getSSZStaticConsensusTest(&cltypes.HistoricalSummary{})).
		With("LightClientBootstrap", getSSZStaticConsensusTest(&cltypes.LightClientBootstrap{})).
		With("LightClientFinalityUpdate", getSSZStaticConsensusTest(&cltypes.LightClientFinalityUpdate{})).
		With("LightClientOptimisticUpdate", getSSZStaticConsensusTest(&cltypes.LightClientOptimisticUpdate{})).
		//With("LightClientUpdate", getSSZStaticConsensusTest(&cltypes.LightClientUpdate{})).
		With("PendingAttestation", getSSZStaticConsensusTest(&solid.PendingAttestation{})).
		//		With("PowBlock", getSSZStaticConsensusTest(&cltypes.PowBlock{})). Unimplemented
		With("ProposerSlashing", getSSZStaticConsensusTest(&cltypes.ProposerSlashing{})).
		With("SignedAggregateAndProof", getSSZStaticConsensusTest(&cltypes.SignedAggregateAndProof{})).
		With("SignedBeaconBlockHeader", getSSZStaticConsensusTest(&cltypes.SignedBeaconBlockHeader{})).
		//With("SignedBlobSidecar", getSSZStaticConsensusTest(&cltypes.SignedBlobSideCar{})).
		With("SignedBLSToExecutionChange", getSSZStaticConsensusTest(&cltypes.SignedBLSToExecutionChange{})).
		With("SignedContributionAndProof", getSSZStaticConsensusTest(&cltypes.SignedContributionAndProof{})).
		With("SignedVoluntaryExit", getSSZStaticConsensusTest(&cltypes.SignedVoluntaryExit{})).
		//	With("SigningData", getSSZStaticConsensusTest(&cltypes.SigningData{})). Not needed.
		With("SyncAggregate", getSSZStaticConsensusTest(&cltypes.SyncAggregate{})).
		With("SyncAggregatorSelectionData", getSSZStaticConsensusTest(&cltypes.SyncAggregatorSelectionData{})).
		With("SyncCommittee", getSSZStaticConsensusTest(&solid.SyncCommittee{})).
		//	With("SyncCommitteeMessage", getSSZStaticConsensusTest(&cltypes.SyncCommitteeMessage{})).
		With("Validator", getSSZStaticConsensusTest(solid.NewValidator())).
		With("ExecutionPayloadHeader", sszStaticTestNewObjectByFunc(
			func(v clparams.StateVersion) *cltypes.Eth1Header {
				return cltypes.NewEth1Header(v)
			}, withTestJson())).
		With("SyncCommitteeContribution", sszStaticTestByEmptyObject(&cltypes.Contribution{})).
		With("Withdrawal", sszStaticTestByEmptyObject(&cltypes.Withdrawal{}, withTestJson())).
		With("LightClientHeader", sszStaticTestNewObjectByFunc(
			func(v clparams.StateVersion) *cltypes.LightClientHeader {
				return cltypes.NewLightClientHeader(v)
			}, withTestJson())).
		With("LightClientUpdate", sszStaticTestNewObjectByFunc(
			func(v clparams.StateVersion) *cltypes.LightClientUpdate {
				return cltypes.NewLightClientUpdate(v)
			}, withTestJson())).
		With("SignedBeaconBlock", sszStaticTestNewObjectByFunc(
			func(v clparams.StateVersion) *cltypes.SignedBeaconBlock {
				return cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, v)
			}, withTestJson())).
		With("ExecutionPayload", sszStaticTestNewObjectByFunc(
			func(v clparams.StateVersion) *cltypes.Eth1Block {
				return cltypes.NewEth1Block(v, &clparams.MainnetBeaconConfig)
			}, withTestJson())).
		With("ExecutionRequests", sszStaticTestNewObjectByFunc(
			func(v clparams.StateVersion) *cltypes.ExecutionRequests {
				return cltypes.NewExecutionRequests(&clparams.MainnetBeaconConfig)
			}, withTestJson(), runAfterVersion(clparams.ElectraVersion))).
		With("IndexedAttestation", sszStaticTestNewObjectByFunc(
			func(v clparams.StateVersion) *cltypes.IndexedAttestation {
				return cltypes.NewIndexedAttestation(v)
			}, withTestJson())).
		With("BeaconBlock", sszStaticTestNewObjectByFunc(
			func(v clparams.StateVersion) *cltypes.BeaconBlock {
				return cltypes.NewBeaconBlock(&clparams.MainnetBeaconConfig, v)
			}, withTestJson())).
		With("AttesterSlashing", sszStaticTestNewObjectByFunc(
			func(v clparams.StateVersion) *cltypes.AttesterSlashing {
				return cltypes.NewAttesterSlashing(v)
			}, withTestJson())).
		With("BeaconBlockBody", sszStaticTestNewObjectByFunc(
			func(v clparams.StateVersion) *cltypes.BeaconBody {
				return cltypes.NewBeaconBody(&clparams.MainnetBeaconConfig, v)
			}, withTestJson())).
		With("Attestation", sszStaticTestNewObjectByFunc(
			func(v clparams.StateVersion) *solid.Attestation {
				return &solid.Attestation{}
			}, withTestJson())).
		With("SyncCommitteeMessage", sszStaticTestByEmptyObject(&cltypes.SyncCommitteeMessage{}, withTestJson())).
		With("VoluntaryExit", sszStaticTestByEmptyObject(&cltypes.VoluntaryExit{}, withTestJson())).
		With("SingleAttestation", sszStaticTestByEmptyObject(&solid.SingleAttestation{}, withTestJson(), runAfterVersion(clparams.ElectraVersion))).
		With("WithdrawalRequest", sszStaticTestByEmptyObject(&solid.WithdrawalRequest{}, runAfterVersion(clparams.ElectraVersion))).
		With("DepositRequest", sszStaticTestByEmptyObject(&solid.DepositRequest{}, withTestJson(), runAfterVersion(clparams.ElectraVersion))).
		With("ConsolidationRequest", sszStaticTestByEmptyObject(&solid.ConsolidationRequest{}, withTestJson(), runAfterVersion(clparams.ElectraVersion))).
		With("PendingConsolidation", sszStaticTestByEmptyObject(&solid.PendingConsolidation{}, runAfterVersion(clparams.ElectraVersion))).         // no need json test
		With("PendingDeposit", sszStaticTestByEmptyObject(&solid.PendingDeposit{}, runAfterVersion(clparams.ElectraVersion))).                     // no need json test
		With("PendingPartialWithdrawal", sszStaticTestByEmptyObject(&solid.PendingPartialWithdrawal{}, runAfterVersion(clparams.ElectraVersion))). // no need json test
		With("DataColumnsByRootIdentifier", sszStaticTestByEmptyObject(&cltypes.DataColumnsByRootIdentifier{}, runAfterVersion(clparams.FuluVersion))).
		With("MatrixEntry", sszStaticTestByEmptyObject(&cltypes.MatrixEntry{}, withTestJson(), runAfterVersion(clparams.FuluVersion))).
		With("DataColumnSidecar", sszStaticTestByEmptyObject(&cltypes.DataColumnSidecar{}, withTestJson(), runAfterVersion(clparams.FuluVersion)))
}
