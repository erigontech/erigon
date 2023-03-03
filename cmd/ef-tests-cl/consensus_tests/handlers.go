package consensustests

import (
	"fmt"
	"path"
)

type testFunc func(context testContext) error

var (
	operationsDivision      = "operations"
	epochProcessingDivision = "epoch_processing"
)

// Epoch processing cases
var (
	caseEffectiveBalanceUpdates      = "effective_balance_updates"
	caseEth1DataReset                = "eth1_data_reset"
	caseHistoricalRootsUpdate        = "historical_roots_update"
	caseInactivityUpdates            = "inactivity_updates"
	caseJustificationAndFinalization = "justification_and_finalization"
	caseParticipationFlagUpdates     = "participation_flag_updates"
	caseRandaoMixesReset             = "randao_mixes_reset"
	caseRegistryUpdates              = "registry_updates"
	caseRewardsAndPenalties          = "rewards_and_penalties"
	caseSlashings                    = "slashings"
	caseSlashingsReset               = "slashings_reset"
)

// Operations cases
var (
	caseAttestation      = "attestation"
	caseAttesterSlashing = "attester_slashing"
	caseProposerSlashing = "proposer_slashing"
	caseBlockHeader      = "block_header"
	caseDeposit          = "deposit"
	caseVoluntaryExit    = "voluntary_exit"
	caseSyncAggregate    = "sync_aggregate"
)

// transitionCoreTest
var finality = "finality/finality"

// sanity
var sanityBlocks = "sanity/blocks"
var sanitySlots = "sanity/slots"

// random
var random = "random/random"

// Stays here bc debugging >:-(
func placeholderTest() error {
	fmt.Println("hallo")
	return nil
}

// Following is just a map for all tests to their execution.
var handlers map[string]testFunc = map[string]testFunc{
	path.Join(epochProcessingDivision, caseEffectiveBalanceUpdates):      effectiveBalancesUpdateTest,
	path.Join(epochProcessingDivision, caseEth1DataReset):                eth1DataResetTest,
	path.Join(epochProcessingDivision, caseHistoricalRootsUpdate):        historicalRootsUpdateTest,
	path.Join(epochProcessingDivision, caseInactivityUpdates):            inactivityUpdateTest,
	path.Join(epochProcessingDivision, caseJustificationAndFinalization): justificationFinalizationTest,
	path.Join(epochProcessingDivision, caseParticipationFlagUpdates):     participationFlagUpdatesTest,
	path.Join(epochProcessingDivision, caseRandaoMixesReset):             randaoMixesTest,
	path.Join(epochProcessingDivision, caseRegistryUpdates):              registryUpdatesTest,
	path.Join(epochProcessingDivision, caseRewardsAndPenalties):          rewardsAndPenaltiesTest,
	path.Join(epochProcessingDivision, caseSlashings):                    slashingsTest,
	path.Join(epochProcessingDivision, caseSlashingsReset):               slashingsResetTest,
	path.Join(operationsDivision, caseAttestation):                       operationAttestationHandler,
	path.Join(operationsDivision, caseAttesterSlashing):                  operationAttesterSlashingHandler,
	path.Join(operationsDivision, caseProposerSlashing):                  operationProposerSlashingHandler,
	path.Join(operationsDivision, caseBlockHeader):                       operationBlockHeaderHandler,
	path.Join(operationsDivision, caseDeposit):                           operationDepositHandler,
	path.Join(operationsDivision, caseSyncAggregate):                     operationSyncAggregateHandler,
	path.Join(operationsDivision, caseVoluntaryExit):                     operationVoluntaryExitHandler,
	sanityBlocks: testSanityFunction,
	sanitySlots:  testSanityFunctionSlot,
	finality:     finalityTestFunction,
	random:       testSanityFunction, // Same as sanity handler.
}
