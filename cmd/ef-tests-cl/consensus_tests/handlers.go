package consensustests

import (
	"fmt"
	"path"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

type testFunc func(context testContext) error

var (
	operationsDivision      = "operations"
	epochProcessingDivision = "epoch_processing"
	sszDivision             = "ssz_static"
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
	caseParticipationRecords         = "participation_record_updates"
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
	caseWithdrawal       = "withdrawals"
	caseBlsChange        = "bls_to_execution_change"
)

// transitionCoreTest
var finality = "finality/finality"

// sanity
var sanityBlocks = "sanity/blocks"
var sanitySlots = "sanity/slots"

// random
var random = "random/random"

// transitionCore
var transitionCore = "transition/core"

// ssz_static cases

var (
	validatorCase         = "Validator"
	beaconStateCase       = "BeaconState"
	checkpointCase        = "Checkpoint"
	depositCase           = "Deposit"
	depositDataCase       = "DepositData"
	signedBeaconBlockCase = "SignedBeaconBlock"
	beaconBlockCase       = "BeaconBlock"
	beaconBodyCase        = "BeaconBody"
	// If you wanna do the rest go ahead but the important ones are all covered. also each of the above include all other encodings.
)

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
	path.Join(epochProcessingDivision, caseParticipationRecords):         recordsResetTest,
	path.Join(operationsDivision, caseAttestation):                       operationAttestationHandler,
	path.Join(operationsDivision, caseAttesterSlashing):                  operationAttesterSlashingHandler,
	path.Join(operationsDivision, caseProposerSlashing):                  operationProposerSlashingHandler,
	path.Join(operationsDivision, caseBlockHeader):                       operationBlockHeaderHandler,
	path.Join(operationsDivision, caseDeposit):                           operationDepositHandler,
	path.Join(operationsDivision, caseSyncAggregate):                     operationSyncAggregateHandler,
	path.Join(operationsDivision, caseVoluntaryExit):                     operationVoluntaryExitHandler,
	path.Join(operationsDivision, caseWithdrawal):                        operationWithdrawalHandler,
	path.Join(operationsDivision, caseBlsChange):                         operationSignedBlsChangeHandler,
	path.Join(sszDivision, validatorCase):                                getSSZStaticConsensusTest(&cltypes.Validator{}),
	path.Join(sszDivision, beaconStateCase):                              getSSZStaticConsensusTest(state.New(&clparams.MainnetBeaconConfig)),
	path.Join(sszDivision, checkpointCase):                               getSSZStaticConsensusTest(&cltypes.Checkpoint{}),
	path.Join(sszDivision, depositCase):                                  getSSZStaticConsensusTest(&cltypes.Deposit{}),
	path.Join(sszDivision, depositDataCase):                              getSSZStaticConsensusTest(&cltypes.DepositData{}),
	path.Join(sszDivision, depositDataCase):                              getSSZStaticConsensusTest(&cltypes.DepositData{}),
	path.Join(sszDivision, signedBeaconBlockCase):                        getSSZStaticConsensusTest(&cltypes.SignedBeaconBlock{}),
	path.Join(sszDivision, beaconBlockCase):                              getSSZStaticConsensusTest(&cltypes.BeaconBlock{}),
	path.Join(sszDivision, beaconBodyCase):                               getSSZStaticConsensusTest(&cltypes.BeaconBody{}),
	transitionCore:                                                       transitionTestFunction,
	sanityBlocks:                                                         testSanityFunction,
	sanitySlots:                                                          testSanityFunctionSlot,
	finality:                                                             finalityTestFunction,
	random:                                                               testSanityFunction, // Same as sanity handler.
}
