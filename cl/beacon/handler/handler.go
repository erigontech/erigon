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

package handler

import (
	"errors"
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"

	"github.com/erigontech/erigon-lib/common"
	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/aggregation"
	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/beacon/builder"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/persistence/state/historical_states_reader"
	"github.com/erigontech/erigon/cl/phase1/core/state/lru"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/attestation_producer"
	"github.com/erigontech/erigon/cl/validator/committee_subscription"
	"github.com/erigontech/erigon/cl/validator/sync_contribution_pool"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

const maxBlobBundleCacheSize = 48 // 8 blocks worth of blobs

// Pre-fulu blob bundle structure to hold the commitment, blob, and KZG proof. (TODO: remove after electra fork)
type BlobBundle struct {
	Commitment common.Bytes48
	Blob       *cltypes.Blob
	KzgProofs  []common.Bytes48
}

type ApiHandler struct {
	o   sync.Once
	mux *chi.Mux

	blockReader          freezeblocks.BeaconSnapshotReader
	indiciesDB           kv.RwDB
	netConfig            *clparams.NetworkConfig
	ethClock             eth_clock.EthereumClock
	beaconChainCfg       *clparams.BeaconChainConfig
	forkchoiceStore      forkchoice.ForkChoiceStorage
	operationsPool       pool.OperationsPool
	syncedData           synced_data.SyncedData
	stateReader          *historical_states_reader.HistoricalStatesReader
	sentinel             sentinel.SentinelClient
	blobStoage           blob_storage.BlobStorage
	columnStorage        blob_storage.DataColumnStorage
	caplinSnapshots      *freezeblocks.CaplinSnapshots
	caplinStateSnapshots *snapshotsync.CaplinStateSnapshots

	peerdas das.PeerDas
	version string // Node's version

	// pools
	randaoMixesPool sync.Pool

	// caches
	lighthouseInclusionCache sync.Map
	emitters                 *beaconevents.EventEmitter

	routerCfg *beacon_router_configuration.RouterConfiguration
	logger    log.Logger

	// Validator data structures
	validatorParams                    *validator_params.ValidatorParams
	blobBundles                        *lru.Cache[common.Bytes48, BlobBundle] // Keep recent bundled blobs from the execution layer.
	engine                             execution_client.ExecutionEngine
	syncMessagePool                    sync_contribution_pool.SyncContributionPool
	committeeSub                       *committee_subscription.CommitteeSubscribeMgmt
	attestationProducer                attestation_producer.AttestationDataProducer
	slotWaitedForAttestationProduction *lru.Cache[uint64, struct{}]
	aggregatePool                      aggregation.AggregationPool

	// services
	syncCommitteeMessagesService     services.SyncCommitteeMessagesService
	syncContributionAndProofsService services.SyncContributionService
	aggregateAndProofsService        services.AggregateAndProofService
	attestationService               services.AttestationService
	voluntaryExitService             services.VoluntaryExitService
	blsToExecutionChangeService      services.BLSToExecutionChangeService
	proposerSlashingService          services.ProposerSlashingService
	builderClient                    builder.BuilderClient
	enableMemoizedHeadState          bool
}

func NewApiHandler(
	logger log.Logger,
	netConfig *clparams.NetworkConfig,
	ethClock eth_clock.EthereumClock,
	beaconChainConfig *clparams.BeaconChainConfig,
	indiciesDB kv.RwDB,
	forkchoiceStore forkchoice.ForkChoiceStorage,
	operationsPool pool.OperationsPool,
	rcsn freezeblocks.BeaconSnapshotReader,
	syncedData synced_data.SyncedData,
	stateReader *historical_states_reader.HistoricalStatesReader,
	sentinel sentinel.SentinelClient,
	version string,
	routerCfg *beacon_router_configuration.RouterConfiguration,
	emitters *beaconevents.EventEmitter,
	blobStoage blob_storage.BlobStorage,
	columnStorage blob_storage.DataColumnStorage,
	caplinSnapshots *freezeblocks.CaplinSnapshots,
	validatorParams *validator_params.ValidatorParams,
	attestationProducer attestation_producer.AttestationDataProducer,
	engine execution_client.ExecutionEngine,
	syncMessagePool sync_contribution_pool.SyncContributionPool,
	committeeSub *committee_subscription.CommitteeSubscribeMgmt,
	aggregatePool aggregation.AggregationPool,
	syncCommitteeMessagesService services.SyncCommitteeMessagesService,
	syncContributionAndProofs services.SyncContributionService,
	aggregateAndProofs services.AggregateAndProofService,
	attestationService services.AttestationService,
	voluntaryExitService services.VoluntaryExitService,
	blsToExecutionChangeService services.BLSToExecutionChangeService,
	proposerSlashingService services.ProposerSlashingService,
	builderClient builder.BuilderClient,
	caplinStateSnapshots *snapshotsync.CaplinStateSnapshots,
	enableMemoizedHeadState bool,
) *ApiHandler {
	blobBundles, err := lru.New[common.Bytes48, BlobBundle]("blobs", maxBlobBundleCacheSize)
	if err != nil {
		panic(err)
	}

	slotWaitedForAttestationProduction, err := lru.New[uint64, struct{}]("slotWaitedForAttestationProduction", 1024)
	if err != nil {
		panic(err)
	}
	return &ApiHandler{
		logger:                             logger,
		validatorParams:                    validatorParams,
		o:                                  sync.Once{},
		netConfig:                          netConfig,
		ethClock:                           ethClock,
		beaconChainCfg:                     beaconChainConfig,
		indiciesDB:                         indiciesDB,
		forkchoiceStore:                    forkchoiceStore,
		operationsPool:                     operationsPool,
		blockReader:                        rcsn,
		syncedData:                         syncedData,
		stateReader:                        stateReader,
		caplinStateSnapshots:               caplinStateSnapshots,
		slotWaitedForAttestationProduction: slotWaitedForAttestationProduction,
		randaoMixesPool: sync.Pool{New: func() interface{} {
			return solid.NewHashVector(int(beaconChainConfig.EpochsPerHistoricalVector))
		}},
		sentinel:                         sentinel,
		version:                          version,
		routerCfg:                        routerCfg,
		emitters:                         emitters,
		blobStoage:                       blobStoage,
		columnStorage:                    columnStorage,
		caplinSnapshots:                  caplinSnapshots,
		attestationProducer:              attestationProducer,
		blobBundles:                      blobBundles,
		engine:                           engine,
		syncMessagePool:                  syncMessagePool,
		committeeSub:                     committeeSub,
		aggregatePool:                    aggregatePool,
		syncCommitteeMessagesService:     syncCommitteeMessagesService,
		syncContributionAndProofsService: syncContributionAndProofs,
		aggregateAndProofsService:        aggregateAndProofs,
		attestationService:               attestationService,
		voluntaryExitService:             voluntaryExitService,
		blsToExecutionChangeService:      blsToExecutionChangeService,
		proposerSlashingService:          proposerSlashingService,
		builderClient:                    builderClient,
		enableMemoizedHeadState:          enableMemoizedHeadState,
	}
}

func (a *ApiHandler) Init() {
	a.o.Do(func() {
		a.init()
	})
}
func (a *ApiHandler) init() {
	r := chi.NewRouter()
	a.mux = r

	r.Get("/", a.GetEthV1NodeHealth)

	if a.routerCfg.Lighthouse {
		r.Route("/lighthouse", func(r chi.Router) {
			r.Get("/validator_inclusion/{epoch}/global", beaconhttp.HandleEndpointFunc(a.GetLighthouseValidatorInclusionGlobal))
			r.Get("/validator_inclusion/{epoch}/{validator_id}", beaconhttp.HandleEndpointFunc(a.GetLighthouseValidatorInclusion))
		})
	}
	r.Route("/eth", func(r chi.Router) {
		r.Route("/v1", func(r chi.Router) {
			if a.routerCfg.Builder {
				r.Get("/builder/states/{state_id}/expected_withdrawals", beaconhttp.HandleEndpointFunc(a.GetEth1V1BuilderStatesExpectedWithdrawals))
			}
			if a.routerCfg.Events {
				r.Get("/events", a.EventSourceGetV1Events)
			}
			if a.routerCfg.Node {
				r.Route("/node", func(r chi.Router) {
					r.Get("/health", a.GetEthV1NodeHealth)
					r.Get("/version", beaconhttp.HandleEndpointFunc(a.GetEthV1NodeVersion))
					r.Get("/peer_count", beaconhttp.HandleEndpointFunc(a.GetEthV1NodePeerCount))
					r.Get("/peers", beaconhttp.HandleEndpointFunc(a.GetEthV1NodePeersInfos))
					r.Get("/peers/{peer_id}", beaconhttp.HandleEndpointFunc(a.GetEthV1NodePeerInfos))
					r.Get("/identity", beaconhttp.HandleEndpointFunc(a.GetEthV1NodeIdentity))
					r.Get("/syncing", beaconhttp.HandleEndpointFunc(a.GetEthV1NodeSyncing))
				})
			}

			if a.routerCfg.Debug {
				r.Get("/debug/fork_choice", a.GetEthV1DebugBeaconForkChoice)
				r.Get("/debug/data_column_sidecars/{block_id}", beaconhttp.HandleEndpointFunc(a.GetEthV1DebugBeaconDataColumnSidecars))
			}
			if a.routerCfg.Config {
				r.Route("/config", func(r chi.Router) {
					r.Get("/spec", beaconhttp.HandleEndpointFunc(a.getSpec))
					r.Get("/deposit_contract", beaconhttp.HandleEndpointFunc(a.getDepositContract))
					r.Get("/fork_schedule", beaconhttp.HandleEndpointFunc(a.getForkSchedule))
				})
			}
			if a.routerCfg.Beacon {
				r.Route("/beacon", func(r chi.Router) {
					if a.routerCfg.Builder {
						r.Post("/blinded_blocks", beaconhttp.HandleEndpointFunc(a.PostEthV1BlindedBlocks))
					}
					r.Route("/rewards", func(r chi.Router) {
						r.Post("/sync_committee/{block_id}", beaconhttp.HandleEndpointFunc(a.PostEthV1BeaconRewardsSyncCommittees))
						r.Get("/blocks/{block_id}", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconRewardsBlocks))
						r.Post("/attestations/{epoch}", beaconhttp.HandleEndpointFunc(a.PostEthV1BeaconRewardsAttestations))
					})
					r.Route("/headers", func(r chi.Router) {
						r.Get("/", beaconhttp.HandleEndpointFunc(a.getHeaders))
						r.Get("/{block_id}", beaconhttp.HandleEndpointFunc(a.getHeader))
					})
					r.Route("/blocks", func(r chi.Router) {
						r.Post("/", beaconhttp.HandleEndpointFunc(a.PostEthV1BeaconBlocks))
						r.Get("/{block_id}", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconBlock))
						r.Get("/{block_id}/attestations", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconBlockAttestations))
						r.Get("/{block_id}/root", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconBlockRoot))
					})
					r.Get("/genesis", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconGenesis))
					r.Get("/blinded_blocks/{block_id}", beaconhttp.HandleEndpointFunc(a.GetEthV1BlindedBlock))
					r.Route("/pool", func(r chi.Router) {
						r.Get("/voluntary_exits", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconPoolVoluntaryExits))
						r.Post("/voluntary_exits", a.PostEthV1BeaconPoolVoluntaryExits)
						r.Get("/attester_slashings", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconPoolAttesterSlashings))
						r.Post("/attester_slashings", a.PostEthV1BeaconPoolAttesterSlashings)
						r.Get("/proposer_slashings", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconPoolProposerSlashings))
						r.Post("/proposer_slashings", a.PostEthV1BeaconPoolProposerSlashings)
						r.Get("/bls_to_execution_changes", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconPoolBLSExecutionChanges))
						r.Post("/bls_to_execution_changes", a.PostEthV1BeaconPoolBlsToExecutionChanges)
						r.Get("/attestations", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconPoolAttestations))
						r.Post("/attestations", a.PostEthV1BeaconPoolAttestations) // deprecate after electra fork
						r.Post("/sync_committees", a.PostEthV1BeaconPoolSyncCommittees)
					})
					r.Route("/light_client", func(r chi.Router) {
						r.Get("/bootstrap/{block_id}", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconLightClientBootstrap))
						r.Get("/optimistic_update", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconLightClientOptimisticUpdate))
						r.Get("/finality_update", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconLightClientFinalityUpdate))
						r.Get("/updates", a.GetEthV1BeaconLightClientUpdates)
					})
					r.Get("/blob_sidecars/{block_id}", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconBlobSidecars))
					r.Route("/states", func(r chi.Router) {
						r.Route("/{state_id}", func(r chi.Router) {
							r.Get("/randao", beaconhttp.HandleEndpointFunc(a.getRandao))
							r.Get("/committees", beaconhttp.HandleEndpointFunc(a.getCommittees))
							r.Get("/sync_committees", beaconhttp.HandleEndpointFunc(a.getSyncCommittees)) // otterscan
							r.Get("/finality_checkpoints", beaconhttp.HandleEndpointFunc(a.getFinalityCheckpoints))
							r.Get("/root", beaconhttp.HandleEndpointFunc(a.getStateRoot))
							r.Get("/fork", beaconhttp.HandleEndpointFunc(a.getStateFork))
							r.Get("/validators", a.GetEthV1BeaconStatesValidators)
							r.Post("/validators", a.PostEthV1BeaconStatesValidators)
							r.Get("/validator_balances", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconValidatorsBalances))
							r.Post("/validator_balances", beaconhttp.HandleEndpointFunc(a.PostEthV1BeaconValidatorsBalances))
							r.Get("/validators/{validator_id}", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconStatesValidator))
							r.Get("/validator_identities", beaconhttp.HandleEndpointFunc(a.GetEthV1ValidatorIdentities))
							r.Get("/pending_consolidations", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconStatesPendingConsolidations))
							r.Get("/pending_deposits", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconStatesPendingDeposits))
							r.Get("/pending_partial_withdrawals", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconStatesPendingPartialWithdrawals))
							r.Get("/proposer_lookahead", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconStatesProposerLookahead))
						})
					})
				})
			}
			if a.routerCfg.Validator {
				r.Route("/validator", func(r chi.Router) {
					r.Route("/duties", func(r chi.Router) {
						r.Post("/attester/{epoch}", beaconhttp.HandleEndpointFunc(a.getAttesterDuties))
						r.Get("/proposer/{epoch}", beaconhttp.HandleEndpointFunc(a.getDutiesProposer))
						r.Post("/sync/{epoch}", beaconhttp.HandleEndpointFunc(a.getSyncDuties))
					})
					r.Get("/blinded_blocks/{slot}", http.NotFound) // deprecated
					r.Get("/attestation_data", beaconhttp.HandleEndpointFunc(a.GetEthV1ValidatorAttestationData))
					r.Get("/aggregate_attestation", beaconhttp.HandleEndpointFunc(a.GetEthV1ValidatorAggregateAttestation)) // deprecated
					r.Post("/aggregate_and_proofs", a.PostEthV1ValidatorAggregatesAndProof)
					r.Post("/beacon_committee_subscriptions", a.PostEthV1ValidatorBeaconCommitteeSubscription)
					r.Post("/sync_committee_subscriptions", a.PostEthV1ValidatorSyncCommitteeSubscriptions)
					r.Get("/sync_committee_contribution", beaconhttp.HandleEndpointFunc(a.GetEthV1ValidatorSyncCommitteeContribution))
					r.Post("/contribution_and_proofs", a.PostEthV1ValidatorContributionsAndProofs)
					r.Post("/prepare_beacon_proposer", a.PostEthV1ValidatorPrepareBeaconProposal)
					r.Post("/liveness/{epoch}", beaconhttp.HandleEndpointFunc(a.liveness))
					if a.routerCfg.Builder {
						r.Post("/register_validator", beaconhttp.HandleEndpointFunc(a.PostEthV1BuilderRegisterValidator))
					}
				})
			}

		})
		r.Route("/v2", func(r chi.Router) {
			if a.routerCfg.Debug {
				r.Route("/debug", func(r chi.Router) {
					r.Route("/beacon", func(r chi.Router) {
						r.Get("/states/{state_id}", beaconhttp.HandleEndpointFunc(a.getFullState))
						r.Get("/heads", beaconhttp.HandleEndpointFunc(a.GetEthV2DebugBeaconHeads))
					})
				})
			}
			if a.routerCfg.Beacon {
				r.Route("/beacon", func(r chi.Router) {
					r.Route("/blocks", func(r chi.Router) {
						r.Post("/", beaconhttp.HandleEndpointFunc(a.PostEthV2BeaconBlocks))
						r.Get("/{block_id}", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconBlock))
						r.Get("/{block_id}/attestations", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconBlockAttestations))
					})
					r.Route("/pool", func(r chi.Router) {
						r.Get("/attestations", beaconhttp.HandleEndpointFunc(a.GetEthV2BeaconPoolAttestations))
						r.Post("/attestations", a.PostEthV2BeaconPoolAttestations)
						r.Get("/attester_slashings", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconPoolAttesterSlashings)) // reuse
						r.Post("/attester_slashings", a.PostEthV1BeaconPoolAttesterSlashings)                              // resue
					})
					if a.routerCfg.Builder {
						r.Post("/blinded_blocks", beaconhttp.HandleEndpointFunc(a.PostEthV2BlindedBlocks))
					}
				})
			}
			if a.routerCfg.Validator {
				r.Route("/validator", func(r chi.Router) {
					r.Get("/blocks/{slot}", beaconhttp.HandleEndpointFunc(a.GetEthV3ValidatorBlock)) // deprecate
					r.Get("/aggregate_attestation", beaconhttp.HandleEndpointFunc(a.GetEthV2ValidatorAggregateAttestation))
					r.Post("/aggregate_and_proofs", a.PostEthV1ValidatorAggregatesAndProof) // reuse
				})
			}
		})
		if a.routerCfg.Validator {
			r.Get("/v3/validator/blocks/{slot}", beaconhttp.HandleEndpointFunc(a.GetEthV3ValidatorBlock))
		}
	})
}

func (a *ApiHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.o.Do(func() {
		a.init()
	})
	a.mux.ServeHTTP(w, r)
}

func (a *ApiHandler) getHead() (common.Hash, uint64, int, error) {
	if a.enableMemoizedHeadState {
		if a.syncedData.Syncing() {
			return common.Hash{}, 0, http.StatusServiceUnavailable, errors.New("beacon node is syncing")
		}
		return a.syncedData.HeadRoot(), a.syncedData.HeadSlot(), 0, nil
	}
	blockRoot, blockSlot, err := a.forkchoiceStore.GetHead(nil)
	if err != nil {
		return common.Hash{}, 0, http.StatusInternalServerError, err
	}
	return blockRoot, blockSlot, 0, nil
}
