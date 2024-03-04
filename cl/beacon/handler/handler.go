package handler

import (
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/beacon/beacon_router_configuration"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconevents"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/beacon/synced_data"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/blob_storage"
	"github.com/ledgerwatch/erigon/cl/persistence/state/historical_states_reader"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/pool"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
)

type ApiHandler struct {
	o   sync.Once
	mux *chi.Mux

	blockReader     freezeblocks.BeaconSnapshotReader
	indiciesDB      kv.RoDB
	genesisCfg      *clparams.GenesisConfig
	beaconChainCfg  *clparams.BeaconChainConfig
	forkchoiceStore forkchoice.ForkChoiceStorage
	operationsPool  pool.OperationsPool
	syncedData      *synced_data.SyncedDataManager
	stateReader     *historical_states_reader.HistoricalStatesReader
	sentinel        sentinel.SentinelClient
	blobStoage      blob_storage.BlobStorage
	caplinSnapshots *freezeblocks.CaplinSnapshots

	version string // Node's version

	// pools
	randaoMixesPool sync.Pool

	// caches
	lighthouseInclusionCache sync.Map
	emitters                 *beaconevents.Emitters

	routerCfg *beacon_router_configuration.RouterConfiguration
}

func NewApiHandler(genesisConfig *clparams.GenesisConfig, beaconChainConfig *clparams.BeaconChainConfig, indiciesDB kv.RoDB, forkchoiceStore forkchoice.ForkChoiceStorage, operationsPool pool.OperationsPool, rcsn freezeblocks.BeaconSnapshotReader, syncedData *synced_data.SyncedDataManager, stateReader *historical_states_reader.HistoricalStatesReader, sentinel sentinel.SentinelClient, version string, routerCfg *beacon_router_configuration.RouterConfiguration, emitters *beaconevents.Emitters, blobStoage blob_storage.BlobStorage, caplinSnapshots *freezeblocks.CaplinSnapshots) *ApiHandler {
	return &ApiHandler{o: sync.Once{}, genesisCfg: genesisConfig, beaconChainCfg: beaconChainConfig, indiciesDB: indiciesDB, forkchoiceStore: forkchoiceStore, operationsPool: operationsPool, blockReader: rcsn, syncedData: syncedData, stateReader: stateReader, randaoMixesPool: sync.Pool{New: func() interface{} {
		return solid.NewHashVector(int(beaconChainConfig.EpochsPerHistoricalVector))
	}}, sentinel: sentinel, version: version, routerCfg: routerCfg, emitters: emitters, blobStoage: blobStoage, caplinSnapshots: caplinSnapshots}
}

func (a *ApiHandler) Init() {
	a.o.Do(func() {
		a.init()
	})
}
func (a *ApiHandler) init() {
	r := chi.NewRouter()
	a.mux = r

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
					r.Get("/version", a.GetEthV1NodeVersion)
					r.Get("/peer_count", a.GetEthV1NodePeerCount)
					r.Get("/peers", a.GetEthV1NodePeersInfos)
					r.Get("/peers/{peer_id}", a.GetEthV1NodePeerInfos)
					r.Get("/identity", a.GetEthV1NodeIdentity)
				})
			}

			if a.routerCfg.Debug {
				r.Get("/debug/fork_choice", a.GetEthV1DebugBeaconForkChoice)
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
						r.Post("/", http.NotFound)
						r.Get("/{block_id}", beaconhttp.HandleEndpointFunc(a.getBlock))
						r.Get("/{block_id}/attestations", beaconhttp.HandleEndpointFunc(a.getBlockAttestations))
						r.Get("/{block_id}/root", beaconhttp.HandleEndpointFunc(a.getBlockRoot))
					})
					r.Get("/genesis", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconGenesis))
					r.Get("/blinded_blocks/{block_id}", beaconhttp.HandleEndpointFunc(a.getBlindedBlock))
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
						r.Post("/attestations", http.NotFound)    // TODO
						r.Post("/sync_committees", http.NotFound) // TODO
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
							r.Get("/validator_balances", a.GetEthV1BeaconValidatorsBalances)
							r.Get("/validators/{validator_id}", beaconhttp.HandleEndpointFunc(a.GetEthV1BeaconStatesValidator))
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
					r.Get("/blinded_blocks/{slot}", http.NotFound)
					r.Get("/attestation_data", http.NotFound)
					r.Get("/aggregate_attestation", http.NotFound)
					r.Post("/aggregate_and_proofs", a.PostEthV1ValidatorAggregatesAndProof)
					r.Post("/beacon_committee_subscriptions", http.NotFound)
					r.Post("/sync_committee_subscriptions", http.NotFound)
					r.Get("/sync_committee_contribution", http.NotFound)
					r.Post("/contribution_and_proofs", http.NotFound)
					r.Post("/prepare_beacon_proposer", http.NotFound)
					r.Post("/liveness/{epoch}", beaconhttp.HandleEndpointFunc(a.liveness))
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
					r.Get("/blocks/{block_id}", beaconhttp.HandleEndpointFunc(a.getBlock))
				})
			}
			if a.routerCfg.Validator {
				r.Route("/validator", func(r chi.Router) {
					r.Post("/blocks/{slot}", http.NotFound)
				})
			}
		})
	})
}

func (a *ApiHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.o.Do(func() {
		a.init()
	})
	a.mux.ServeHTTP(w, r)
}
