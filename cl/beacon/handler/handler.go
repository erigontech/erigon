package handler

import (
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/pool"
)

type ApiHandler struct {
	o   sync.Once
	mux chi.Router

	blockSource     persistence.RawBeaconBlockChain
	indiciesDB      kv.RoDB
	genesisCfg      *clparams.GenesisConfig
	beaconChainCfg  *clparams.BeaconChainConfig
	forkchoiceStore forkchoice.ForkChoiceStorage
	operationsPool  pool.OperationsPool
}

func NewApiHandler(genesisConfig *clparams.GenesisConfig, beaconChainConfig *clparams.BeaconChainConfig, source persistence.RawBeaconBlockChain, indiciesDB kv.RoDB, forkchoiceStore forkchoice.ForkChoiceStorage, operationsPool pool.OperationsPool) *ApiHandler {
	return &ApiHandler{o: sync.Once{}, genesisCfg: genesisConfig, beaconChainCfg: beaconChainConfig, indiciesDB: indiciesDB, blockSource: source, forkchoiceStore: forkchoiceStore, operationsPool: operationsPool}
}

func (a *ApiHandler) init() {
	r := chi.NewRouter()
	a.mux = r
	// This is the set of apis for validation + otterscan
	// otterscn specific ones are commented as such
	r.Route("/eth", func(r chi.Router) {
		r.Route("/v1", func(r chi.Router) {
			r.Get("/events", nil)
			r.Route("/config", func(r chi.Router) {
				r.Get("/spec", beaconHandlerWrapper(a.getSpec, false))
				r.Get("/deposit_contract", beaconHandlerWrapper(a.getDepositContract, false))
				r.Get("/fork_schedule", beaconHandlerWrapper(a.getForkSchedule, false))
			})
			r.Route("/beacon", func(r chi.Router) {
				r.Route("/headers", func(r chi.Router) {
					r.Get("/", beaconHandlerWrapper(a.getHeaders, false))
					r.Get("/{block_id}", beaconHandlerWrapper(a.getHeader, false))
				})
				r.Route("/blocks", func(r chi.Router) {
					r.Post("/", nil)
					r.Get("/{block_id}", beaconHandlerWrapper(a.getBlock, true))
					r.Get("/{block_id}/attestations", beaconHandlerWrapper(a.getBlockAttestations, true))
					r.Get("/{block_id}/root", beaconHandlerWrapper(a.getBlockRoot, false))
				})
				r.Get("/genesis", beaconHandlerWrapper(a.getGenesis, false))
				r.Post("/binded_blocks", nil)
				r.Route("/pool", func(r chi.Router) {
					r.Post("/attestations", nil)
					r.Get("/voluntary_exits", beaconHandlerWrapper(a.poolVoluntaryExits, false))
					r.Get("/attester_slashings", beaconHandlerWrapper(a.poolAttesterSlashings, false))
					r.Get("/proposer_slashings", beaconHandlerWrapper(a.poolProposerSlashings, false))
					r.Get("/bls_to_execution_changes", beaconHandlerWrapper(a.poolBlsToExecutionChanges, false))
					r.Get("/attestations", beaconHandlerWrapper(a.poolAttestations, false))
					r.Post("/sync_committees", nil)
				})
				r.Get("/node/syncing", nil)

				r.Route("/states", func(r chi.Router) {
					r.Get("/head/validators/{index}", nil) // otterscan
					r.Get("/head/committees", nil)         // otterscan
					r.Route("/{state_id}", func(r chi.Router) {
						r.Get("/validators", nil)
						r.Get("/root", beaconHandlerWrapper(a.getStateRoot, false))
						r.Get("/fork", beaconHandlerWrapper(a.getStateFork, false))
						r.Get("/validators/{id}", nil)
					})
				})
			})
			r.Route("/validator", func(r chi.Router) {
				r.Route("/duties", func(r chi.Router) {
					r.Post("/attester/{epoch}", nil)
					r.Get("/proposer/{epoch}", nil)
					r.Post("/sync/{epoch}", nil)
				})
				r.Get("/blinded_blocks/{slot}", nil)
				r.Get("/attestation_data", nil)
				r.Get("/aggregate_attestation", nil)
				r.Post("/aggregate_and_proofs", nil)
				r.Post("/beacon_committee_subscriptions", nil)
				r.Post("/sync_committee_subscriptions", nil)
				r.Get("/sync_committee_contribution", nil)
				r.Post("/contribution_and_proofs", nil)
				r.Post("/prepare_beacon_proposer", nil)
			})
		})
		r.Route("/v2", func(r chi.Router) {
			r.Route("/beacon", func(r chi.Router) {
				r.Post("/blocks/{slot}", nil) //otterscan
			})
			r.Route("/validator", func(r chi.Router) {
				r.Post("/blocks/{slot}", nil)
			})
		})
	})
}

func (a *ApiHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.o.Do(func() {
		a.init()
	})
	a.mux.ServeHTTP(w, r)
}
