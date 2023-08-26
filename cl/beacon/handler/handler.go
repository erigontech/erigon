package handler

import (
	"database/sql"
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/persistence"
)

type ApiHandler struct {
	o   sync.Once
	mux chi.Router

	blockSource    persistence.BlockSource
	indiciesDB     *sql.DB
	genesisCfg     *clparams.GenesisConfig
	beaconChainCfg *clparams.BeaconChainConfig
}

func NewApiHandler(genesisConfig *clparams.GenesisConfig, beaconChainConfig *clparams.BeaconChainConfig, source persistence.BlockSource, indiciesDB *sql.DB) *ApiHandler {
	return &ApiHandler{o: sync.Once{}, genesisCfg: genesisConfig, beaconChainCfg: beaconChainConfig, indiciesDB: indiciesDB, blockSource: source}
}

func (a *ApiHandler) init() {
	r := chi.NewRouter()
	a.mux = r
	// This is the set of apis for validation + otterscan
	// otterscn specific ones are commented as such
	r.Route("/eth", func(r chi.Router) {
		r.Route("/v1", func(r chi.Router) {
			r.Get("/events", nil)
			r.Route("/beacon", func(r chi.Router) {
				r.Get("/headers/{tag}", nil)                     // otterscan
				r.Get("/blocks/{block_id}", a.getBlock)          //otterscan
				r.Get("/blocks/{block_id}/root", a.getBlockRoot) //otterscan
				r.Get("/genesis", a.getGenesis)
				r.Post("/binded_blocks", nil)
				r.Post("/blocks", nil)
				r.Route("/pool", func(r chi.Router) {
					r.Post("/attestations", nil)
					r.Post("/sync_committees", nil)
				})
				r.Get("/node/syncing", nil)
				r.Get("/config/spec", nil)
				r.Route("/states", func(r chi.Router) {
					r.Get("/head/validators/{index}", nil) // otterscan
					r.Get("/head/committees", nil)         // otterscan
					r.Route("/{state_id}", func(r chi.Router) {
						r.Get("/validators", nil)
						r.Get("/fork", nil)
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
