package validatorapi

import (
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
)

type ValidatorApiHandler struct {
	FC forkchoice.ForkChoiceStorage

	BeaconChainCfg *clparams.BeaconChainConfig
	GenesisCfg     *clparams.GenesisConfig

	o   sync.Once
	mux chi.Router
}

func (v *ValidatorApiHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	v.o.Do(func() {
		v.mux = chi.NewRouter()
		v.init(v.mux)
	})
	v.mux.ServeHTTP(w, r)
}

func (v *ValidatorApiHandler) init(r chi.Router) {
	r.Route("/eth", func(r chi.Router) {
		r.Route("/v1", func(r chi.Router) {
			r.Route("/beacon", func(r chi.Router) {
				r.Get("/genesis", beaconhttp.HandleEndpointFunc(v.GetEthV1BeaconGenesis))
				r.Route("/states", func(r chi.Router) {
					r.Route("/{state_id}", func(r chi.Router) {
						r.Get("/fork", beaconhttp.HandleEndpointFunc(v.GetEthV1BeaconStatesStateIdFork))
						r.Get("/validators/{validator_id}", beaconhttp.HandleEndpointFunc(v.GetEthV1BeaconStatesStateIdValidatorsValidatorId))
					})
				})
				r.Post("/binded_blocks", http.NotFound)
				r.Post("/blocks", http.NotFound)
				r.Route("/pool", func(r chi.Router) {
					r.Post("/attestations", http.NotFound)
					r.Post("/sync_committees", http.NotFound)
				})
				r.Get("/node/syncing", beaconhttp.HandleEndpointFunc(v.GetEthV1NodeSyncing))
			})
			r.Get("/config/spec", beaconhttp.HandleEndpointFunc(v.GetEthV1ConfigSpec))
			r.Get("/events", http.NotFound)
			r.Route("/validator", func(r chi.Router) {
				r.Route("/duties", func(r chi.Router) {
					r.Post("/attester/{epoch}", http.NotFound)
					r.Get("/proposer/{epoch}", http.NotFound)
					r.Post("/sync/{epoch}", http.NotFound)
				})
				//		r.Get("/blinded_blocks/{slot}", http.NotFound) - deprecated
				r.Get("/attestation_data", http.NotFound)
				r.Get("/aggregate_attestation", http.NotFound)
				r.Post("/aggregate_and_proofs", http.NotFound)
				r.Post("/beacon_committee_subscriptions", http.NotFound)
				r.Post("/sync_committee_subscriptions", http.NotFound)
				r.Get("/sync_committee_contribution", http.NotFound)
				r.Post("/contribution_and_proofs", http.NotFound)
				r.Post("/prepare_beacon_proposer", http.NotFound)
			})
		})
		r.Route("/v2", func(r chi.Router) {
			r.Route("/debug", func(r chi.Router) {
				r.Route("/beacon", func(r chi.Router) {
					r.Get("/states/{state_id}", http.NotFound)
				})
			})
			r.Route("/beacon", func(r chi.Router) {
				r.Post("/blocks/{block_id}", http.NotFound)
			})
			r.Route("/validator", func(r chi.Router) {
				r.Post("/blocks/{slot}", http.NotFound)
			})
		})
		r.Route("/v3", func(r chi.Router) {
			r.Route("/beacon", func(r chi.Router) {
				r.Get("/blocks/{block_id}", http.NotFound)
			})
		})
	})

}
