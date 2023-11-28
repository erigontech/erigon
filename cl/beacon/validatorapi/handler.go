package validatorapi

import (
	"github.com/go-chi/chi/v5"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
)

type ValidatorApiHandler struct {
	FC forkchoice.ForkChoiceStorage

	BeaconChainCfg *clparams.BeaconChainConfig
	GenesisCfg     *clparams.GenesisConfig
}

func (v *ValidatorApiHandler) Route(r chi.Router) {
	r.Route("/eth", func(r chi.Router) {
		r.Route("/v1", func(r chi.Router) {
			r.Route("/beacon", func(r chi.Router) {
				r.Get("/genesis", beaconhttp.HandleEndpointFunc(v.GetEthV1BeaconGenesis))
				r.Route("/states", func(r chi.Router) {
					r.Route("/{state_id}", func(r chi.Router) {
						r.Get("/fork", nil)
						r.Get("/validators/{id}", nil)
					})
				})
				r.Post("/binded_blocks", nil)
				r.Post("/blocks", nil)
				r.Route("/pool", func(r chi.Router) {
					r.Post("/attestations", nil)
					r.Post("/sync_committees", nil)
				})
				r.Get("/node/syncing", nil)
			})
			r.Get("/config/spec", beaconhttp.HandleEndpointFunc(v.GetEthV1BeaconGenesis))
			r.Get("/events", nil)
			r.Route("/validator", func(r chi.Router) {
				r.Route("/duties", func(r chi.Router) {
					r.Post("/attester/{epoch}", nil)
					r.Get("/proposer/{epoch}", nil)
					r.Post("/sync/{epoch}", nil)
				})
				//		r.Get("/blinded_blocks/{slot}", nil) - deprecated
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
			r.Route("/debug", func(r chi.Router) {
				r.Route("/beacon", func(r chi.Router) {
					r.Get("/states/{state_id}", nil)
				})
			})
			r.Route("/beacon", func(r chi.Router) {
				r.Post("/blocks/{block_id}", nil)
			})
			r.Route("/validator", func(r chi.Router) {
				r.Post("/blocks/{slot}", nil)
			})
		})
		r.Route("/v3", func(r chi.Router) {
			r.Route("/beacon", func(r chi.Router) {
				r.Get("/blocks/{block_id}", nil)
			})
		})
	})

}
