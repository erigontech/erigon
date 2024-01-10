package validatorapi

import (
	"net/http"
	"sync"

	"github.com/go-chi/chi/v5"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/beacon/building"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
)

type ValidatorApiHandler struct {
	FC forkchoice.ForkChoiceStorage

	BeaconChainCfg *clparams.BeaconChainConfig
	GenesisCfg     *clparams.GenesisConfig

	state *building.State

	o   sync.Once
	mux *chi.Mux
}

func (v *ValidatorApiHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	v.o.Do(func() {
		v.mux = chi.NewRouter()
		v.state = building.NewState()
		v.Route(v.mux)
	})
	v.mux.ServeHTTP(w, r)
}

func (v *ValidatorApiHandler) Route(r chi.Router) {
	r.Route("/eth", func(r chi.Router) {
		r.Route("/v1", func(r chi.Router) {
			r.Route("/beacon", func(r chi.Router) {
				r.Route("/states", func(r chi.Router) {
					r.Route("/{state_id}", func(r chi.Router) {
						r.Get("/fork", beaconhttp.HandleEndpointFunc(v.GetEthV1BeaconStatesStateIdFork))
						// r.Get("/validators/{validator_id}", beaconhttp.HandleEndpointFunc(v.GetEthV1BeaconStatesStateIdValidatorsValidatorId))
					})
				})
				r.Post("/blocks", beaconhttp.HandleEndpointFunc(v.PostEthV1BeaconBlocks))
				r.Post("/blinded_blocks", beaconhttp.HandleEndpointFunc(v.PostEthV1BeaconBlindedBlocks))
				r.Route("/pool", func(r chi.Router) {
					r.Post("/attestations", beaconhttp.HandleEndpointFunc(v.PostEthV1BeaconPoolAttestations))
					r.Post("/sync_committees", beaconhttp.HandleEndpointFunc(v.PostEthV1BeaconPoolAttestations))
				})
			})
			r.Route("/node", func(r chi.Router) {
				r.Get("/syncing", beaconhttp.HandleEndpointFunc(v.GetEthV1NodeSyncing))
			})
			r.Get("/events", v.EventSourceGetV1Events)
			r.Route("/validator", func(r chi.Router) {
				// implemented by archive api (for now)
				//		r.Route("/duties", func(r chi.Router) {
				//			r.Post("/attester/{epoch}", http.NotFound)
				//			r.Post("/sync/{epoch}", http.NotFound)
				//			r.Get("/proposer/{epoch}", http.NotFound)
				//		})
				//		r.Get("/blinded_blocks/{slot}", http.NotFound) - deprecated
				r.Get("/attestation_data", http.NotFound)
				r.Get("/aggregate_attestation", http.NotFound)
				r.Post("/aggregate_and_proofs", beaconhttp.HandleEndpointFunc(v.PostEthV1ValidatorAggregateAndProofs))
				r.Post("/beacon_committee_subscriptions", beaconhttp.HandleEndpointFunc(v.PostEthV1ValidatorBeaconCommitteeSubscriptions))
				r.Post("/sync_committee_subscriptions", beaconhttp.HandleEndpointFunc(v.PostEthV1ValidatorSyncCommitteeSubscriptions))
				r.Get("/sync_committee_contribution", http.NotFound)
				r.Post("/contribution_and_proofs", beaconhttp.HandleEndpointFunc(v.PostEthV1ValidatorContributionAndProofs))
				r.Post("/prepare_beacon_proposer", beaconhttp.HandleEndpointFunc(v.PostEthV1ValidatorPrepareBeaconProposer))
			})
		})
		r.Route("/v2", func(r chi.Router) {
			r.Route("/debug", func(r chi.Router) {
				r.Route("/beacon", func(r chi.Router) {
					r.Get("/states/{state_id}", http.NotFound)
				})
			})
			r.Route("/beacon", func(r chi.Router) {
				r.Post("/blocks", beaconhttp.HandleEndpointFunc(v.PostEthV2BeaconBlocks))
				r.Post("/blinded_blocks", beaconhttp.HandleEndpointFunc(v.PostEthV2BeaconBlindedBlocks))
			})
			r.Route("/validator", func(r chi.Router) {
				r.Post("/blocks/{slot}", beaconhttp.HandleEndpointFunc(v.GetEthV3ValidatorBlocksSlot))
			})
		})
		r.Route("/v3", func(r chi.Router) {
			r.Route("/validator", func(r chi.Router) {
				r.Get("/blocks/{block_id}", http.NotFound)
			})
		})
	})

}
