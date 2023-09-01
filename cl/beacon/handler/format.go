package handler

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/log/v3"
)

type BeaconResponse struct {
	Finalized           *bool         `json:"finalized,omitempty"`
	Version             string        `json:"version,omitempty"`
	ExecutionOptimistic *bool         `json:"execution_optimistic,omitempty"`
	Data                ssz.Marshaler `json:"data,omitempty"`
}

// In case of it being a json we need to also expose finalization, version, etc...
type beaconHandlerFn func(r *http.Request) (data ssz.Marshaler, finalized *bool, version *clparams.StateVersion, httpStatus int, err error)

func beaconHandlerWrapper(fn beaconHandlerFn) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		accept := r.Header.Get("Accept")
		isSSZ := !strings.Contains(accept, "application/json") && strings.Contains(accept, "application/stream-octect")
		data, finalized, version, httpStatus, err := fn(r)
		if err != nil {
			w.WriteHeader(httpStatus)
			io.WriteString(w, err.Error())
			log.Debug("[Beacon API] failed", "method", r.Method, "err", err, "ssz", isSSZ)
			return
		}

		if isSSZ {
			// SSZ encoding
			encoded, err := data.EncodeSSZ(nil)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				io.WriteString(w, err.Error())
				log.Debug("[Beacon API] failed", "method", r.Method, "err", err, "accepted", accept)
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.WriteHeader(httpStatus)
			w.Write(encoded)
			log.Debug("[Beacon API] genesis handler failed", err)
			return
		}
		resp := &BeaconResponse{Data: data}
		if version != nil {
			resp.Version = clparams.ClVersionToString(*version)
		}
		if finalized != nil {
			resp.ExecutionOptimistic = new(bool)
			resp.Finalized = new(bool)
			*resp.Finalized = *finalized
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(httpStatus)
		json.NewEncoder(w).Encode(resp)
	}
}
