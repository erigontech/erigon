package beaconhttp

import (
	"net/http"

	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/clparams"
)

type BeaconResponse struct {
	Data                any                    `json:"data,omitempty"`
	Finalized           *bool                  `json:"finalized,omitempty"`
	Version             *clparams.StateVersion `json:"version,omitempty"`
	ExecutionOptimistic *bool                  `json:"execution_optimistic,omitempty"`
}

func NewBeaconResponse(data any) *BeaconResponse {
	return &BeaconResponse{
		Data: data,
	}
}

func (b *BeaconResponse) EncodeSSZ(xs []byte) ([]byte, error) {
	marshaler, ok := b.Data.(ssz.Marshaler)
	if !ok {
		return nil, NewEndpointError(http.StatusBadRequest, "This endpoint does not support SSZ response")
	}
	encoded, err := marshaler.EncodeSSZ(nil)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}

func (b *BeaconResponse) EncodingSizeSSZ() int {
	marshaler, ok := b.Data.(ssz.Marshaler)
	if !ok {
		return 9
	}
	return marshaler.EncodingSizeSSZ()
}

func (r *BeaconResponse) WithFinalized(finalized bool) (out *BeaconResponse) {
	out = new(BeaconResponse)
	*out = *r
	out.Finalized = new(bool)
	out.ExecutionOptimistic = new(bool)
	out.Finalized = &finalized
	return out
}

func (r *BeaconResponse) WithOptimistic(optimistic bool) (out *BeaconResponse) {
	out = new(BeaconResponse)
	*out = *r
	out.ExecutionOptimistic = new(bool)
	out.ExecutionOptimistic = &optimistic
	return out
}

func (r *BeaconResponse) WithVersion(version clparams.StateVersion) (out *BeaconResponse) {
	out = new(BeaconResponse)
	*out = *r
	out.Version = new(clparams.StateVersion)
	out.Version = &version
	return out
}
