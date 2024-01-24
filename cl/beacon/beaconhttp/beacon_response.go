package beaconhttp

import (
	"encoding/json"
	"net/http"

	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/clparams"
)

type BeaconResponse struct {
	Data                any
	Finalized           *bool
	Version             *clparams.StateVersion
	ExecutionOptimistic *bool

	Extra map[string]any
}

func NewBeaconResponse(data any) *BeaconResponse {
	return &BeaconResponse{
		Data: data,
	}
}

func (r *BeaconResponse) With(key string, value any) (out *BeaconResponse) {
	out = new(BeaconResponse)
	*out = *r
	out.Extra[key] = value
	return out
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

func (b *BeaconResponse) MarshalJSON() ([]byte, error) {
	o := map[string]any{
		"data": b.Data,
	}
	if b.Finalized != nil {
		o["finalized"] = *b.Finalized
	}
	if b.Version != nil {
		o["version"] = *b.Version
	}
	if b.ExecutionOptimistic != nil {
		o["execution_optimistic"] = *b.ExecutionOptimistic
	}
	for k, v := range b.Extra {
		o[k] = v
	}
	return json.Marshal(o)
}

func (b *BeaconResponse) EncodeSSZ(xs []byte) ([]byte, error) {
	marshaler, ok := b.Data.(ssz.Marshaler)
	if !ok {
		return nil, NewEndpointError(http.StatusBadRequest, ErrorSszNotSupported)
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
