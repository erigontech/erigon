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

package beaconhttp

import (
	"encoding/json"
	"net/http"

	"github.com/erigontech/erigon-lib/types/ssz"
	"github.com/erigontech/erigon/cl/clparams"
)

type BeaconResponse struct {
	Data                any
	Finalized           *bool
	Version             *clparams.StateVersion
	ExecutionOptimistic *bool

	Extra   map[string]any
	headers map[string]string
}

func NewBeaconResponse(data any) *BeaconResponse {
	return &BeaconResponse{
		Data: data,
	}
}

func (r *BeaconResponse) Headers() map[string]string {
	if r.headers == nil {
		return make(map[string]string)
	}
	return r.headers
}

func (r *BeaconResponse) WithHeaders(headers map[string]string) (out *BeaconResponse) {
	if r.headers == nil {
		r.headers = make(map[string]string)
	}
	r.headers = headers
	return r
}

func (r *BeaconResponse) WithHeader(key string, value string) (out *BeaconResponse) {
	if r.headers == nil {
		r.headers = make(map[string]string)
	}
	r.headers[key] = value
	return r
}

func (r *BeaconResponse) With(key string, value any) (out *BeaconResponse) {
	out = new(BeaconResponse)
	*out = *r
	if out.Extra == nil {
		out.Extra = make(map[string]any)
	}
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
		o["version"] = b.Version.String()
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
