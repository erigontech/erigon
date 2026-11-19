// Copyright 2026 The Erigon Authors
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

package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	builder_mock "github.com/erigontech/erigon/cl/beacon/builder/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

// Supply padding without allocating an oversized request in the test. The
// LimitedReader also records how many bytes the handler actually consumes.
type publishingJSONSpaces struct{}

func (publishingJSONSpaces) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = ' '
	}
	return len(p), nil
}

func TestPublishedBlockSSZBodyLimit(t *testing.T) {
	h := &ApiHandler{beaconChainCfg: &clparams.MainnetBeaconConfig}
	for _, tc := range []struct {
		name    string
		blinded bool
		handler beaconhttp.EndpointHandlerFunc[*beaconhttp.BeaconResponse]
	}{
		{"v1/full", false, h.PostEthV1BeaconBlocks},
		{"v2/full", false, h.PostEthV2BeaconBlocks},
		{"v1/blinded", true, h.PostEthV1BlindedBlocks},
		{"v2/blinded", true, h.PostEthV2BlindedBlocks},
	} {
		t.Run(tc.name, func(t *testing.T) {
			limit, err := h.publishedBlockBodyLimit(clparams.FuluVersion, tc.blinded)
			require.NoError(t, err)
			for _, contentLength := range []int64{-1, 1} {
				body := &io.LimitedReader{R: publishingJSONSpaces{}, N: limit + 4096}
				req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", body)
				req.ContentLength = contentLength
				req.Header.Set("Content-Type", "application/octet-stream")
				req.Header.Set("Eth-Consensus-Version", clparams.FuluVersion.String())

				_, err := tc.handler(httptest.NewRecorder(), req)
				var endpointErr *beaconhttp.EndpointError
				require.ErrorAs(t, err, &endpointErr)
				require.Equal(t, http.StatusBadRequest, endpointErr.Code)
				require.ErrorContains(t, err, "request body exceeds")
				require.EqualValues(t, 4095, body.N)
			}
		})
	}
}

func TestPublishedBlockJSONBodyLimit(t *testing.T) {
	for _, blinded := range []bool{false, true} {
		versions := []clparams.StateVersion{clparams.DenebVersion, clparams.ElectraVersion, clparams.FuluVersion}
		if blinded {
			versions = append([]clparams.StateVersion{clparams.BellatrixVersion, clparams.CapellaVersion}, versions...)
		}
		for _, version := range versions {
			route := "/eth/v2/beacon/blocks"
			if blinded {
				route = "/eth/v2/beacon/blinded_blocks"
			}
			t.Run(route+"/"+version.String(), func(t *testing.T) {
				h := &ApiHandler{beaconChainCfg: &clparams.MainnetBeaconConfig}
				limit, err := h.publishedBlockBodyLimit(version, blinded)
				require.NoError(t, err)
				var block any
				if blinded {
					block = cltypes.NewSignedBlindedBeaconBlock(h.beaconChainCfg, version)
				} else {
					block = cltypes.NewDenebSignedBeaconBlock(h.beaconChainCfg, version)
				}
				body, err := json.Marshal(block)
				require.NoError(t, err)
				for _, size := range []int64{int64(len(body)), limit, limit + 1, limit + 4096} {
					// Boundary padding is fork-independent; exercise it once per route.
					if size == limit && version != clparams.ElectraVersion {
						continue
					}
					padding := &io.LimitedReader{R: publishingJSONSpaces{}, N: size - int64(len(body))}
					req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, route,
						io.MultiReader(bytes.NewReader(body), padding))
					require.EqualValues(t, -1, req.ContentLength)
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("Eth-Consensus-Version", version.String())
					recorder := httptest.NewRecorder()
					switch {
					case blinded:
						client := builder_mock.NewMockBuilderClient(gomock.NewController(t))
						builderErr := errors.New("decoded block reached builder")
						if size <= limit {
							client.EXPECT().SubmitBlindedBlocks(gomock.Any(), gomock.Any()).Return(nil, nil, nil, builderErr)
						}
						h.builderClient = client
						_, err = h.PostEthV2BlindedBlocks(recorder, req)
						if size <= limit {
							require.ErrorIs(t, err, builderErr)
							continue
						}
					case size <= limit:
						decoded, err := h.parseRequestBeaconBlock(version, req)
						require.NoError(t, err)
						require.Equal(t, version, decoded.SignedBlock.Version())
						continue
					default:
						_, err = h.PostEthV2BeaconBlocks(recorder, req)
					}
					var endpointErr *beaconhttp.EndpointError
					require.ErrorAs(t, err, &endpointErr)
					require.Equal(t, http.StatusBadRequest, endpointErr.Code)
					require.ErrorContains(t, err, "request body exceeds")
					// Even though a complete valid JSON value comes first, the body must be
					// checked before decoding. Do not just limit a streaming JSON decoder.
					require.Equal(t, size-limit-1, padding.N)
				}
			})
		}
	}
}

func TestPublishedBlockAdmitsContents(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.DenebVersion, clparams.ElectraVersion, clparams.FuluVersion} {
		t.Run(version.String(), func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			cfg.BlobSchedule = append(cfg.BlobSchedule, clparams.BlobParameters{Epoch: 1 << 32, MaxBlobsPerBlock: 192})
			h := &ApiHandler{beaconChainCfg: &cfg}
			block := cltypes.NewDenebSignedBeaconBlock(&cfg, version)
			// A large execution payload together with the maximum configured contents.
			block.SignedBlock.Block.Body.ExecutionPayload.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{make([]byte, 12<<20)})
			blobs := cfg.MaxBlobsPerBlockByVersion(version)
			proofsPerBlob := uint64(1)
			if version >= clparams.FuluVersion {
				blobs = cfg.MaxBlobsPerBlockUpperBound()
				proofsPerBlob = cfg.NumberOfColumns
			}
			for range blobs {
				block.Blobs.Append(&cltypes.Blob{})
			}
			for range blobs * proofsPerBlob {
				block.KZGProofs.Append(&cltypes.KZGProof{})
			}
			body, err := json.Marshal(block)
			require.NoError(t, err)
			limit, err := h.publishedBlockBodyLimit(version, false)
			require.NoError(t, err)
			require.Less(t, int64(len(body)), limit)
			blindedLimit, err := h.publishedBlockBodyLimit(version, true)
			require.NoError(t, err)
			blobsJSON, err := json.Marshal(block.Blobs)
			require.NoError(t, err)
			proofsJSON, err := json.Marshal(block.KZGProofs)
			require.NoError(t, err)
			require.GreaterOrEqual(t, limit-blindedLimit, int64(len(blobsJSON)+len(proofsJSON)))
			if version >= clparams.FuluVersion {
				// The scheduled contents need their own budget beyond the block.
				require.Greater(t, int64(len(body)), blindedLimit)
			}
			for _, contentType := range []string{"application/json", "application/octet-stream"} {
				if contentType == "application/octet-stream" {
					body, err = block.EncodeSSZ(nil)
					require.NoError(t, err)
				}
				req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blocks", bytes.NewReader(body))
				req.Header.Set("Content-Type", contentType)
				decoded, err := h.parseRequestBeaconBlock(version, req)
				require.NoError(t, err)
				require.EqualValues(t, blobs, decoded.Blobs.Len())
				require.EqualValues(t, blobs*proofsPerBlob, decoded.KZGProofs.Len())
				require.Len(t, decoded.SignedBlock.Block.Body.ExecutionPayload.Transactions.UnderlyngReference()[0], 12<<20)
			}
		})
	}
}

func TestPublishedBlockJSONLimitErrorIsServerFault(t *testing.T) {
	for _, blinded := range []bool{false, true} {
		cfg := clparams.MainnetBeaconConfig
		h := &ApiHandler{beaconChainCfg: &cfg, netConfig: &clparams.NetworkConfig{GossipMaxSize: math.MaxUint64}}
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/blocks", bytes.NewReader([]byte("{}")))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Eth-Consensus-Version", clparams.ElectraVersion.String())
		var err error
		if blinded {
			_, err = h.PostEthV2BlindedBlocks(httptest.NewRecorder(), req)
		} else {
			_, err = h.PostEthV2BeaconBlocks(httptest.NewRecorder(), req)
		}
		var endpointErr *beaconhttp.EndpointError
		require.ErrorAs(t, err, &endpointErr)
		require.Equal(t, http.StatusInternalServerError, endpointErr.Code, "blinded=%t", blinded)
	}
}

func TestPublishedBlockJSONLimitRejectsOverflow(t *testing.T) {
	for _, mutate := range []func(*ApiHandler){
		func(h *ApiHandler) { h.netConfig = &clparams.NetworkConfig{GossipMaxSize: math.MaxUint64} },
		func(h *ApiHandler) { h.beaconChainCfg.NumberOfColumns = math.MaxUint64 },
		func(h *ApiHandler) { h.beaconChainCfg.MaxBlobsPerBlockElectra = math.MaxUint64 },
	} {
		cfg := clparams.MainnetBeaconConfig
		h := &ApiHandler{beaconChainCfg: &cfg}
		mutate(h)
		_, err := h.publishedBlockBodyLimit(clparams.FuluVersion, false)
		require.Error(t, err)
	}
}
