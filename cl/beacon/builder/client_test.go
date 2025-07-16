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

package builder

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
)

type mockRoundTripper func(req *http.Request) (*http.Response, error)

func (m mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m(req)
}

var (
	mockUrl, _       = url.Parse("https://anywhere.io")
	mockBeaconConfig = &clparams.BeaconChainConfig{
		SlotsPerEpoch: 32,
	}

	//go:embed test_data/mock_blinded_block.json
	mockBlindedBlockBytes []byte
	//go:embed test_data/mock_blinded_block_resp.json
	mockBlindedResponseBytes []byte
	//go:embed test_data/mock_header.json
	mockHeaderBytes []byte
)

func TestGetStatus(t *testing.T) {
	ctx := context.Background()
	expectPath := mockUrl.JoinPath("/eth/v1/builder/status").String()
	expectMethod := http.MethodGet
	t.Run("No content", func(t *testing.T) {
		mockHttpClient := &http.Client{
			Transport: mockRoundTripper(func(req *http.Request) (*http.Response, error) {
				require.Nil(t, req.Body)
				require.Equal(t, expectPath, req.URL.String())
				require.Equal(t, expectMethod, req.Method)
				return &http.Response{
					StatusCode: http.StatusNoContent,
					Body:       io.NopCloser(nil),
					Request:    req.Clone(context.Background()),
				}, nil
			}),
		}
		builderClient := &builderClient{
			httpClient:   mockHttpClient,
			url:          mockUrl,
			beaconConfig: mockBeaconConfig,
		}
		builderClient.httpClient = mockHttpClient
		err := builderClient.GetStatus(ctx)
		require.NoError(t, err)
	})

	t.Run("200 OK", func(t *testing.T) {
		mockHttpClient := &http.Client{
			Transport: mockRoundTripper(func(req *http.Request) (*http.Response, error) {
				require.Nil(t, req.Body)
				require.Equal(t, expectPath, req.URL.String())
				require.Equal(t, expectMethod, req.Method)
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewBuffer([]byte(""))),
					Request:    req.Clone(context.Background()),
				}, nil
			}),
		}
		builderClient := &builderClient{
			httpClient:   mockHttpClient,
			url:          mockUrl,
			beaconConfig: mockBeaconConfig,
		}
		err := builderClient.GetStatus(ctx)
		require.NoError(t, err)
	})

	t.Run("400 error", func(t *testing.T) {
		mockHttpClient := &http.Client{
			Transport: mockRoundTripper(func(req *http.Request) (*http.Response, error) {
				require.Nil(t, req.Body)
				require.Equal(t, expectPath, req.URL.String())
				require.Equal(t, expectMethod, req.Method)
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       io.NopCloser(bytes.NewBuffer([]byte(""))),
					Request:    req.Clone(context.Background()),
				}, nil
			}),
		}
		builderClient := &builderClient{
			httpClient:   mockHttpClient,
			url:          mockUrl,
			beaconConfig: mockBeaconConfig,
		}
		err := builderClient.GetStatus(ctx)
		require.Error(t, err)
	})
}

func TestRegisterValidator(t *testing.T) {
	ctx := context.Background()
	expectPath := mockUrl.JoinPath("/eth/v1/builder/validators").String()
	expectMethod := http.MethodPost

	t.Run("empty validators get error", func(t *testing.T) {
		builderClient := &builderClient{
			httpClient:   nil,
			url:          mockUrl,
			beaconConfig: mockBeaconConfig,
		}
		err := builderClient.RegisterValidator(ctx, []*cltypes.ValidatorRegistration{})
		require.Error(t, err)
	})

	t.Run("valid validators", func(t *testing.T) {
		mockValidators := []*cltypes.ValidatorRegistration{
			{
				Message: cltypes.ValidatorRegistrationMessage{
					FeeRecipient: common.HexToAddress("0xAbcF8e0d4e9587369b2301D0790347320302cc09"),
					GasLimit:     "456",
					Timestamp:    "456",
					PubKey:       newBytes48FromString("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
				},
				Signature: newBytes96FromString("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
			},
			{
				Message: cltypes.ValidatorRegistrationMessage{
					FeeRecipient: common.HexToAddress("0x123"),
					GasLimit:     "123",
					Timestamp:    "123",
					PubKey:       newBytes48FromString("0x9876543210abcdef9876543210abcdef9876543210abcdef9876543210abcdef"),
				},
				Signature: newBytes96FromString("0x9876543210abcdef9876543210abcdef9876543210abcdef9876543210abcdef"),
			},
		}

		mockHttpClient := &http.Client{
			Transport: mockRoundTripper(func(req *http.Request) (*http.Response, error) {
				require.NotNil(t, req.Body)
				require.Equal(t, expectPath, req.URL.String())
				require.Equal(t, expectMethod, req.Method)
				// read request body
				bodyBytes, err := io.ReadAll(req.Body)
				require.NoError(t, err)
				mockValidatorsBytes, err := json.Marshal(mockValidators)
				require.NoError(t, err)
				require.Equal(t, mockValidatorsBytes, bodyBytes)
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewBuffer([]byte(""))),
					Request:    req.Clone(context.Background()),
				}, nil
			}),
		}
		builderClient := &builderClient{
			httpClient:   mockHttpClient,
			url:          mockUrl,
			beaconConfig: mockBeaconConfig,
		}
		err := builderClient.RegisterValidator(ctx, mockValidators)
		require.NoError(t, err)
	})
}

func TestGetHeader(t *testing.T) {
	ctx := context.Background()
	expectMethod := http.MethodGet
	mockSlot := int64(123)
	mockParentHash := common.HexToHash("0x1234567")
	mockPubKey := newBytes48FromString("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
	path := fmt.Sprintf("/eth/v1/builder/header/%d/%s/%s", mockSlot, mockParentHash.Hex(), mockPubKey.Hex())
	expectPath := mockUrl.JoinPath(path).String()
	t.Run("success", func(t *testing.T) {
		mockHttpClient := &http.Client{
			Transport: mockRoundTripper(func(req *http.Request) (*http.Response, error) {
				require.Nil(t, req.Body)
				require.Equal(t, expectPath, req.URL.String(), req.URL.String())
				require.Equal(t, expectMethod, req.Method)
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewBuffer(mockHeaderBytes)),
					Request:    req.Clone(context.Background()),
				}, nil
			}),
		}
		builderClient := &builderClient{
			httpClient:   mockHttpClient,
			url:          mockUrl,
			beaconConfig: mockBeaconConfig,
		}
		header, err := builderClient.GetHeader(ctx, mockSlot, mockParentHash, mockPubKey)
		header.Data.Message.ExecutionRequests = nil
		require.NoError(t, err)
		require.NotNil(t, header)
		// marshal and unmarshal to compare
		headerBytes, err := json.Marshal(header)
		require.NoError(t, err)
		require.JSONEq(t, string(mockHeaderBytes), string(headerBytes))
	})

	t.Run("400 error", func(t *testing.T) {
		mockHttpClient := &http.Client{
			Transport: mockRoundTripper(func(req *http.Request) (*http.Response, error) {
				require.Nil(t, req.Body)
				require.Equal(t, expectPath, req.URL.String(), req.URL.String())
				require.Equal(t, expectMethod, req.Method)
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       io.NopCloser(bytes.NewBuffer([]byte("bad request"))),
					Request:    req.Clone(context.Background()),
				}, nil
			}),
		}
		builderClient := &builderClient{
			httpClient:   mockHttpClient,
			url:          mockUrl,
			beaconConfig: mockBeaconConfig,
		}
		header, err := builderClient.GetHeader(ctx, mockSlot, mockParentHash, mockPubKey)
		require.Error(t, err)
		require.Nil(t, header)
	})
}

func TestSubmitBlindedBlocks(t *testing.T) {
	ctx := context.Background()
	expectMethod := http.MethodPost
	expectPath := mockUrl.JoinPath("/eth/v1/builder/blinded_blocks").String()
	mockBlindedBlock := &cltypes.SignedBlindedBeaconBlock{}
	err := json.Unmarshal(mockBlindedBlockBytes, mockBlindedBlock)
	require.NoError(t, err)

	t.Run("post blinded block success", func(t *testing.T) {
		mockHttpClient := &http.Client{
			Transport: mockRoundTripper(func(req *http.Request) (*http.Response, error) {
				require.NotNil(t, req.Body)
				require.Equal(t, expectPath, req.URL.String(), req.URL.String())
				require.Equal(t, expectMethod, req.Method)
				// read request body
				bodyBytes, err := io.ReadAll(req.Body)
				require.NoError(t, err)
				require.JSONEq(t, string(mockBlindedBlockBytes), string(bodyBytes))
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(bytes.NewBuffer(mockBlindedResponseBytes)),
					Request:    req.Clone(context.Background()),
				}, nil
			}),
		}
		builderClient := &builderClient{
			httpClient:   mockHttpClient,
			url:          mockUrl,
			beaconConfig: mockBeaconConfig,
		}
		block, bundle, _, err := builderClient.SubmitBlindedBlocks(ctx, mockBlindedBlock)
		require.NoError(t, err)
		result := struct {
			Version string `json:"version"`
			Data    struct {
				ExecutionPayload *cltypes.Eth1Block          `json:"execution_payload"`
				BlobsBundle      *engine_types.BlobsBundleV1 `json:"blobs_bundle"`
			} `json:"data"`
		}{
			Version: "deneb",
			Data: struct {
				ExecutionPayload *cltypes.Eth1Block          `json:"execution_payload"`
				BlobsBundle      *engine_types.BlobsBundleV1 `json:"blobs_bundle"`
			}{
				ExecutionPayload: block,
				BlobsBundle:      bundle,
			},
		}
		resultBytes, err := json.Marshal(result)
		require.NoError(t, err)
		require.JSONEq(t, string(mockBlindedResponseBytes), string(resultBytes))
	})

	t.Run("400 error", func(t *testing.T) {
		mockHttpClient := &http.Client{
			Transport: mockRoundTripper(func(req *http.Request) (*http.Response, error) {
				require.NotNil(t, req.Body)
				require.Equal(t, expectPath, req.URL.String(), req.URL.String())
				require.Equal(t, expectMethod, req.Method)
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Body:       io.NopCloser(bytes.NewBuffer([]byte("bad request"))),
					Request:    req.Clone(context.Background()),
				}, nil
			}),
		}
		builderClient := &builderClient{
			httpClient:   mockHttpClient,
			url:          mockUrl,
			beaconConfig: mockBeaconConfig,
		}
		block, bundle, _, err := builderClient.SubmitBlindedBlocks(ctx, mockBlindedBlock)
		require.Error(t, err)
		require.Nil(t, block)
		require.Nil(t, bundle)
	})
}

func newBytes48FromString(s string) common.Bytes48 {
	bytes := common.Hex2Bytes(s)
	var b common.Bytes48
	copy(b[:], bytes)
	return b
}

func newBytes96FromString(s string) common.Bytes96 {
	bytes := common.Hex2Bytes(s)
	var b common.Bytes96
	copy(b[:], bytes)
	return b
}
