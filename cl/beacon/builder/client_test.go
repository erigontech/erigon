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
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
)

type mockRoundTripper func(req *http.Request) (*http.Response, error)

func (m mockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m(req)
}

var (
	mockUrl, _       = url.Parse("https://anywhere.io")
	mockBeaconConfig = &clparams.BeaconChainConfig{
		SlotsPerEpoch:    32,
		ElectraForkEpoch: math.MaxUint64,
		FuluForkEpoch:    math.MaxUint64,
		GloasForkEpoch:   math.MaxUint64,
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
				ExecutionPayload *cltypes.Eth1Block        `json:"execution_payload"`
				BlobsBundle      *engine_types.BlobsBundle `json:"blobs_bundle"`
			} `json:"data"`
		}{
			Version: "deneb",
			Data: struct {
				ExecutionPayload *cltypes.Eth1Block        `json:"execution_payload"`
				BlobsBundle      *engine_types.BlobsBundle `json:"blobs_bundle"`
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

func TestRequestExecutionPayloadBid(t *testing.T) {
	auth := validBuilderRequestAuth()
	proposer := common.Bytes48{1}
	parentHash := common.Hash{2}
	parentRoot := common.Hash{3}
	client := publicBuilderTestClient(mockRoundTripper(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, "/eth/v1/builder/execution_payload_bid/12/"+parentHash.Hex()+"/"+parentRoot.Hex()+"/"+proposer.Hex(), r.URL.Path)
		require.Equal(t, "gloas", r.Header.Get("Eth-Consensus-Version"))
		require.Equal(t, "750", r.Header.Get("X-Timeout-Ms"))
		date, err := strconv.ParseInt(r.Header.Get("Date-Milliseconds"), 10, 64)
		require.NoError(t, err)
		require.NotZero(t, date)
		var got cltypes.SignedBuilderRequestAuth
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		require.Equal(t, auth, &got)
		return builderTestResponse(r, http.StatusOK, `{"version":"gloas","data":{"message":{"blob_kzg_commitments":[]},"signature":"0x`+strings.Repeat("00", 96)+`"}}`, http.Header{"Eth-Consensus-Version": {"gloas"}}), nil
	}))
	bid, err := client.RequestExecutionPayloadBid(context.Background(), "https://builder.example", 12, parentHash, parentRoot, proposer, auth, 750*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, bid)
}

func TestRequestExecutionPayloadBidNoContent(t *testing.T) {
	client := publicBuilderTestClient(mockRoundTripper(func(r *http.Request) (*http.Response, error) {
		return builderTestResponse(r, http.StatusNoContent, "", nil), nil
	}))

	bid, err := client.RequestExecutionPayloadBid(context.Background(), "https://builder.example", 1, common.Hash{}, common.Hash{}, common.Bytes48{}, validBuilderRequestAuth(), time.Second)
	require.NoError(t, err)
	require.Nil(t, bid)
}

func TestRequestExecutionPayloadBidEnforcesTimeout(t *testing.T) {
	client := publicBuilderTestClient(mockRoundTripper(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	}))
	started := time.Now()
	bid, err := client.RequestExecutionPayloadBid(context.Background(), "https://builder.example", 1, common.Hash{}, common.Hash{}, common.Bytes48{}, validBuilderRequestAuth(), 10*time.Millisecond)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, bid)
	require.Less(t, time.Since(started), time.Second)
}

func TestBuilderTransportRejectsUnsafeTargets(t *testing.T) {
	for _, rawURL := range []string{"http://127.0.0.1:18550", "http://[::1]:18550", "http://localhost:18550"} {
		client := &builderClient{httpClient: &http.Client{Transport: mockRoundTripper(func(*http.Request) (*http.Response, error) {
			t.Fatal("unsafe target reached transport")
			return nil, nil
		})}}
		_, err := client.RequestExecutionPayloadBid(context.Background(), rawURL, 1, common.Hash{}, common.Hash{}, common.Bytes48{}, validBuilderRequestAuth(), time.Second)
		require.Error(t, err, rawURL)
	}

	client := &builderClient{
		httpClient: &http.Client{Transport: mockRoundTripper(func(*http.Request) (*http.Response, error) {
			t.Fatal("private resolved address reached transport")
			return nil, nil
		})},
		lookupIP: func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{{IP: net.ParseIP("10.0.0.1")}}, nil
		},
	}
	_, err := client.RequestExecutionPayloadBid(context.Background(), "https://builder.example", 1, common.Hash{}, common.Hash{}, common.Bytes48{}, validBuilderRequestAuth(), time.Second)
	require.Error(t, err)

	client.lookupIP = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}, {IP: net.ParseIP("10.0.0.1")}}, nil
	}
	_, err = client.RequestExecutionPayloadBid(context.Background(), "https://builder.example", 1, common.Hash{}, common.Hash{}, common.Bytes48{}, validBuilderRequestAuth(), time.Second)
	require.Error(t, err)
}

func TestRequestExecutionPayloadBidBoundsResponseAndRefusesRedirect(t *testing.T) {
	t.Run("bounded response", func(t *testing.T) {
		client := publicBuilderTestClient(mockRoundTripper(func(r *http.Request) (*http.Response, error) {
			return builderTestResponse(r, http.StatusOK, string(make([]byte, maxBuilderResponseBodySize+1)), nil), nil
		}))
		bid, err := client.RequestExecutionPayloadBid(context.Background(), "https://builder.example", 1, common.Hash{}, common.Hash{}, common.Bytes48{}, validBuilderRequestAuth(), time.Second)
		require.Error(t, err)
		require.Nil(t, bid)
	})

	t.Run("redirect", func(t *testing.T) {
		var redirected atomic.Bool
		client := publicBuilderTestClient(mockRoundTripper(func(r *http.Request) (*http.Response, error) {
			if r.URL.Host == "redirected.example" {
				redirected.Store(true)
			}
			return builderTestResponse(r, http.StatusTemporaryRedirect, "", http.Header{"Location": {"https://redirected.example"}}), nil
		}))
		bid, err := client.RequestExecutionPayloadBid(context.Background(), "https://builder.example", 1, common.Hash{}, common.Hash{}, common.Bytes48{}, validBuilderRequestAuth(), time.Second)
		require.Error(t, err)
		require.Nil(t, bid)
		require.False(t, redirected.Load())
	})
}

func TestSubmitBuilderPreferences(t *testing.T) {
	auth := validBuilderRequestAuth()
	proposer := common.Bytes48{4}
	request := &cltypes.BuilderPreferencesRequest{Preferences: &cltypes.BuilderPreferences{MaxExecutionPayment: 9}, Auth: auth}
	client := publicBuilderTestClient(mockRoundTripper(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, "/eth/v1/builder/builder_preferences/"+proposer.Hex(), r.URL.Path)
		require.Equal(t, "gloas", r.Header.Get("Eth-Consensus-Version"))
		var got cltypes.BuilderPreferencesRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		require.Equal(t, request, &got)
		return builderTestResponse(r, http.StatusAccepted, "", nil), nil
	}))
	require.NoError(t, client.SubmitBuilderPreferences(context.Background(), "https://builder.example", proposer, request))
}

func TestSubmitBuilderPreferencesHasBoundedTimeoutAndError(t *testing.T) {
	request := &cltypes.BuilderPreferencesRequest{Preferences: &cltypes.BuilderPreferences{}, Auth: validBuilderRequestAuth()}
	t.Run("timeout", func(t *testing.T) {
		client := publicBuilderTestClient(mockRoundTripper(func(r *http.Request) (*http.Response, error) {
			<-r.Context().Done()
			return nil, r.Context().Err()
		}))
		started := time.Now()
		err := client.SubmitBuilderPreferences(context.Background(), "https://builder.example", common.Bytes48{}, request)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Less(t, time.Since(started), 2*time.Second)
	})

	t.Run("error body", func(t *testing.T) {
		client := publicBuilderTestClient(mockRoundTripper(func(r *http.Request) (*http.Response, error) {
			return builderTestResponse(r, http.StatusBadRequest, strings.Repeat("x", 1<<20), nil), nil
		}))
		err := client.SubmitBuilderPreferences(context.Background(), "https://builder.example", common.Bytes48{}, request)
		require.Error(t, err)
		require.Less(t, len(err.Error()), 1024)
		require.NotContains(t, err.Error(), "xxxx")
	})
}

func TestDynamicBuilderCallsShareBoundedAdmission(t *testing.T) {
	var active atomic.Int64
	var maximum atomic.Int64
	release := make(chan struct{})
	client := publicBuilderTestClient(mockRoundTripper(func(r *http.Request) (*http.Response, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for current > maximum.Load() && !maximum.CompareAndSwap(maximum.Load(), current) {
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
		return builderTestResponse(r, http.StatusAccepted, "", nil), nil
	}))
	request := &cltypes.BuilderPreferencesRequest{Preferences: &cltypes.BuilderPreferences{}, Auth: validBuilderRequestAuth()}

	var wg sync.WaitGroup
	for range defaultBuilderCallLimit + 8 {
		wg.Go(func() {
			_ = client.SubmitBuilderPreferences(t.Context(), "https://builder.example", common.Bytes48{}, request)
		})
	}
	require.Eventually(t, func() bool { return maximum.Load() == defaultBuilderCallLimit }, time.Second, time.Millisecond)
	close(release)
	wg.Wait()
	require.EqualValues(t, defaultBuilderCallLimit, maximum.Load())
}

func TestDynamicBuilderAdmissionCancellationDoesNotLeakPermit(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	client := publicBuilderTestClient(mockRoundTripper(func(r *http.Request) (*http.Response, error) {
		started <- struct{}{}
		select {
		case <-release:
			return builderTestResponse(r, http.StatusAccepted, "", nil), nil
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
	}))
	client.admission = semaphore.NewWeighted(1)
	request := &cltypes.BuilderPreferencesRequest{Preferences: &cltypes.BuilderPreferences{}, Auth: validBuilderRequestAuth()}
	firstDone := make(chan error, 1)
	go func() {
		firstDone <- client.SubmitBuilderPreferences(t.Context(), "https://builder.example", common.Bytes48{}, request)
	}()
	<-started
	waitingContext, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, client.SubmitBuilderPreferences(waitingContext, "https://builder.example", common.Bytes48{}, request), context.DeadlineExceeded)
	close(release)
	require.NoError(t, <-firstDone)
	require.NoError(t, client.SubmitBuilderPreferences(t.Context(), "https://builder.example", common.Bytes48{}, request))
}

func TestDynamicBuilderClientDoesNotRequireLegacyRelay(t *testing.T) {
	client := NewDynamicBuilderClient(mockBeaconConfig, BuilderTargetPolicy{})
	require.NotNil(t, client)
	require.Nil(t, client.url)
}

func TestDynamicBuilderDialUsesValidatedAddress(t *testing.T) {
	var dialed string
	client := NewDynamicBuilderClient(mockBeaconConfig, BuilderTargetPolicy{})
	client.lookupIP = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
	}
	client.transport = newPinnedBuilderTransport(func(_ context.Context, _, address string) (net.Conn, error) {
		dialed = address
		clientConn, serverConn := net.Pipe()
		go func() {
			defer serverConn.Close()
			request, err := http.ReadRequest(bufio.NewReader(serverConn))
			if err == nil {
				request.Body.Close()
				_, _ = serverConn.Write([]byte("HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"))
			}
		}()
		return clientConn, nil
	})
	request := &cltypes.BuilderPreferencesRequest{Preferences: &cltypes.BuilderPreferences{}, Auth: validBuilderRequestAuth()}
	require.NoError(t, client.SubmitBuilderPreferences(t.Context(), "http://builder.example:18550", common.Bytes48{}, request))
	require.Equal(t, "93.184.216.34:18550", dialed)
}

func TestDynamicBuilderDialFallsBackAcrossValidatedAddresses(t *testing.T) {
	addresses := []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}, {IP: net.ParseIP("93.184.216.35")}}
	var lookups int
	var dialed []string
	client := NewDynamicBuilderClient(mockBeaconConfig, BuilderTargetPolicy{})
	client.lookupIP = func(context.Context, string) ([]net.IPAddr, error) {
		lookups++
		return addresses, nil
	}
	client.transport = newPinnedBuilderTransport(func(_ context.Context, _, address string) (net.Conn, error) {
		dialed = append(dialed, address)
		if address == "93.184.216.34:18550" {
			return nil, errors.New("first address unavailable")
		}
		clientConn, serverConn := net.Pipe()
		go func() {
			defer serverConn.Close()
			request, err := http.ReadRequest(bufio.NewReader(serverConn))
			if err == nil {
				request.Body.Close()
				_, _ = serverConn.Write([]byte("HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"))
			}
		}()
		return clientConn, nil
	})
	request := &cltypes.BuilderPreferencesRequest{Preferences: &cltypes.BuilderPreferences{}, Auth: validBuilderRequestAuth()}

	require.NoError(t, client.SubmitBuilderPreferences(t.Context(), "http://builder.example:18550", common.Bytes48{}, request))
	require.Equal(t, 1, lookups)
	require.Equal(t, []string{"93.184.216.34:18550", "93.184.216.35:18550"}, dialed)
}

func TestDynamicBuilderDialAllFailAndCancellation(t *testing.T) {
	addresses := []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}, {IP: net.ParseIP("93.184.216.35")}}
	request := &cltypes.BuilderPreferencesRequest{Preferences: &cltypes.BuilderPreferences{}, Auth: validBuilderRequestAuth()}

	t.Run("all fail", func(t *testing.T) {
		var dialed []string
		client := NewDynamicBuilderClient(mockBeaconConfig, BuilderTargetPolicy{})
		client.lookupIP = func(context.Context, string) ([]net.IPAddr, error) { return addresses, nil }
		client.transport = newPinnedBuilderTransport(func(_ context.Context, _, address string) (net.Conn, error) {
			dialed = append(dialed, address)
			return nil, errors.New("unavailable")
		})

		require.Error(t, client.SubmitBuilderPreferences(t.Context(), "http://builder.example:18550", common.Bytes48{}, request))
		require.Equal(t, []string{"93.184.216.34:18550", "93.184.216.35:18550"}, dialed)
	})

	t.Run("canceled dial stops fallback", func(t *testing.T) {
		var dialed []string
		var dialedMu sync.Mutex
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		client := NewDynamicBuilderClient(mockBeaconConfig, BuilderTargetPolicy{})
		client.lookupIP = func(context.Context, string) ([]net.IPAddr, error) { return addresses, nil }
		client.transport = newPinnedBuilderTransport(func(dialCtx context.Context, _, address string) (net.Conn, error) {
			dialedMu.Lock()
			dialed = append(dialed, address)
			dialedMu.Unlock()
			cancel()
			<-dialCtx.Done()
			return nil, dialCtx.Err()
		})

		require.ErrorIs(t, client.SubmitBuilderPreferences(ctx, "http://builder.example:18550", common.Bytes48{}, request), context.Canceled)
		dialedMu.Lock()
		defer dialedMu.Unlock()
		require.Equal(t, []string{"93.184.216.34:18550"}, dialed)
	})
}

func TestDynamicBuilderDialBlackholeDoesNotConsumeFallbackDeadline(t *testing.T) {
	addresses := []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}, {IP: net.ParseIP("93.184.216.35")}}
	client := NewDynamicBuilderClient(mockBeaconConfig, BuilderTargetPolicy{})
	client.lookupIP = func(context.Context, string) ([]net.IPAddr, error) { return addresses, nil }
	client.transport = newPinnedBuilderTransport(func(ctx context.Context, _, address string) (net.Conn, error) {
		if address == "93.184.216.34:18550" {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		clientConn, serverConn := net.Pipe()
		go func() {
			defer serverConn.Close()
			request, err := http.ReadRequest(bufio.NewReader(serverConn))
			if err == nil {
				request.Body.Close()
				_, _ = serverConn.Write([]byte("HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"))
			}
		}()
		return clientConn, nil
	})
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	require.NoError(t, client.SubmitSignedBeaconBlock(t.Context(), "http://builder.example:18550", block))
}

func TestPrivateBuilderTargetsRequireExplicitPolicy(t *testing.T) {
	request := &cltypes.BuilderPreferencesRequest{Preferences: &cltypes.BuilderPreferences{}, Auth: validBuilderRequestAuth()}
	rejected := NewDynamicBuilderClient(mockBeaconConfig, BuilderTargetPolicy{})
	rejected.lookupIP = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, nil
	}
	require.Error(t, rejected.SubmitBuilderPreferences(t.Context(), "http://builder.local:18550", common.Bytes48{}, request))

	allowed := NewDynamicBuilderClient(mockBeaconConfig, BuilderTargetPolicy{AllowPrivate: true})
	allowed.lookupIP = rejected.lookupIP
	allowed.httpClient.Transport = mockRoundTripper(func(r *http.Request) (*http.Response, error) {
		return builderTestResponse(r, http.StatusAccepted, "", nil), nil
	})
	allowed.transport = nil
	require.NoError(t, allowed.SubmitBuilderPreferences(t.Context(), "http://builder.local:18550", common.Bytes48{}, request))
	require.Error(t, allowed.SubmitSignedBeaconBlock(t.Context(), "http://builder.local:18550", cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)))
}

func TestSubmitSignedBeaconBlock(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	client := publicBuilderTestClient(mockRoundTripper(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, "/eth/v1/builder/beacon_blocks", r.URL.Path)
		require.Equal(t, "gloas", r.Header.Get("Eth-Consensus-Version"))
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		expected, err := json.Marshal(block)
		require.NoError(t, err)
		require.JSONEq(t, string(expected), string(body))
		return builderTestResponse(r, http.StatusAccepted, "", nil), nil
	}))
	require.NoError(t, client.SubmitSignedBeaconBlock(context.Background(), "https://builder.example", block))
}

func TestSubmitSignedBeaconBlockHasHotPathDeadline(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	client := publicBuilderTestClient(mockRoundTripper(func(r *http.Request) (*http.Response, error) {
		deadline, ok := r.Context().Deadline()
		require.True(t, ok)
		require.LessOrEqual(t, time.Until(deadline), builderBeaconBlockTimeout)
		return nil, errors.New("builder unavailable")
	}))

	require.Error(t, client.SubmitSignedBeaconBlock(context.Background(), "https://builder.example", block))
}

func validBuilderRequestAuth() *cltypes.SignedBuilderRequestAuth {
	return &cltypes.SignedBuilderRequestAuth{
		Message: &cltypes.BuilderRequestAuth{Data: []byte("builder-auth"), Slot: 12},
	}
}

func publicBuilderTestClient(transport http.RoundTripper) *builderClient {
	return &builderClient{
		httpClient: &http.Client{Transport: transport},
		lookupIP: func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
		},
		beaconConfig: mockBeaconConfig,
	}
}

func builderTestResponse(request *http.Request, status int, body string, header http.Header) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: header, Request: request}
}

func TestSubmitBlindedBlocksFulu(t *testing.T) {
	ctx := context.Background()
	expectPath := mockUrl.JoinPath("/eth/v2/builder/blinded_blocks").String()
	block := cltypes.NewSignedBlindedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)

	t.Run("empty accepted response", func(t *testing.T) {
		mockHttpClient := &http.Client{
			Transport: mockRoundTripper(func(req *http.Request) (*http.Response, error) {
				require.Equal(t, expectPath, req.URL.String())
				require.Equal(t, http.MethodPost, req.Method)
				require.Equal(t, clparams.FuluVersion.String(), req.Header.Get("Eth-Consensus-Version"))
				return &http.Response{
					StatusCode: http.StatusAccepted,
					Body:       io.NopCloser(bytes.NewReader(nil)),
					Request:    req.Clone(context.Background()),
				}, nil
			}),
		}
		builderClient := &builderClient{
			httpClient:   mockHttpClient,
			url:          mockUrl,
			beaconConfig: mockBeaconConfig,
		}

		payload, bundle, requests, err := builderClient.SubmitBlindedBlocks(ctx, block)
		require.NoError(t, err)
		require.Nil(t, payload)
		require.Nil(t, bundle)
		require.Nil(t, requests)
	})

	t.Run("server error", func(t *testing.T) {
		mockHttpClient := &http.Client{
			Transport: mockRoundTripper(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusInternalServerError,
					Body:       io.NopCloser(bytes.NewBufferString("builder error")),
					Request:    req.Clone(context.Background()),
				}, nil
			}),
		}
		builderClient := &builderClient{
			httpClient:   mockHttpClient,
			url:          mockUrl,
			beaconConfig: mockBeaconConfig,
		}

		payload, bundle, requests, err := builderClient.SubmitBlindedBlocks(ctx, block)
		require.Error(t, err)
		require.Nil(t, payload)
		require.Nil(t, bundle)
		require.Nil(t, requests)
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
