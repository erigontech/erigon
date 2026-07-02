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

package heimdall

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/polygon/heimdall/poshttp"
)

type emptyBodyReadCloser struct{}

func (ebrc emptyBodyReadCloser) Read(_ []byte) (n int, err error) {
	return 0, io.EOF
}

func (ebrc emptyBodyReadCloser) Close() error {
	return nil
}

func TestHeimdallClientFetchesTerminateUponTooManyErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}

	ctx := context.Background()
	ctrl := gomock.NewController(t)
	requestHandler := poshttp.NewMockhttpRequestHandler(ctrl)
	requestHandler.EXPECT().
		Do(gomock.Any()).
		Return(&http.Response{
			StatusCode: 404,
			Body:       emptyBodyReadCloser{},
		}, nil).
		Times(5)
	logger := testlog.Logger(t, log.LvlDebug)
	heimdallClient := NewHttpClient(
		"https://dummyheimdal.com",
		logger,
		poshttp.WithHttpRequestHandler(requestHandler),
		poshttp.WithHttpRetryBackOff(100*time.Millisecond),
		poshttp.WithHttpMaxRetries(5),
	)

	spanRes, err := heimdallClient.FetchSpan(ctx, 1534)
	require.Nil(t, spanRes)
	require.Error(t, err)
}

type mockHeimdallResponse struct {
	status int
	body   string
}

func okBody(body string) mockHeimdallResponse {
	return mockHeimdallResponse{status: http.StatusOK, body: body}
}

func newTestHttpClient(
	t *testing.T,
	version poshttp.HeimdallVersion,
	responses map[string]mockHeimdallResponse,
) (*HttpClient, map[string]int) {
	requestHandler := poshttp.NewMockhttpRequestHandler(gomock.NewController(t))
	requests := map[string]int{}
	requestHandler.EXPECT().
		Do(gomock.Any()).
		DoAndReturn(func(req *http.Request) (*http.Response, error) {
			key := req.URL.Path
			if req.URL.RawQuery != "" {
				key += "?" + req.URL.RawQuery
			}
			requests[key]++
			response, ok := responses[key]
			if !ok {
				return &http.Response{
					StatusCode: http.StatusNotFound,
					Body:       io.NopCloser(strings.NewReader("unexpected request: " + key)),
				}, nil
			}
			return &http.Response{
				StatusCode: response.status,
				Body:       io.NopCloser(strings.NewReader(response.body)),
			}, nil
		}).
		AnyTimes()

	opts := []poshttp.ClientOption{
		poshttp.WithHttpRequestHandler(requestHandler),
		poshttp.WithHttpRetryBackOff(time.Millisecond),
		poshttp.WithHttpMaxRetries(2),
	}
	if version == poshttp.HeimdallV2 {
		responses["/chainmanager/params"] = okBody(`{"params":{"chain_params":{"pol_token_address":"0x0000000000000000000000000000000000001010"}}}`)
		opts = append(opts, poshttp.WithApiVersioner(context.Background()))
	}

	return NewHttpClient("https://dummyheimdall.com", testlog.Logger(t, log.LvlDebug), opts...), requests
}

func TestHttpClientFetchersRouteByVersion(t *testing.T) {
	proposer := common.HexToAddress("0x0000000000000000000000000000000000000001")
	rootHash := common.HexToHash("0x1234000000000000000000000000000000000000000000000000000000005678")
	rootHashBase64 := base64.StdEncoding.EncodeToString(rootHash[:])

	spanV1JSON := `{"span_id":1534,"start_block":100,"end_block":200,"bor_chain_id":"137"}`
	wantSpanV1 := &Span{Id: 1534, StartBlock: 100, EndBlock: 200, ChainID: "137"}

	validatorJSON := fmt.Sprintf(`{"val_id":"1","signer":"%s","voting_power":"10","proposer_priority":"0"}`, proposer)
	wantValidator := Validator{ID: 1, Address: proposer, VotingPower: 10}
	spanV2JSON := fmt.Sprintf(
		`{"id":"1534","start_block":"100","end_block":"200","validator_set":{"validators":[%s],"proposer":%s},"selected_producers":[%s],"bor_chain_id":"137"}`,
		validatorJSON, validatorJSON, validatorJSON,
	)
	wantSpanV2 := &Span{
		Id:         1534,
		StartBlock: 100,
		EndBlock:   200,
		ValidatorSet: ValidatorSet{
			Validators: []*Validator{&wantValidator},
			Proposer:   &wantValidator,
		},
		SelectedProducers: []Validator{wantValidator},
		ChainID:           "137",
	}

	wantWaypointFields := WaypointFields{
		Proposer:   proposer,
		StartBlock: big.NewInt(100),
		EndBlock:   big.NewInt(200),
		RootHash:   rootHash,
		ChainID:    "137",
		Timestamp:  1712,
	}
	checkpointV1JSON := fmt.Sprintf(
		`{"id":5,"proposer":"%s","start_block":100,"end_block":200,"root_hash":"%s","bor_chain_id":"137","timestamp":1712}`,
		proposer, rootHash,
	)
	checkpointV2JSON := fmt.Sprintf(
		`{"id":"5","proposer":"%s","start_block":"100","end_block":"200","root_hash":"%s","bor_chain_id":"137","timestamp":"1712"}`,
		proposer, rootHashBase64,
	)
	wantCheckpoint := &Checkpoint{Id: 5, Fields: wantWaypointFields}

	milestoneV1JSON := fmt.Sprintf(
		`{"milestone_id":"m-100","proposer":"%s","start_block":100,"end_block":200,"hash":"%s","bor_chain_id":"137","timestamp":1712}`,
		proposer, rootHash,
	)
	milestoneV2JSON := fmt.Sprintf(
		`{"milestone_id":"m-100","proposer":"%s","start_block":"100","end_block":"200","hash":"%s","bor_chain_id":"137","timestamp":"1712"}`,
		proposer, rootHashBase64,
	)
	wantMilestone := &Milestone{Id: 100, MilestoneId: "m-100", Fields: wantWaypointFields}

	statusJSON := `{"latest_block_hash":"0xabc","latest_app_hash":"0xdef","latest_block_time":"2024-01-01T00:00:00Z","catching_up":true}`
	wantStatus := &Status{
		LatestBlockHash: "0xabc",
		LatestAppHash:   "0xdef",
		LatestBlockTime: "2024-01-01T00:00:00Z",
		CatchingUp:      true,
	}

	for _, tc := range []struct {
		name      string
		version   poshttp.HeimdallVersion
		responses map[string]mockHeimdallResponse
		fetch     func(ctx context.Context, client *HttpClient) (any, error)
		want      any
	}{
		{
			name:    "FetchLatestSpan v1",
			version: poshttp.HeimdallV1,
			responses: map[string]mockHeimdallResponse{
				"/bor/latest-span": okBody(`{"height":"1","result":` + spanV1JSON + `}`),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchLatestSpan(ctx)
			},
			want: wantSpanV1,
		},
		{
			name:    "FetchLatestSpan v2",
			version: poshttp.HeimdallV2,
			responses: map[string]mockHeimdallResponse{
				"/bor/spans/latest": okBody(`{"span":` + spanV2JSON + `}`),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchLatestSpan(ctx)
			},
			want: wantSpanV2,
		},
		{
			name:    "FetchSpan v1",
			version: poshttp.HeimdallV1,
			responses: map[string]mockHeimdallResponse{
				"/bor/span/1534": okBody(`{"height":"1","result":` + spanV1JSON + `}`),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchSpan(ctx, 1534)
			},
			want: wantSpanV1,
		},
		{
			name:    "FetchSpan v2",
			version: poshttp.HeimdallV2,
			responses: map[string]mockHeimdallResponse{
				"/bor/spans/1534": okBody(`{"span":` + spanV2JSON + `}`),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchSpan(ctx, 1534)
			},
			want: wantSpanV2,
		},
		{
			name:    "FetchSpans v1",
			version: poshttp.HeimdallV1,
			responses: map[string]mockHeimdallResponse{
				"/bor/span/list?page=2&limit=10": okBody(`{"height":"1","result":[` + spanV1JSON + `]}`),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchSpans(ctx, 2, 10)
			},
			want: []*Span{wantSpanV1},
		},
		{
			name:    "FetchSpans v2",
			version: poshttp.HeimdallV2,
			responses: map[string]mockHeimdallResponse{
				"/bor/spans/list?pagination.offset=10&pagination.limit=10": okBody(`{"span_list":[` + spanV2JSON + `]}`),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchSpans(ctx, 2, 10)
			},
			want: []*Span{wantSpanV2},
		},
		{
			name:    "FetchCheckpoint v1",
			version: poshttp.HeimdallV1,
			responses: map[string]mockHeimdallResponse{
				"/checkpoints/5": okBody(`{"height":"1","result":` + checkpointV1JSON + `}`),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchCheckpoint(ctx, 5)
			},
			want: wantCheckpoint,
		},
		{
			name:    "FetchCheckpoint v2",
			version: poshttp.HeimdallV2,
			responses: map[string]mockHeimdallResponse{
				"/checkpoints/5": okBody(`{"checkpoint":` + checkpointV2JSON + `}`),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchCheckpoint(ctx, 5)
			},
			want: wantCheckpoint,
		},
		{
			name:    "FetchCheckpoints v1",
			version: poshttp.HeimdallV1,
			responses: map[string]mockHeimdallResponse{
				"/checkpoints/list?page=2&limit=10": okBody(`{"height":"1","result":[` + checkpointV1JSON + `]}`),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchCheckpoints(ctx, 2, 10)
			},
			want: []*Checkpoint{wantCheckpoint},
		},
		{
			name:    "FetchCheckpoints v2",
			version: poshttp.HeimdallV2,
			responses: map[string]mockHeimdallResponse{
				"/checkpoints/list?pagination.offset=10&pagination.limit=10": okBody(`{"checkpoint_list":[` + checkpointV2JSON + `]}`),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchCheckpoints(ctx, 2, 10)
			},
			want: []*Checkpoint{wantCheckpoint},
		},
		{
			name:    "FetchMilestone v1",
			version: poshttp.HeimdallV1,
			responses: map[string]mockHeimdallResponse{
				"/milestone/100": okBody(`{"height":"1","result":` + milestoneV1JSON + `}`),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchMilestone(ctx, 100)
			},
			want: wantMilestone,
		},
		{
			name:    "FetchMilestone v2",
			version: poshttp.HeimdallV2,
			responses: map[string]mockHeimdallResponse{
				"/milestones/100": okBody(`{"milestone":` + milestoneV2JSON + `}`),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchMilestone(ctx, 100)
			},
			want: wantMilestone,
		},
		{
			name:    "FetchStatus v1",
			version: poshttp.HeimdallV1,
			responses: map[string]mockHeimdallResponse{
				"/status": okBody(`{"height":"1","result":` + statusJSON + `}`),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchStatus(ctx)
			},
			want: wantStatus,
		},
		{
			name:    "FetchStatus v2",
			version: poshttp.HeimdallV2,
			responses: map[string]mockHeimdallResponse{
				"/status": okBody(statusJSON),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchStatus(ctx)
			},
			want: wantStatus,
		},
		{
			name:    "FetchCheckpointCount v1",
			version: poshttp.HeimdallV1,
			responses: map[string]mockHeimdallResponse{
				"/checkpoints/count": okBody(`{"height":"1","result":{"result":420}}`),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchCheckpointCount(ctx)
			},
			want: int64(420),
		},
		{
			name:    "FetchCheckpointCount v2",
			version: poshttp.HeimdallV2,
			responses: map[string]mockHeimdallResponse{
				"/checkpoints/count": okBody(`{"ack_count":"420"}`),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchCheckpointCount(ctx)
			},
			want: int64(420),
		},
		{
			name:    "FetchMilestoneCount v1",
			version: poshttp.HeimdallV1,
			responses: map[string]mockHeimdallResponse{
				"/milestone/count": okBody(`{"height":"1","result":{"count":420}}`),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchMilestoneCount(ctx)
			},
			want: int64(420),
		},
		{
			name:    "FetchMilestoneCount v2",
			version: poshttp.HeimdallV2,
			responses: map[string]mockHeimdallResponse{
				"/milestones/count": okBody(`{"count":"420"}`),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchMilestoneCount(ctx)
			},
			want: int64(420),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, _ := newTestHttpClient(t, tc.version, tc.responses)
			got, err := tc.fetch(context.Background(), client)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestHttpClientFetchErrorHandling(t *testing.T) {
	for _, tc := range []struct {
		name         string
		version      poshttp.HeimdallVersion
		responses    map[string]mockHeimdallResponse
		fetch        func(ctx context.Context, client *HttpClient) (any, error)
		assertErr    func(t *testing.T, err error)
		wantRequests map[string]int
	}{
		{
			name:      "FetchSpan v1 wraps fetch error with spanID",
			version:   poshttp.HeimdallV1,
			responses: map[string]mockHeimdallResponse{},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchSpan(ctx, 1534)
			},
			assertErr: func(t *testing.T, err error) {
				require.ErrorIs(t, err, poshttp.ErrNotSuccessfulResponse)
				require.ErrorContains(t, err, "spanID=1534")
			},
			wantRequests: map[string]int{"/bor/span/1534": 2},
		},
		{
			name:      "FetchSpan v2 wraps fetch error with spanID",
			version:   poshttp.HeimdallV2,
			responses: map[string]mockHeimdallResponse{},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchSpan(ctx, 1534)
			},
			assertErr: func(t *testing.T, err error) {
				require.ErrorIs(t, err, poshttp.ErrNotSuccessfulResponse)
				require.ErrorContains(t, err, "spanID=1534")
			},
			wantRequests: map[string]int{"/bor/spans/1534": 2},
		},
		{
			name:    "FetchMilestone v1 pruned milestone is not retried",
			version: poshttp.HeimdallV1,
			responses: map[string]mockHeimdallResponse{
				"/milestone/5":     {status: http.StatusInternalServerError, body: "Invalid milestone index"},
				"/milestone/count": okBody(`{"height":"1","result":{"count":200}}`),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchMilestone(ctx, 5)
			},
			assertErr: func(t *testing.T, err error) {
				require.ErrorIs(t, err, ErrNotInMilestoneList)
				require.ErrorContains(t, err, "number 5")
			},
			wantRequests: map[string]int{"/milestone/5": 1},
		},
		{
			name:    "FetchMilestone v2 pruned milestone is not retried",
			version: poshttp.HeimdallV2,
			responses: map[string]mockHeimdallResponse{
				"/milestones/5":     {status: http.StatusInternalServerError, body: "Invalid milestone index"},
				"/milestones/count": okBody(`{"count":"200"}`),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchMilestone(ctx, 5)
			},
			assertErr: func(t *testing.T, err error) {
				require.ErrorIs(t, err, ErrNotInMilestoneList)
				require.ErrorContains(t, err, "number 5")
			},
			wantRequests: map[string]int{"/milestones/5": 1},
		},
		{
			name:    "FetchMilestone v1 non-pruned milestone is retried",
			version: poshttp.HeimdallV1,
			responses: map[string]mockHeimdallResponse{
				"/milestone/150":   {status: http.StatusInternalServerError, body: "Invalid milestone index"},
				"/milestone/count": okBody(`{"height":"1","result":{"count":200}}`),
			},
			fetch: func(ctx context.Context, client *HttpClient) (any, error) {
				return client.FetchMilestone(ctx, 150)
			},
			assertErr: func(t *testing.T, err error) {
				require.ErrorIs(t, err, ErrNotInMilestoneList)
				require.ErrorContains(t, err, "number 150")
			},
			wantRequests: map[string]int{"/milestone/150": 2},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, requests := newTestHttpClient(t, tc.version, tc.responses)
			result, err := tc.fetch(context.Background(), client)
			require.Nil(t, result)
			tc.assertErr(t, err)
			for key, count := range tc.wantRequests {
				require.Equal(t, count, requests[key], "unexpected request count for %s", key)
			}
		})
	}
}
