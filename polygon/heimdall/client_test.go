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
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/turbo/testlog"
)

type emptyBodyReadCloser struct{}

func (ebrc emptyBodyReadCloser) Read(_ []byte) (n int, err error) {
	return 0, io.EOF
}

func (ebrc emptyBodyReadCloser) Close() error {
	return nil
}

func TestHeimdallClientFetchesTerminateUponTooManyErrors(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	httpClient := NewMockHttpClient(ctrl)
	httpClient.EXPECT().
		Do(gomock.Any()).
		Return(&http.Response{
			StatusCode: 404,
			Body:       emptyBodyReadCloser{},
		}, nil).
		Times(5)
	logger := testlog.Logger(t, log.LvlDebug)
	heimdallClient := newHeimdallClient("https://dummyheimdal.com", httpClient, 100*time.Millisecond, 5, logger)

	spanRes, err := heimdallClient.FetchSpan(ctx, 1534)
	require.Nil(t, spanRes)
	require.Error(t, err)
}

func TestHeimdallClientStateSyncEventsReturnsErrNoResponseWhenHttp200WithEmptyBody(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	httpClient := NewMockHttpClient(ctrl)
	httpClient.EXPECT().
		Do(gomock.Any()).
		Return(&http.Response{
			StatusCode: 200,
			Body:       emptyBodyReadCloser{},
		}, nil).
		Times(2)
	logger := testlog.Logger(t, log.LvlDebug)
	heimdallClient := newHeimdallClient("https://dummyheimdal.com", httpClient, time.Millisecond, 2, logger)

	spanRes, err := heimdallClient.FetchStateSyncEvents(ctx, 100, time.Now(), 0)
	require.Nil(t, spanRes)
	require.ErrorIs(t, err, ErrNoResponse)
}
