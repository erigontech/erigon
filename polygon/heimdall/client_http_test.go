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
	"github.com/erigontech/erigon-lib/testlog"
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
