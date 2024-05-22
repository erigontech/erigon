package heimdall

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ledgerwatch/erigon/turbo/testlog"
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
