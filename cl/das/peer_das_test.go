package das

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/sentinel/httpreqresp"
)

func TestIsExpectedColumnDownloadMiss(t *testing.T) {
	require.False(t, isExpectedColumnDownloadMiss(nil))
	require.True(t, isExpectedColumnDownloadMiss(&httpreqresp.PeerResponseError{
		Code: httpreqresp.ResponseCodeResourceUnavailable,
	}))
	require.True(t, isExpectedColumnDownloadMiss(fmt.Errorf("column miss: %w", &httpreqresp.PeerResponseError{
		Code: httpreqresp.ResponseCodeResourceUnavailable,
	})))
	require.False(t, isExpectedColumnDownloadMiss(&httpreqresp.PeerResponseError{
		Code:    httpreqresp.ResponseCodeServerError,
		Message: "broken",
	}))
	require.False(t, isExpectedColumnDownloadMiss(&httpreqresp.HTTPError{
		StatusCode: 400,
		Body:       "Read Code: EOF",
	}))
	require.False(t, isExpectedColumnDownloadMiss(errors.New("peer error code: 2 (server error). Error message: broken")))
}
