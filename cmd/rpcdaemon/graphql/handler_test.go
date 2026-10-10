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

package graphql

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
)

func TestGraphQLRequestBodyLimit(t *testing.T) {
	t.Parallel()

	h := CreateHandler(nil)
	send := func(body io.Reader, contentLength int64) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, urlPath, body)
		req.Header.Set("Content-Type", "application/json")
		req.ContentLength = contentLength
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	small := `{"query":"{__typename}"}`
	rec := send(strings.NewReader(small), int64(len(small)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	big := small + strings.Repeat(" ", maxRequestBodySize)
	rec = send(strings.NewReader(big), int64(len(big)))
	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)

	rec = send(bytes.NewReader([]byte(big)), -1)
	require.NotEqual(t, http.StatusOK, rec.Code, "a body without Content-Length must be cut off at the limit too")
}

type overloadedGraphQLAPI struct{ jsonrpc.GraphQLAPI }

func (overloadedGraphQLAPI) GasPrice(context.Context) (string, error) {
	return "", kv.ErrReadTxLimitExceeded
}

func TestGraphQLReadTxLimitIsServiceUnavailable(t *testing.T) {
	t.Parallel()

	h := CreateHandler([]rpc.API{{Service: overloadedGraphQLAPI{}}})
	query := `{"query":"{gasPrice}"}`
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, urlPath, strings.NewReader(query))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	require.Equal(t, "1", rec.Header().Get("Retry-After"))
}
