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

package rpc

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

func TestAllowListMarshaling(t *testing.T) {

}

func TestAllowListUnmarshaling(t *testing.T) {
	allowListJSON := `[ "one", "two", "three" ]`

	var allowList AllowList
	err := json.Unmarshal([]byte(allowListJSON), &allowList)
	require.NoError(t, err, "should unmarshal successfully")

	m := map[string]struct{}{"one": {}, "two": {}, "three": {}}
	assert.Equal(t, allowList, AllowList(m))
}

func TestAllowListAppliesToEveryTransport(t *testing.T) {
	t.Parallel()
	logger := log.New()
	srv := newTestServer(logger)
	defer srv.Stop()
	srv.SetAllowList(AllowList{"test_echo": {}})

	httpsrv := httptest.NewServer(srv)
	defer httpsrv.Close()
	wssrv := httptest.NewServer(srv.WebsocketHandler([]string{"*"}, nil, false, logger))
	defer wssrv.Close()

	dialURL := func(url string) func(t *testing.T) *Client {
		return func(t *testing.T) *Client {
			client, err := DialContext(t.Context(), url, logger)
			require.NoError(t, err)
			return client
		}
	}
	for name, dial := range map[string]func(t *testing.T) *Client{
		"http":  dialURL(httpsrv.URL),
		"ws":    dialURL("ws:" + strings.TrimPrefix(wssrv.URL, "http:")),
		"codec": func(*testing.T) *Client { return DialInProc(srv, logger) }, // the path IPC takes
	} {
		t.Run(name, func(t *testing.T) {
			client := dial(t)
			defer client.Close()

			var res echoResult
			require.NoError(t, client.Call(&res, "test_echo", "x", 1, &echoArgs{S: "y"}))
			require.ErrorContains(t, client.Call(nil, "test_noArgsRets"), "does not exist/is not available")
		})
	}
}
