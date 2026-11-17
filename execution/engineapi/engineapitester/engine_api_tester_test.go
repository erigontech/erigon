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

package engineapitester

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/jwt"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

func TestEngineApiTesterSlowRequest(t *testing.T) {
	defaultTimeouts := rpccfg.DefaultHTTPTimeouts
	rpccfg.DefaultHTTPTimeouts.ReadTimeout = time.Second
	t.Cleanup(func() { rpccfg.DefaultHTTPTimeouts = defaultTimeouts })

	genesis, _, err := DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	clientTimeout := time.Minute
	tester, err := InitialiseEngineApiTester(t.Context(), EngineApiTesterInitArgs{
		Logger:                 testlog.Logger(t, log.LvlError),
		DataDir:                t.TempDir(),
		Genesis:                genesis,
		NoEmptyBlock1:          true,
		EngineApiClientTimeout: &clientTimeout,
		DisableTxPool:          true,
		DisableSentry:          true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, tester.Close()) })

	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = reader.Close() })
	go func() {
		defer writer.Close()
		if _, err := io.WriteString(writer, `{"jsonrpc":"2.0",`); err != nil {
			return
		}
		timer := time.NewTimer(2 * time.Second)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-t.Context().Done():
			return
		}
		_, _ = io.WriteString(writer, `"id":1,"method":"engine_exchangeCapabilities","params":[[]]}`)
	}()

	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, tester.EngineApiUrl, reader)
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	transport := &http.Transport{}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Timeout: clientTimeout, Transport: jwt.NewHttpRoundTripper(transport, tester.JwtSecret)}
	response, err := client.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode, string(body))
	var result struct {
		ID     int      `json:"id"`
		Result []string `json:"result"`
	}
	require.NoError(t, json.Unmarshal(body, &result))
	require.Equal(t, 1, result.ID)
	require.Contains(t, result.Result, "engine_newPayloadV5")
}
