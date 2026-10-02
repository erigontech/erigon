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

package requests

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

func MockRequestGenerator(reqId int) *requestGenerator {
	return &requestGenerator{
		reqID:  reqId,
		client: nil,
	}
}

func TestRequestGenerator_TxpoolContent(t *testing.T) {
	testCases := []struct {
		reqId    int
		expected string
	}{
		{1, `{"jsonrpc":"2.0","method":"txpool_content","params":[],"id":1}`},
		{2, `{"jsonrpc":"2.0","method":"txpool_content","params":[],"id":2}`},
		{3, `{"jsonrpc":"2.0","method":"txpool_content","params":[],"id":3}`},
	}

	for _, testCase := range testCases {
		reqGen := MockRequestGenerator(testCase.reqId)
		_, got := reqGen.txpoolContent()
		require.Equal(t, testCase.expected, got)
	}
}

func TestParseResponse(t *testing.T) {
	type Person struct {
		Name string
		Age  int
	}

	testCases := []struct {
		input    any
		expected string
	}{
		{
			Person{
				Name: "Leonard",
				Age:  10,
			},
			`{"Name":"Leonard","Age":10}`,
		},
		{
			struct {
				Person struct {
					Name string
					Age  int
				}
				WorkID string
			}{
				Person: Person{
					Name: "Uzi",
					Age:  23,
				},
				WorkID: "123456",
			},
			`{"Person":{"Name":"Uzi","Age":23},"WorkID":"123456"}`,
		},
	}

	for _, testCase := range testCases {
		got, _ := parseResponse(testCase.input)
		require.Equal(t, testCase.expected, got)
	}
}

// TraceCall sends a call object the server accepts: an empty data is filled in only when the
// call sets neither data nor input, so it never disagrees with the input.
func TestRequestGenerator_TraceCallData(t *testing.T) {
	bodies := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies <- body
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":{"output":"0x"}}`))
	}))
	t.Cleanup(server.Close)
	reqGen := NewRequestGenerator(strings.TrimPrefix(server.URL, "http://"), log.New())

	calldata := hexutil.Bytes{0xaa}
	for _, tc := range []struct {
		name string
		args ethapi.CallArgs
		want hexutil.Bytes
	}{
		{name: "neither", args: ethapi.CallArgs{}, want: hexutil.Bytes{}},
		{name: "data", args: ethapi.CallArgs{Data: &calldata}, want: calldata},
		{name: "input", args: ethapi.CallArgs{Input: &calldata}, want: calldata},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := reqGen.TraceCall(rpc.LatestBlock, tc.args)
			require.NoError(t, err)
			var req struct {
				Params []json.RawMessage `json:"params"`
			}
			require.NoError(t, json.Unmarshal(<-bodies, &req))
			var sent ethapi.CallArgs
			require.NoError(t, json.Unmarshal(req.Params[0], &sent))
			if sent.Input != nil {
				require.Equal(t, tc.want, *sent.Input)
			} else {
				require.Equal(t, tc.want, *sent.Data)
			}
		})
	}
}
