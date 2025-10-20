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
	"testing"

	"github.com/stretchr/testify/require"
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
		input    interface{}
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
