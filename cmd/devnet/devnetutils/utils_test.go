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

package devnetutils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHexToInt(t *testing.T) {
	testCases := []struct {
		hexStr   string
		expected uint64
	}{
		{"0x0", 0},
		{"0x32424", 205860},
		{"0x200", 512},
		{"0x39", 57},
	}

	for _, testCase := range testCases {
		got := HexToInt(testCase.hexStr)
		require.Equal(t, testCase.expected, got)
	}
}

func TestUniqueIDFromEnode(t *testing.T) {
	testCases := []struct {
		input       string
		expectedRes string
		shouldError bool
	}{
		{
			input:       "",
			expectedRes: "",
			shouldError: true,
		},
		{
			input:       "enode://11c368e7a2775951d66ff155a982844ccd5219d10b53e310001e1e40c6a4e76c2f6e42f39acc1e4015cd3b7428765125214d89b07ca5fa2c19ac94746fc360b0@127.0.0.1:63380?discport=0",
			expectedRes: "enode://11c368e7a2775951d66ff155a982844ccd5219d10b53e310001e1e40c6a4e76c2f6e42f39acc1e4015cd3b7428765125214d89b07ca5fa2c19ac94746fc360b0@127.0.0.1:63380",
			shouldError: false,
		},
		{
			input:       "enode://11c368e7a2775951d66ff155a982844ccd5219d10b53e310001e1e40c6a4e76c2f6e42f39acc1e4015cd3b7428765125214d89b07ca5fa2c19ac94746fc360b0@127.0.0.1:63380",
			expectedRes: "enode://11c368e7a2775951d66ff155a982844ccd5219d10b53e310001e1e40c6a4e76c2f6e42f39acc1e4015cd3b7428765125214d89b07ca5fa2c19ac94746fc360b0@127.0.0.1:63380",
			shouldError: false,
		},
		{
			input:       "enode://11c368e7a2775951d66ff155a982844ccd5219d10b53e310001e1e40c6a4e76c2f6e42f39acc1e4015cd3b7428765125214d89b07ca5fa2c19ac94746fc360b0@127.0.0.1:63380discport=0",
			expectedRes: "",
			shouldError: true,
		},
	}

	for _, testCase := range testCases {
		got, err := UniqueIDFromEnode(testCase.input)
		if testCase.shouldError && err == nil {
			t.Errorf("expected error to happen, got no error")
		}
		if !testCase.shouldError && err != nil {
			t.Errorf("expected no error, got %s", err)
		}
		require.EqualValues(t, testCase.expectedRes, got)
	}
}

func TestNamespaceAndSubMethodFromMethod(t *testing.T) {
	expectedError := fmt.Errorf("invalid string to split")

	testCases := []struct {
		method            string
		expectedNamespace string
		expectedSubMethod string
		shouldError       bool
		expectedError     error
	}{
		{
			"eth_logs",
			"eth",
			"logs",
			false,
			nil,
		},
		{
			"ethNewHeads",
			"",
			"",
			true,
			expectedError,
		},
		{
			"",
			"",
			"",
			true,
			expectedError,
		},
	}

	for _, testCase := range testCases {
		namespace, subMethod, err := NamespaceAndSubMethodFromMethod(testCase.method)
		require.Equal(t, testCase.expectedNamespace, namespace)
		require.Equal(t, testCase.expectedSubMethod, subMethod)
		require.Equal(t, testCase.expectedError, err)
		if testCase.shouldError {
			require.Errorf(t, testCase.expectedError, expectedError.Error())
		}
	}
}

func TestGenerateTopic(t *testing.T) {
	testCases := []struct {
		signature string
		expected  string
	}{
		{"random string", "0x0d9d89437ff2d48ce95779dc9457bc48287b75a573eddbf50954efac5a97c4b9"},
		{"SubscriptionEvent()", "0x67abc7edb0ab50964ef0e90541d39366b9c69f6f714520f2ff4570059ee8ad80"},
		{"", "0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"},
	}

	for _, testCase := range testCases {
		got := GenerateTopic(testCase.signature)
		require.Equal(t, testCase.expected, fmt.Sprintf("%s", got[0]))
	}
}
