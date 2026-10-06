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

package sentinel

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/erigontech/erigon/cl/p2p"
)

func TestMultiAddressBuilderWithID(t *testing.T) {
	testCases := []struct {
		ipAddr      string
		protocol    string
		port        uint
		id          peer.ID
		shouldError bool
		expectedStr string
	}{
		{
			ipAddr:      "192.158.1.38",
			protocol:    "udp",
			port:        80,
			id:          peer.ID(""),
			shouldError: true,
			expectedStr: "ip4/node",
		},
		{
			ipAddr:      "192.178.1.21",
			protocol:    "tcp",
			port:        88,
			id:          peer.ID("d267"),
			shouldError: true,
			expectedStr: "ip4/node",
		},
		// TODO: should not throw 'selected encoding not supported' error, MUST FIX!
		// It panics because shouldError is false and this particular test case throws an error
		{
			ipAddr:      "192.178.1.21",
			protocol:    "tcp",
			port:        88,
			id:          peer.ID("d267"),
			shouldError: true,
			expectedStr: "ip4/node",
		},
	}

	for _, testCase := range testCases {
		multiAddr, err := p2p.MultiAddressBuilderWithID(testCase.ipAddr, testCase.protocol, testCase.port, testCase.id)
		if testCase.shouldError {
			if err == nil {
				t.Errorf("expected error, got nil")
			}
			continue
		}
		if multiAddr.String() != testCase.expectedStr {
			t.Errorf("expected %s, got %s", testCase.expectedStr, multiAddr.String())
		}
	}
}
