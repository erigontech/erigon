// Copyright 2025 The Erigon Authors
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

package helper

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const validEnode = "enode://abc@1.2.3.4:30303"

// A malformed admin_nodeInfo (e.g. from an unrelated service that grabbed the
// port) must yield an error, never a panic. An empty genesis previously panicked
// when converted to [32]byte.
func TestParseNodeInfoRejectsInvalidNode(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"empty genesis", `{"result":{"enode":"` + validEnode + `","protocols":{"eth":{"genesis":"","network":1337}}}}`},
		{"short genesis", `{"result":{"enode":"` + validEnode + `","protocols":{"eth":{"genesis":"0x1234","network":1337}}}}`},
		{"non-hex genesis", `{"result":{"enode":"` + validEnode + `","protocols":{"eth":{"genesis":"0x` + strings.Repeat("zz", 32) + `","network":1337}}}}`},
		{"empty enode", `{"result":{"enode":"","protocols":{"eth":{"genesis":"0x` + strings.Repeat("ab", 32) + `","network":1337}}}}`},
		{"not json", `not json`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				_, err := parseNodeInfo(strings.NewReader(tc.body))
				require.Error(t, err)
			})
		})
	}
}

func TestParseNodeInfoValid(t *testing.T) {
	genesis := "0x" + strings.Repeat("ab", 32)
	info, err := parseNodeInfo(strings.NewReader(
		`{"result":{"enode":"` + validEnode + `","protocols":{"eth":{"genesis":"` + genesis + `","network":1337,"difficulty":1}}}}`,
	))
	require.NoError(t, err)
	require.Equal(t, validEnode, info.Enode)
	require.Equal(t, uint64(1337), info.NetworkID)
	require.Equal(t, uint64(1), info.Difficulty.Uint64())

	var want [32]byte
	for i := range want {
		want[i] = 0xab
	}
	require.Equal(t, want, info.Genesis)
}
