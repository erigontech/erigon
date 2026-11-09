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

package ethstats

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStatsURLsUsePlaintextOnlyWhenAskedTo(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		host string
		want []string
	}{
		{"stats.example:3000", []string{"wss://stats.example:3000/api"}},
		{"wss://stats.example:3000", []string{"wss://stats.example:3000/api"}},
		{"ws://stats.example:3000", []string{"ws://stats.example:3000/api"}},
	} {
		require.Equal(t, tc.want, statsURLs(tc.host), tc.host)
	}
}
