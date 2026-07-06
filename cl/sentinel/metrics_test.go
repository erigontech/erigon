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

package sentinel

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	if peerCountGauge != nil {
		fmt.Fprintln(os.Stderr, "caplin_peer_count gauge is registered before sentinel metrics are recorded")
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestRecordPeerMetrics(t *testing.T) {
	recordPeerMetrics(7)
	require.NotNil(t, peerCountGauge)
	require.Equal(t, float64(7), peerCountGauge.GetValue())
}
