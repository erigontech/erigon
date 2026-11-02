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

package raw

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

func TestGloasStateJSONExecutionPayloadAvailabilityIsHex(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	state := New(&cfg)
	state.version = clparams.GloasVersion
	state.SetExecutionPayloadAvailability(0, true)

	out, err := json.Marshal(state)
	require.NoError(t, err)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(out, &fields))
	want := "0x01" + strings.Repeat("00", int(cfg.SlotsPerHistoricalRoot)/8-1)
	require.Equal(t, want, fields["execution_payload_availability"])
}
