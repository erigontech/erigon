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

package attestation_producer_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/validator/attestation_producer"
	"github.com/stretchr/testify/require"
)

func TestAttestationProducer(t *testing.T) {
	attProducer := attestation_producer.New(context.Background(), &clparams.MainnetBeaconConfig)

	_, _, headState := tests.GetPhase0Random()
	root, err := headState.BlockRoot()
	require.NoError(t, err)

	att, err := attProducer.ProduceAndCacheAttestationData(nil, headState, root, headState.Slot())
	require.NoError(t, err)

	attJson, err := json.Marshal(att)
	require.NoError(t, err)

	// check if the json match with the expected value
	require.JSONEq(t, string(attJson), `{"slot":"8322","index":"0","beacon_block_root":"0xeffdd8ef40c3c901f0724d48e04ce257967cf1da31929f3b6db614f89ef8d660","source":{"epoch":"258","root":"0x31885d5a2405876b7203f9cc1a7e115b9977412107c51c81ab4fd49bde93905e"},"target":{"epoch":"260","root":"0x86979f6f6dc7626064ef0d38d4dffb89e91d1d4c18492e3fb7d7ee93cedca3ed"}}`)
}
