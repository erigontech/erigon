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

package cltypes_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
)

func TestExecutionRequestsEncodingSizeSSZ(t *testing.T) {
	tests := []struct {
		name    string
		version clparams.StateVersion
	}{
		{name: "electra", version: clparams.ElectraVersion},
		{name: "gloas", version: clparams.GloasVersion},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requests := cltypes.NewExecutionRequestsWithVersion(&clparams.MainnetBeaconConfig, test.version)
			encoded, err := requests.EncodeSSZ(nil)
			require.NoError(t, err)
			require.Equal(t, len(encoded), requests.EncodingSizeSSZ())
		})
	}
}
