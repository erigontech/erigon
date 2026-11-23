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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

func TestDepositUnmarshalJSONRejectsOversizedProof(t *testing.T) {
	encoded, err := json.Marshal(struct {
		Proof []common.Hash        `json:"proof"`
		Data  *cltypes.DepositData `json:"data"`
	}{
		Proof: make([]common.Hash, 4097),
		Data:  new(cltypes.DepositData),
	})
	require.NoError(t, err)
	var deposit cltypes.Deposit
	require.Error(t, json.Unmarshal(encoded, &deposit))
}

func TestDepositUnmarshalJSONRejectsNullFields(t *testing.T) {
	for _, input := range []string{
		`{"proof":null,"data":{}}`,
		`{"proof":[],"data":null}`,
	} {
		t.Run(input, func(t *testing.T) {
			var deposit cltypes.Deposit
			require.ErrorContains(t, json.Unmarshal([]byte(input), &deposit), "null")
		})
	}
}

func TestDepositUnmarshalJSONAcceptsOmittedFields(t *testing.T) {
	var deposit cltypes.Deposit
	require.NoError(t, json.Unmarshal([]byte(`{}`), &deposit))
	require.NotNil(t, deposit.Proof)
	require.NotNil(t, deposit.Data)
}
