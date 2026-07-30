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

package testutil

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func TestStateTestExpectedErrorIgnoresPostState(t *testing.T) {
	test := StateTest{
		Json: stJSON{
			Post: map[string][]stPostState{
				"Frontier": {
					{
						Root:            common.UnprefixedHash{1},
						ExpectException: "TR_TypeNotSupported",
					},
				},
			},
		},
	}

	err := test.checkResult(
		StateSubtest{Fork: "Frontier"},
		nil,
		common.Hash{2},
		errors.New("transaction type not supported"),
	)

	require.NoError(t, err)
}
