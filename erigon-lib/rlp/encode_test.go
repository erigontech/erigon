/*
   Copyright 2023 The Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package rlp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common/hexutility"
)

// Strings of length 56 are a boundary case.
// See https://ethereum.org/en/developers/docs/data-structures-and-encoding/rlp/#definition
func TestStringLen56(t *testing.T) {
	str := hexutility.MustDecodeHex("7907ca011864321def1e92a3021868f397516ce37c959f25f8dddd3161d7b8301152b35f135c814fae9f487206471b6b0d713cd51a2d3598")
	require.Equal(t, 56, len(str))

	strLen := StringLen(str)
	assert.Equal(t, 56+2, strLen)

	encoded := make([]byte, strLen)
	EncodeString(str, encoded)

	dataPos, dataLen, err := String(encoded, 0)
	require.NoError(t, err)
	assert.Equal(t, dataPos, 2)
	assert.Equal(t, dataLen, 56)
}
