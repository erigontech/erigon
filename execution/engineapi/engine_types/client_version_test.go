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

package engine_types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewClientVersionV1Commit(t *testing.T) {
	require.Equal(t, "0xa53e9545", NewClientVersionV1("EG", "erigon", "1.2.3", "a53e9545abcd").Commit)
	require.Equal(t, "0xa53e9545", NewClientVersionV1("EG", "erigon", "1.2.3", "0xa53e9545abcd").Commit)
	require.Equal(t, "0x00000000", NewClientVersionV1("EG", "erigon", "1.2.3", "").Commit)
	require.Equal(t, "0x00000000", NewClientVersionV1("EG", "erigon", "1.2.3", "0xabc").Commit)
}
