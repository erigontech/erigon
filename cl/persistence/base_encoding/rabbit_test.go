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

package base_encoding

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRabbit(t *testing.T) {
	list := []uint64{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17, 23, 90}
	var w bytes.Buffer
	if err := WriteRabbits(list, &w); err != nil {
		t.Fatal(err)
	}
	var out []uint64
	out, err := ReadRabbits(out, &w)
	if err != nil {
		t.Fatal(err)
	}
	require.Equal(t, list, out)
}
