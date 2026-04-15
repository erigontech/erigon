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

package app_test

import (
	"strconv"
	"testing"

	"github.com/erigontech/erigon/node/app"
	"github.com/stretchr/testify/require"
)

func TestCreateDomains(t *testing.T) {
	for i := 0; i < 10; i++ {
		d, err := app.NewDomain[int]()
		require.NoError(t, err)
		require.NotNil(t, d)
		require.Equal(t, d.Id().String(), strconv.Itoa(i))
	}

	d, err := app.NewNamedDomain[int]("test")
	require.NoError(t, err)
	require.NotNil(t, d)
	require.Equal(t, d.Id().String(), "test")
	d1, err := app.NewNamedDomain[int]("test")
	require.NoError(t, err)
	require.NotNil(t, d1)
	require.Equal(t, d.Id().String(), "test")
	require.True(t, d == d1)
}
