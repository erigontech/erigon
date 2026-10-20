// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the license, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompileTestFilter(t *testing.T) {
	filter, err := compileTestFilter("refundReset", []string{
		`/stTimeConsuming/`,
		`/bcStateTests/refundReset\.json::refundReset_Constantinople$`,
	})
	require.NoError(t, err)

	require.False(t, filter.includeFile("/fixtures/stTimeConsuming/loop.json"))
	require.True(t, filter.includeFile("/fixtures/ValidBlocks/bcStateTests/refundReset.json"))
	require.False(t, filter.includeCase(
		"/fixtures/ValidBlocks/bcStateTests/refundReset.json",
		"refundReset_Constantinople",
	))
	require.True(t, filter.includeCase(
		"/fixtures/ValidBlocks/bcStateTests/refundReset.json",
		"refundReset_Berlin",
	))
	require.False(t, filter.includeCase(
		"/fixtures/ValidBlocks/bcStateTests/other.json",
		"unrelated",
	))
}

func TestCompileTestFilterRejectsInvalidExclude(t *testing.T) {
	_, err := compileTestFilter(".*", []string{"["})
	require.ErrorContains(t, err, `invalid regex --exclude`)
}
