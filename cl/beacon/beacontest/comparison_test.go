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

package beacontest

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComparisonExprList(t *testing.T) {
	// Singular expr must be evaluated, not silently dropped.
	require.Equal(t, []string{"actual_code == 400"}, (&Comparison{Expr: "actual_code == 400"}).exprList())
	// Plural exprs are used as-is.
	require.Equal(t, []string{"a", "b"}, (&Comparison{Exprs: []string{"a", "b"}}).exprList())
	// Both forms combine.
	require.Equal(t, []string{"a", "actual_code == 400"}, (&Comparison{Exprs: []string{"a"}, Expr: "actual_code == 400"}).exprList())
	// Neither falls back to the defaults.
	require.Equal(t, []string{"actual_code == 200", "actual == expect"}, (&Comparison{}).exprList())
}
