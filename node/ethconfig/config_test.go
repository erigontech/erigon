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

package ethconfig

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCommitmentRefsFirstStart(t *testing.T) {
	t.Parallel()
	tr, fa := true, false

	require.Nil(t, (&Config{CommitmentPlainValues: nil}).CommitmentRefsFirstStart(), "unset => nil")

	// plainValues=true => references=false
	gotPlain := (&Config{CommitmentPlainValues: &tr}).CommitmentRefsFirstStart()
	require.NotNil(t, gotPlain)
	require.False(t, *gotPlain)

	// plainValues=false => references=true
	gotRefs := (&Config{CommitmentPlainValues: &fa}).CommitmentRefsFirstStart()
	require.NotNil(t, gotRefs)
	require.True(t, *gotRefs)
}
