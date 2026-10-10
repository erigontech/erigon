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

package rpc

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBudgetedReaderSettlesUndeclaredCharge pins that an undeclared body is charged the cap
// while it is read and holds only its own size once it is read to the end.
func TestBudgetedReaderSettlesUndeclaredCharge(t *testing.T) {
	budget := &ingressBudget{limit: maxRequestContentLength}
	size := int(smallBodyLimit) + 1
	body, ok := budget.admit(bytes.NewReader(make([]byte, size)), -1)
	require.True(t, ok)

	buf := make([]byte, size)
	_, err := io.ReadFull(body, buf)
	require.NoError(t, err)
	require.Equal(t, int64(maxRequestContentLength), budget.used.Load())

	_, err = body.Read(buf[:1])
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, int64(size), budget.used.Load())

	body.release()
	require.Zero(t, budget.used.Load())
}
