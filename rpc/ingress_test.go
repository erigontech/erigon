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

// TestBudgetedReaderChargesBytesPastSmallBodyLimit pins that a body is charged exactly for
// the bytes past smallBodyLimit it has delivered, and released as a whole.
func TestBudgetedReaderChargesBytesPastSmallBodyLimit(t *testing.T) {
	budget := &ingressBudget{limit: maxRequestContentLength}
	size := int(smallBodyLimit) + 3
	body := budget.reader(bytes.NewReader(make([]byte, size)))

	buf := make([]byte, size)
	_, err := io.ReadFull(body, buf[:smallBodyLimit])
	require.NoError(t, err)
	require.Zero(t, budget.used.Load())

	_, err = io.ReadFull(body, buf[smallBodyLimit:smallBodyLimit+1])
	require.NoError(t, err)
	require.Equal(t, int64(1), budget.used.Load())

	_, err = io.ReadFull(body, buf[smallBodyLimit+1:])
	require.NoError(t, err)
	_, err = body.Read(buf[:1])
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, int64(3), budget.used.Load())

	body.release()
	require.Zero(t, budget.used.Load())
}
