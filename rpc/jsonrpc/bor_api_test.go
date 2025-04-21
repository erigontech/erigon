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

package jsonrpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/polygon/bor/valset"
)

func TestUseSpanProducersReader(t *testing.T) {
	// test for Go's interface nil-ness caveat - https://codefibershq.com/blog/golang-why-nil-is-not-always-nil
	var spr *mockSpanProducersReader
	api := NewBorAPI(nil, nil, spr)
	require.False(t, api.useSpanProducersReader)
	spr = &mockSpanProducersReader{}
	api = NewBorAPI(nil, nil, spr)
	require.True(t, api.useSpanProducersReader)
}

var _ spanProducersReader = mockSpanProducersReader{}

type mockSpanProducersReader struct{}

func (m mockSpanProducersReader) Producers(context.Context, uint64) (*valset.ValidatorSet, error) {
	panic("mock")
}
