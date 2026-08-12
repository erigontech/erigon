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

package execmodule_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
)

type headerNumberErrorBlockReader struct {
	dbservices.FullBlockReader
	err error
}

func (r headerNumberErrorBlockReader) HeaderNumber(context.Context, kv.Getter, common.Hash) (*uint64, error) {
	return nil, r.err
}

func TestValidateChainPropagatesCanonicalWalkError(t *testing.T) {
	ctx := t.Context()
	m := execmoduletester.New(t)
	chainPack, err := m.GenerateChain(1, nil)
	require.NoError(t, err)
	status, err := m.InsertBlocks(ctx, chainPack.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)

	canonicalReadErr := errors.New("canonical header lookup failed")
	m.ExecModule.SetBlockReaderForTest(headerNumberErrorBlockReader{
		FullBlockReader: m.BlockReader,
		err:             canonicalReadErr,
	})

	_, err = m.ValidateChain(ctx, chainPack.Blocks[0].Header())
	require.ErrorIs(t, err, canonicalReadErr)
}
