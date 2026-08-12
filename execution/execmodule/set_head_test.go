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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
)

func TestSetHeadClearsRetainedExtendingFork(t *testing.T) {
	ctx := t.Context()
	m := execmoduletester.New(t)
	chainPack, err := m.GenerateChain(3, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertValidateAndUfc1By1(ctx, chainPack.Blocks[:2]))

	status, err := m.InsertBlocks(ctx, chainPack.Blocks[2:])
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	validation, err := m.ValidateChain(ctx, chainPack.Blocks[2].Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, validation.ValidationStatus)

	extendingHash, extendingNumber, extendingDomains := m.ForkValidator.ExtendingFork()
	require.Equal(t, chainPack.Blocks[2].Hash(), extendingHash)
	require.Equal(t, uint64(3), extendingNumber)
	require.NotNil(t, extendingDomains)

	require.NoError(t, m.ExecModule.SetHead(ctx, 1))

	extendingHash, extendingNumber, extendingDomains = m.ForkValidator.ExtendingFork()
	require.Equal(t, common.Hash{}, extendingHash)
	require.Zero(t, extendingNumber)
	require.Nil(t, extendingDomains)
}
