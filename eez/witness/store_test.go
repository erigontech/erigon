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

package witness

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/rpc/jsonrpc"
)

// TestPutIsDurable: a kept witness is on disk once Put returns. Nothing
// rebuilds a witness, so one a crash rolls back is lost for good.
func TestPutIsDurable(t *testing.T) {
	s, err := Open(t.Context(), t.TempDir(), log.New())
	require.NoError(t, err)
	t.Cleanup(s.Close)

	w := &jsonrpc.ExecutionWitnessResult{State: []hexutil.Bytes{make([]byte, 4096)}}
	require.NoError(t, s.Put(t.Context(), 1, common.Hash{1}, w))
	info, err := s.db.(*mdbx.MdbxKV).Env().Info(nil)
	require.NoError(t, err)
	require.Zero(t, info.UnsyncedBytes, "a kept witness waits for a deferred sync")
}
