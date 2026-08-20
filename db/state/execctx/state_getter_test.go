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

package execctx_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
)

type valSizeTx struct {
	kv.TemporalTx
	size  int
	found bool
}

func (tx valSizeTx) GetLatestValSize(kv.Domain, []byte) (int, bool, error) {
	return tx.size, tx.found, nil
}

func TestTemporalTxStateGetterCodePresence(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		size      int
		keyFound  bool
		codeFound bool
	}{
		{name: "missing", keyFound: false, codeFound: false},
		{name: "empty record", keyFound: true, codeFound: false},
		{name: "code", size: 3, keyFound: true, codeFound: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			getter := execctx.NewTemporalTxStateGetter(valSizeTx{size: test.size, found: test.keyFound})
			size, found, err := getter.GetCodeSize(nil, 0)
			require.NoError(t, err)
			require.Equal(t, test.size, size)
			require.Equal(t, test.codeFound, found)
		})
	}
}
