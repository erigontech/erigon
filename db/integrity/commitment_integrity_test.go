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

package integrity

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type countingStateReader struct {
	commitmentdb.StateReader
	reads map[string]int
}

func (r *countingStateReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) ([]byte, kv.Step, error) {
	r.reads[string(plainKey)]++
	return r.StateReader.Read(d, plainKey, stepSize)
}

func TestTouchHistoricalKeysReadsEachKeyOnce(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(true))
	ctx := t.Context()
	tx, err := temporaltest.NewTestDB(t, datadir.New(t.TempDir())).BeginTemporalRw(ctx)
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	sd1, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd1.Close)
	addr := common.HexToAddress("0x01")
	for txNum := uint64(1); txNum <= 3; txNum++ {
		acc := accounts.NewAccount()
		acc.Balance = *uint256.NewInt(txNum)
		require.NoError(t, sd1.DomainPut(kv.AccountsDomain, tx, addr[:], accounts.SerialiseV3(&acc), txNum, nil))
	}
	require.NoError(t, sd1.Flush(ctx, tx))

	sd2, err := execctx.NewSharedDomains(ctx, tx, log.New())
	require.NoError(t, err)
	t.Cleanup(sd2.Close)
	reader := &countingStateReader{StateReader: commitmentdb.NewLatestStateReader(tx, sd2, commitmentdb.LatestStateReaderOptions{}), reads: map[string]int{}}
	sd2.GetCommitmentCtx().SetStateReader(reader)
	touches, err := touchHistoricalKeys(sd2, tx, kv.AccountsDomain, 0, 4, 0, nil, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(3), touches)
	require.Equal(t, map[string]int{string(addr[:]): 1}, reader.reads)
}
