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

package state_test

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func Fuzz_AggregatorV3_Merge(f *testing.F) {
	db, agg := testFuzzDbAndAggregatorv3(f, 10)

	rwTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(f, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(rwTx, log.New())
	require.NoError(f, err)
	defer domains.Close()

	const txs = uint64(1000)

	var (
		commKey1 = []byte("someCommKey")
		commKey2 = []byte("otherCommKey")
	)

	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	var maxWrite, otherMaxWrite uint64
	//f.Add([]common.Address{common.HexToAddress("0x123"), common.HexToAddress("0x456")})
	//f.Add([]common.Hash{common.HexToHash("0x123"), common.HexToHash("0x456")})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < int(txs*(length.Addr+length.Hash)) {
			t.Skip()
		}
		addrData := data[:txs*length.Addr]
		locData := data[txs*length.Addr : txs*(length.Addr+length.Hash)]
		addrs := make([]common.Address, 1000)
		for i := 0; i < 1000; i++ {
			copy(addrs[i][:], addrData[i*length.Addr:(i+1)*length.Addr])
		}
		locs := make([]common.Address, 1000)
		for i := 0; i < 1000; i++ {
			copy(locs[i][:], locData[i*length.Hash:(i+1)*length.Hash])
		}
		for txNum := uint64(1); txNum <= txs; txNum++ {
			acc := accounts.Account{
				Nonce:       1,
				Balance:     uint256.Int{},
				CodeHash:    common.Hash{},
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			err = domains.DomainPut(kv.AccountsDomain, rwTx, addrs[txNum].Bytes(), buf, txNum, nil, 0)
			require.NoError(t, err)

			err = domains.DomainPut(kv.StorageDomain, rwTx, composite(addrs[txNum].Bytes(), locs[txNum].Bytes()), []byte{addrs[txNum].Bytes()[0], locs[txNum].Bytes()[0]}, txNum, nil, 0)
			require.NoError(t, err)

			var v [8]byte
			binary.BigEndian.PutUint64(v[:], txNum)
			if txNum%135 == 0 {
				pv, step, err := rwTx.GetLatest(kv.CommitmentDomain, commKey2)
				require.NoError(t, err)

				err = domains.DomainPut(kv.CommitmentDomain, rwTx, commKey2, v[:], txNum, pv, step)
				require.NoError(t, err)
				otherMaxWrite = txNum
			} else {
				pv, step, err := rwTx.GetLatest(kv.CommitmentDomain, commKey1)
				require.NoError(t, err)

				err = domains.DomainPut(kv.CommitmentDomain, rwTx, commKey1, v[:], txNum, pv, step)
				require.NoError(t, err)
				maxWrite = txNum
			}
			require.NoError(t, err)

		}

		err = domains.Flush(context.Background(), rwTx)
		require.NoError(t, err)

		require.NoError(t, err)
		err = rwTx.Commit()
		require.NoError(t, err)

		err = agg.BuildFiles(txs)
		require.NoError(t, err)

		rwTx, err = db.BeginTemporalRw(context.Background())
		require.NoError(t, err)
		defer rwTx.Rollback()

		_, err := rwTx.PruneSmallBatches(context.Background(), time.Hour)
		require.NoError(t, err)

		err = rwTx.Commit()
		require.NoError(t, err)

		err = agg.MergeLoop(context.Background())
		require.NoError(t, err)

		// Check the history
		roTx, err := db.BeginTemporalRo(context.Background())
		require.NoError(t, err)
		defer roTx.Rollback()

		v, _, err := roTx.GetLatest(kv.CommitmentDomain, commKey1)
		require.NoError(t, err)
		require.NotNil(t, v, "key %x not found", commKey1)

		require.Equal(t, maxWrite, binary.BigEndian.Uint64(v[:]))

		v, _, err = roTx.GetLatest(kv.CommitmentDomain, commKey2)
		require.NoError(t, err)
		require.NotNil(t, v, "key %x not found", commKey2)

		require.Equal(t, otherMaxWrite, binary.BigEndian.Uint64(v[:]))
	})

}

func Fuzz_AggregatorV3_MergeValTransform(f *testing.F) {
	db, agg := testFuzzDbAndAggregatorv3(f, 10)
	agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, true)

	rwTx, err := db.BeginTemporalRw(context.Background())
	require.NoError(f, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(rwTx, log.New())
	require.NoError(f, err)
	defer domains.Close()

	const txs = uint64(1000)

	state := make(map[string][]byte)

	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	//var maxWrite, otherMaxWrite uint64
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < int(txs*(length.Addr+length.Hash)) {
			t.Skip()
		}
		addrData := data[:txs*length.Addr]
		locData := data[txs*length.Addr : txs*(length.Addr+length.Hash)]
		addrs := make([]common.Address, 1000)
		for i := 0; i < 1000; i++ {
			copy(addrs[i][:], addrData[i*length.Addr:(i+1)*length.Addr])
		}
		locs := make([]common.Address, 1000)
		for i := 0; i < 1000; i++ {
			copy(locs[i][:], locData[i*length.Hash:(i+1)*length.Hash])
		}
		for txNum := uint64(1); txNum <= txs; txNum++ {
			acc := accounts.Account{
				Nonce:       1,
				Balance:     *uint256.NewInt(txNum * 1e6),
				CodeHash:    common.Hash{},
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			err = domains.DomainPut(kv.AccountsDomain, rwTx, addrs[txNum].Bytes(), buf, txNum, nil, 0)
			require.NoError(t, err)

			k := composite(addrs[txNum].Bytes(), locs[txNum].Bytes())
			v := []byte{addrs[txNum].Bytes()[0], locs[txNum].Bytes()[0]}
			err = domains.DomainPut(kv.StorageDomain, rwTx, k, v, txNum, nil, 0)
			require.NoError(t, err)

			if (txNum+1)%agg.StepSize() == 0 {
				_, err := domains.ComputeCommitment(context.Background(), rwTx, true, txNum/10, txNum, "", nil)
				require.NoError(t, err)
			}

			state[string(addrs[txNum].Bytes())] = buf
			state[string(addrs[txNum].Bytes())+string(locs[txNum].Bytes())] = []byte{addrs[txNum].Bytes()[0], locs[txNum].Bytes()[0]}
		}

		err = domains.Flush(context.Background(), rwTx)
		require.NoError(t, err)

		err = rwTx.Commit()
		require.NoError(t, err)

		err = agg.BuildFiles(txs)
		require.NoError(t, err)

		rwTx, err = db.BeginTemporalRw(context.Background())
		require.NoError(t, err)
		defer rwTx.Rollback()

		_, err := rwTx.PruneSmallBatches(context.Background(), time.Hour)
		require.NoError(t, err)

		err = rwTx.Commit()
		require.NoError(t, err)

		err = agg.MergeLoop(context.Background())
		require.NoError(t, err)
	})
}

func testFuzzDbAndAggregatorv3(f *testing.F, stepSize uint64) (kv.TemporalRwDB, *state.Aggregator) {
	f.Helper()
	require := require.New(f)
	dirs := datadir.New(f.TempDir())
	logger := log.New()
	db := mdbx.New(dbcfg.ChainDB, logger).InMem(f, dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	f.Cleanup(db.Close)

	agg, err := state.NewTest(dirs).StepSize(stepSize).Logger(logger).Open(f.Context(), db)
	require.NoError(err)
	f.Cleanup(agg.Close)
	err = agg.OpenFolder()
	require.NoError(err)
	tdb, err := temporal.New(db, agg)
	require.NoError(err)
	return tdb, agg
}
