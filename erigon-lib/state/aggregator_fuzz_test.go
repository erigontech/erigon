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

//go:build !nofuzz

package state

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/holiman/uint256"

	"github.com/stretchr/testify/require"
)

func Fuzz_BtreeIndex_Allocation(f *testing.F) {
	f.Add(uint64(1_000_000), uint64(1024))
	f.Fuzz(func(t *testing.T, keyCount, M uint64) {
		if keyCount < M*4 || M < 4 {
			t.Skip()
		}
		bt := newBtAlloc(keyCount, M, false, nil, nil)
		bt.traverseDfs()
		require.GreaterOrEqual(t, bt.N, keyCount)

		require.LessOrEqual(t, float64(bt.N-keyCount)/float64(bt.N), 0.05)

	})
}

func Fuzz_AggregatorV3_Merge(f *testing.F) {
	db, agg := testFuzzDbAndAggregatorv3(f, 10)
	rwTx, err := db.BeginRwNosync(context.Background())
	require.NoError(f, err)
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()

	ac := agg.BeginFilesRo()
	defer ac.Close()
	domains, err := NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
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
			domains.SetTxNum(txNum)

			buf := types.EncodeAccountBytesV3(1, uint256.NewInt(0), nil, 0)
			err = domains.DomainPut(kv.AccountsDomain, addrs[txNum].Bytes(), nil, buf, nil, 0)
			require.NoError(t, err)

			err = domains.DomainPut(kv.StorageDomain, addrs[txNum].Bytes(), locs[txNum].Bytes(), []byte{addrs[txNum].Bytes()[0], locs[txNum].Bytes()[0]}, nil, 0)
			require.NoError(t, err)

			var v [8]byte
			binary.BigEndian.PutUint64(v[:], txNum)
			if txNum%135 == 0 {
				pv, step, _, err := ac.GetLatest(kv.CommitmentDomain, commKey2, nil, rwTx)
				require.NoError(t, err)

				err = domains.DomainPut(kv.CommitmentDomain, commKey2, nil, v[:], pv, step)
				require.NoError(t, err)
				otherMaxWrite = txNum
			} else {
				pv, step, _, err := ac.GetLatest(kv.CommitmentDomain, commKey1, nil, rwTx)
				require.NoError(t, err)

				err = domains.DomainPut(kv.CommitmentDomain, commKey1, nil, v[:], pv, step)
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
		rwTx = nil

		err = agg.BuildFiles(txs)
		require.NoError(t, err)

		rwTx, err = db.BeginRw(context.Background())
		require.NoError(t, err)
		defer rwTx.Rollback()

		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()
		stat, err := ac.Prune(context.Background(), rwTx, 0, logEvery)
		require.NoError(t, err)
		t.Logf("Prune: %s", stat)

		err = rwTx.Commit()
		require.NoError(t, err)

		err = agg.MergeLoop(context.Background())
		require.NoError(t, err)

		// Check the history
		roTx, err := db.BeginRo(context.Background())
		require.NoError(t, err)
		defer roTx.Rollback()

		dc := agg.BeginFilesRo()

		v, _, ex, err := dc.GetLatest(kv.CommitmentDomain, commKey1, nil, roTx)
		require.NoError(t, err)
		require.Truef(t, ex, "key %x not found", commKey1)

		require.EqualValues(t, maxWrite, binary.BigEndian.Uint64(v[:]))

		v, _, ex, err = dc.GetLatest(kv.CommitmentDomain, commKey2, nil, roTx)
		require.NoError(t, err)
		require.Truef(t, ex, "key %x not found", commKey2)
		dc.Close()

		require.EqualValues(t, otherMaxWrite, binary.BigEndian.Uint64(v[:]))
	})

}

func Fuzz_AggregatorV3_MergeValTransform(f *testing.F) {
	db, agg := testFuzzDbAndAggregatorv3(f, 10)
	rwTx, err := db.BeginRwNosync(context.Background())
	require.NoError(f, err)
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()
	ac := agg.BeginFilesRo()
	defer ac.Close()
	domains, err := NewSharedDomains(WrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(f, err)
	defer domains.Close()

	const txs = uint64(1000)

	agg.commitmentValuesTransform = true

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
			domains.SetTxNum(txNum)

			buf := types.EncodeAccountBytesV3(1, uint256.NewInt(txNum*1e6), nil, 0)
			err = domains.DomainPut(kv.AccountsDomain, addrs[txNum].Bytes(), nil, buf, nil, 0)
			require.NoError(t, err)

			err = domains.DomainPut(kv.StorageDomain, addrs[txNum].Bytes(), locs[txNum].Bytes(), []byte{addrs[txNum].Bytes()[0], locs[txNum].Bytes()[0]}, nil, 0)
			require.NoError(t, err)

			if (txNum+1)%agg.StepSize() == 0 {
				_, err := domains.ComputeCommitment(context.Background(), true, txNum/10, "")
				require.NoError(t, err)
			}

			state[string(addrs[txNum].Bytes())] = buf
			state[string(addrs[txNum].Bytes())+string(locs[txNum].Bytes())] = []byte{addrs[txNum].Bytes()[0], locs[txNum].Bytes()[0]}
		}

		err = domains.Flush(context.Background(), rwTx)
		require.NoError(t, err)

		err = rwTx.Commit()
		require.NoError(t, err)
		rwTx = nil

		err = agg.BuildFiles(txs)
		require.NoError(t, err)

		ac.Close()
		ac = agg.BeginFilesRo()
		defer ac.Close()

		rwTx, err = db.BeginRwNosync(context.Background())
		require.NoError(t, err)
		defer func() {
			if rwTx != nil {
				rwTx.Rollback()
			}
		}()

		logEvery := time.NewTicker(30 * time.Second)
		defer logEvery.Stop()
		stat, err := ac.Prune(context.Background(), rwTx, 0, logEvery)
		require.NoError(t, err)
		t.Logf("Prune: %s", stat)

		err = rwTx.Commit()
		require.NoError(t, err)

		err = agg.MergeLoop(context.Background())
		require.NoError(t, err)
	})
}

func testFuzzDbAndAggregatorv3(f *testing.F, aggStep uint64) (kv.RwDB, *Aggregator) {
	f.Helper()
	require := require.New(f)
	dirs := datadir.New(f.TempDir())
	logger := log.New()
	db := mdbx.NewMDBX(logger).InMem(dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).MustOpen()
	f.Cleanup(db.Close)

	agg, err := NewAggregator(context.Background(), dirs, aggStep, db, logger)
	require.NoError(err)
	f.Cleanup(agg.Close)
	err = agg.OpenFolder()
	require.NoError(err)
	agg.DisableFsync()
	return db, agg
}
