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

package test

import (
	"context"
	"encoding/binary"
	"fmt"
	"io/fs"
	"math/rand"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	state2 "github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state"
	reset2 "github.com/erigontech/erigon/eth/rawdbreset"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// if fpath is empty, tempDir is used, otherwise fpath is reused
func testDbAndAggregatorv3(t *testing.T, fpath string, aggStep uint64) (kv.TemporalRwDB, *state.Aggregator, string) {
	t.Helper()

	path := t.TempDir()
	if fpath != "" {
		path = fpath
	}
	dirs := datadir.New(path)
	db := temporaltest.NewTestDBWithStepSize(t, dirs, aggStep)
	return db, db.(state.HasAgg).Agg().(*state.Aggregator), path
}

func Test_AggregatorV3_RestartOnDatadir_WithoutDB(t *testing.T) {
	t.Skip("fix me!")
	// generate some updates on domains.
	// record all roothashes on those updates after some POINT which will be stored in db and never fall to files
	// remove db
	// start aggregator on datadir
	// evaluate commitment after restart
	// continue from  POINT and compare hashes when `block` ends

	aggStep := uint64(100)
	blockSize := uint64(10) // lets say that each block contains 10 tx, after each block we do commitment
	ctx := context.Background()

	db, agg, datadir := testDbAndAggregatorv3(t, "", aggStep)
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	domains, err := state.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	blockNum, txNum := uint64(0), uint64(0)
	domains.SetTxNum(txNum)

	rnd := rand.New(rand.NewSource(time.Now().Unix()))

	var (
		aux     [8]byte
		loc     = common.Hash{}
		maxStep = uint64(20)
		txs     = aggStep*maxStep + aggStep/2 // we do 20.5 steps, 1.5 left in db.

		// list of hashes and txNum when i'th block was committed
		hashedTxs = make([]uint64, 0)
		hashes    = make([][]byte, 0)

		// list of inserted accounts and storage locations
		addrs = make([]common.Address, 0)
		accs  = make([]*accounts.Account, 0)
		locs  = make([]common.Hash, 0)

		writer = state2.NewWriter(domains.AsPutDel(tx), nil, txNum)
	)

	for i := uint64(1); i <= txs; i++ {
		txNum = i
		blockNum = txNum / blockSize
		writer.SetTxNum(txNum)
		domains.SetTxNum(txNum)
		domains.SetBlockNum(txNum / blockSize)
		binary.BigEndian.PutUint64(aux[:], txNum)

		n, err := rnd.Read(loc[:])
		require.NoError(t, err)
		require.Equal(t, length.Hash, n)

		acc, addr := randomAccount(t)
		interesting := txNum/aggStep > maxStep-1
		if interesting { // one and half step will be left in db
			addrs = append(addrs, addr)
			accs = append(accs, acc)
			locs = append(locs, loc)
		}

		err = writer.UpdateAccountData(addr, &accounts.Account{}, acc)
		//buf := EncodeAccountBytes(1, uint256.NewInt(rnd.Uint64()), nil, 0)
		//err = domains.UpdateAccountData(addr, buf, nil)
		require.NoError(t, err)

		err = writer.WriteAccountStorage(addr, 0, loc, uint256.Int{}, *uint256.NewInt(txNum))
		//err = domains.WriteAccountStorage(addr, loc, sbuf, nil)
		require.NoError(t, err)
		if txNum%blockSize == 0 {
			err = rawdbv3.TxNums.Append(tx, blockNum, txNum)
			require.NoError(t, err)
		}

		if txNum%blockSize == 0 && interesting {
			rh, err := domains.ComputeCommitment(ctx, true, blockNum, txNum, "")
			require.NoError(t, err)
			fmt.Printf("tx %d bn %d rh %x\n", txNum, txNum/blockSize, rh)

			hashes = append(hashes, rh)
			hashedTxs = append(hashedTxs, txNum) //nolint
		}
	}

	rh, err := domains.ComputeCommitment(ctx, true, blockNum, txNum, "")
	require.NoError(t, err)
	t.Logf("executed tx %d root %x datadir %q\n", txs, rh, datadir)

	err = domains.Flush(ctx, tx)
	require.NoError(t, err)

	//COMS := make(map[string][]byte)
	//{
	//	cct := domains.Commitment.BeginFilesRo()
	//	err = cct.IteratePrefix(tx, []byte("state"), func(k, v []byte) {
	//		COMS[string(k)] = v
	//		//fmt.Printf("k %x v %x\n", k, v)
	//	})
	//	cct.Close()
	//}

	err = tx.Commit()
	require.NoError(t, err)

	err = agg.BuildFiles(txs)
	require.NoError(t, err)

	domains.Close()
	agg.Close()
	db.Close()

	// ======== delete DB, reset domains ========
	ffs := os.DirFS(datadir)
	dirs, err := fs.ReadDir(ffs, ".")
	require.NoError(t, err)
	for _, d := range dirs {
		if strings.HasPrefix(d.Name(), "db") {
			err = dir.RemoveAll(path.Join(datadir, d.Name()))
			t.Logf("remove DB %q err %v", d.Name(), err)
			require.NoError(t, err)
			break
		}
	}

	db, _, _ = testDbAndAggregatorv3(t, datadir, aggStep)

	tx, err = db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	domains, err = state.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	//{
	//	cct := domains.Commitment.BeginFilesRo()
	//	err = cct.IteratePrefix(tx, []byte("state"), func(k, v []byte) {
	//		cv, _ := COMS[string(k)]
	//		if !bytes.Equal(cv, v) {
	//			ftx, fb := binary.BigEndian.Uint64(cv[0:8]), binary.BigEndian.Uint64(cv[8:16])
	//			ntx, nb := binary.BigEndian.Uint64(v[0:8]), binary.BigEndian.Uint64(v[8:16])
	//			fmt.Printf("before rm DB tx %d block %d len %d\n", ftx, fb, len(cv))
	//			fmt.Printf("after  rm DB tx %d block %d len %d\n", ntx, nb, len(v))
	//		}
	//	})
	//	cct.Close()
	//}

	err = domains.SeekCommitment(ctx, tx)
	require.NoError(t, err)
	tx.Rollback()

	domains.Close()

	err = reset2.ResetExec(ctx, db)
	require.NoError(t, err)
	// ======== reset domains end ========

	tx, err = db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err = state.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	writer = state2.NewWriter(domains.AsPutDel(tx), nil, txNum)

	txToStart := domains.TxNum()

	rh, err = domains.ComputeCommitment(ctx, false, blockNum, txNum, "")
	require.NoError(t, err)
	t.Logf("restart hash %x\n", rh)

	var i, j int
	for tt := txToStart; tt <= txs; tt++ {
		txNum = tt
		blockNum = txNum / blockSize
		domains.SetTxNum(txNum)
		domains.SetBlockNum(blockNum)
		binary.BigEndian.PutUint64(aux[:], txNum)

		//fmt.Printf("tx+ %d addr %x\n", txNum, addrs[i])
		err = writer.UpdateAccountData(addrs[i], &accounts.Account{}, accs[i])
		require.NoError(t, err)

		err = writer.WriteAccountStorage(addrs[i], 0, locs[i], uint256.Int{}, *uint256.NewInt(txNum))
		require.NoError(t, err)
		i++

		if txNum%blockSize == 0 /*&& txNum >= txs-aggStep */ {
			rh, err := domains.ComputeCommitment(ctx, true, blockNum, txNum, "")
			require.NoError(t, err)
			fmt.Printf("tx %d rh %x\n", txNum, rh)
			require.Equal(t, hashes[j], rh)
			j++
		}
	}
}

func Test_AggregatorV3_RestartOnDatadir_WithoutAnything(t *testing.T) {
	t.Skip("fix me: seems i don't clean all my files")
	// generate some updates on domains.
	// record all roothashes on those updates after some POINT which will be stored in db and never fall to files
	// remove whole datadir
	// start aggregator on datadir
	// evaluate commitment after restart
	// restart from beginning and compare hashes when `block` ends

	aggStep := uint64(100)
	blockSize := uint64(10) // lets say that each block contains 10 tx, after each block we do commitment
	maxStep := uint64(20)
	txs := aggStep*maxStep + aggStep/2 // we do 20.5 steps, 1.5 left in db.

	var (
		aux [8]byte
		loc = common.Hash{}
		// list of inserted accounts and storage locations
		addrs = make([]common.Address, 0)
		accs  = make([]*accounts.Account, 0)
		locs  = make([]common.Hash, 0)

		// list of hashes and txNum when i'th block was committed
		hashedTxs = make([]uint64, 0)
		hashes    = make([][]byte, 0)
	)

	ctx := context.Background()

	db, agg, datadir := testDbAndAggregatorv3(t, "", aggStep)
	blockNum, txNum := uint64(0), uint64(0)
	testStartedFromTxNum := uint64(1)

	t.Run("gen_data", func(t *testing.T) {
		tx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer tx.Rollback()

		domains, err := state.NewSharedDomains(tx, log.New())
		require.NoError(t, err)
		defer domains.Close()
		rnd := rand.New(rand.NewSource(time.Now().Unix()))

		domains.SetTxNum(txNum)
		writer := state2.NewWriter(domains.AsPutDel(tx), nil, txNum)

		for i := testStartedFromTxNum; i <= txs; i++ {
			txNum = i
			blockNum = txNum / blockSize
			domains.SetTxNum(txNum)
			binary.BigEndian.PutUint64(aux[:], txNum)

			n, err := rnd.Read(loc[:])
			require.NoError(t, err)
			require.Equal(t, length.Hash, n)

			acc, addr := randomAccount(t)
			addrs = append(addrs, addr)
			accs = append(accs, acc)
			locs = append(locs, loc)

			err = writer.UpdateAccountData(addr, &accounts.Account{}, acc)
			require.NoError(t, err)

			err = writer.WriteAccountStorage(addr, 0, loc, uint256.Int{}, *uint256.NewInt(txNum))
			require.NoError(t, err)

			if txNum%blockSize == 0 {
				rh, err := domains.ComputeCommitment(ctx, true, blockNum, txNum, "")
				require.NoError(t, err)

				hashes = append(hashes, rh)
				hashedTxs = append(hashedTxs, txNum) //nolint
				err = rawdbv3.TxNums.Append(tx, blockNum, txNum)
				require.NoError(t, err)
			}
		}

		latestHash, err := domains.ComputeCommitment(ctx, true, blockNum, txNum, "")
		require.NoError(t, err)
		_ = latestHash
		//require.EqualValues(t, params.MainnetGenesisHash, common.Hash(latestHash))
		//t.Logf("executed tx %d root %x datadir %q\n", txs, latestHash, datadir)

		err = domains.Flush(ctx, tx)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		err = agg.BuildFiles(txs)
		require.NoError(t, err)

		domains.Close()
		agg.Close()
		db.Close()
	})

	// ======== delete datadir and restart domains ========
	t.Run("delete_datadir", func(t *testing.T) {
		err := dir.RemoveAll(datadir)
		require.NoError(t, err)
		//t.Logf("datadir has been removed")

		db, _, _ = testDbAndAggregatorv3(t, datadir, aggStep)

		tx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer tx.Rollback()

		domains, err := state.NewSharedDomains(tx, log.New())
		require.NoError(t, err)
		defer domains.Close()

		err = domains.SeekCommitment(ctx, tx)
		tx.Rollback()
		require.NoError(t, err)

		domains.Close()

		err = reset2.ResetExec(ctx, db)
		require.NoError(t, err)
		// ======== reset domains end ========

		tx, err = db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer tx.Rollback()
		domains, err = state.NewSharedDomains(tx, log.New())
		require.NoError(t, err)
		defer domains.Close()

		writer := state2.NewWriter(domains.AsPutDel(tx), nil, txNum)

		txToStart := domains.TxNum()
		require.EqualValues(t, 0, txToStart)
		txToStart = testStartedFromTxNum

		rh, err := domains.ComputeCommitment(ctx, false, blockNum, txNum, "")
		require.NoError(t, err)

		s, err := chainspec.ChainSpecByName(networkname.Test)
		require.NoError(t, err)
		require.Equal(t, s.GenesisStateRoot, common.BytesToHash(rh))
		//require.NotEqualValues(t, latestHash, common.BytesToHash(rh))
		//common.BytesToHash(rh))

		var i, j int
		for tt := txToStart; tt <= txs; tt++ {
			txNum = tt
			blockNum = txNum / blockSize
			domains.SetTxNum(txNum)
			domains.SetBlockNum(blockNum)
			binary.BigEndian.PutUint64(aux[:], txNum)

			err = writer.UpdateAccountData(addrs[i], &accounts.Account{}, accs[i])
			require.NoError(t, err)

			err = writer.WriteAccountStorage(addrs[i], 0, locs[i], uint256.Int{}, *uint256.NewInt(txNum))
			require.NoError(t, err)
			i++

			if txNum%blockSize == 0 {
				rh, err := domains.ComputeCommitment(ctx, true, blockNum, txNum, "")
				require.NoError(t, err)
				//fmt.Printf("tx %d rh %x\n", txNum, rh)
				require.Equal(t, hashes[j], rh)
				j++
			}
		}
	})

}

func randomAccount(t *testing.T) (*accounts.Account, common.Address) {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	acc := accounts.NewAccount()
	acc.Initialised = true
	acc.Balance = *uint256.NewInt(uint64(rand.Int63()))
	addr := crypto.PubkeyToAddress(key.PublicKey)
	return &acc, addr
}

func TestCommit(t *testing.T) {
	aggStep := uint64(100)

	ctx := context.Background()
	db, _, _ := testDbAndAggregatorv3(t, "", aggStep)
	tx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	domains, err := state.NewSharedDomains(tx, log.New())
	require.NoError(t, err)
	defer domains.Close()
	blockNum, txNum := uint64(0), uint64(0)

	acc := accounts.Account{
		Nonce:       0,
		Balance:     *uint256.NewInt(7),
		CodeHash:    common.Hash{},
		Incarnation: 1,
	}
	buf := accounts.SerialiseV3(&acc)

	addr := common.Hex2Bytes("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e")
	loc := common.Hex2Bytes("24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed")

	for i := 1; i < 3; i++ {
		addr[0] = byte(i)

		err = domains.DomainPut(kv.AccountsDomain, tx, addr, buf, txNum, nil, 0)
		require.NoError(t, err)
		loc[0] = byte(i)

		err = domains.DomainPut(kv.StorageDomain, tx, append(common.Copy(addr), loc...), []byte("0401"), txNum, nil, 0)
		require.NoError(t, err)
	}

	domains.SetTrace(false)
	domainsHash, err := domains.ComputeCommitment(ctx, true, blockNum, txNum, "")
	require.NoError(t, err)
	err = domains.Flush(ctx, tx)
	require.NoError(t, err)

	require.Equal(t, common.BytesToHash(common.FromHex("0xfe81cd91357cd915cae7c02b5a4771e903c16b29dec582818076954be3741030")), common.BytesToHash(domainsHash))

}
