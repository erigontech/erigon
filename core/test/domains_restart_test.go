package test

import (
	"context"
	"encoding/binary"
	"fmt"
	"io/fs"
	"math"
	"math/rand"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common/datadir"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/state"
	reset2 "github.com/ledgerwatch/erigon/core/rawdb/rawdbreset"
	state2 "github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/crypto"
)

// if fpath is empty, tempDir is used, otherwise fpath is reused
func testDbAndAggregatorv3(t *testing.T, fpath string, aggStep uint64) (kv.RwDB, *state.AggregatorV3, string) {
	t.Helper()

	path := t.TempDir()
	if fpath != "" {
		path = fpath
	}
	dirs := datadir.New(path)

	logger := log.New()
	db := mdbx.NewMDBX(logger).Path(dirs.Chaindata).WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.ChaindataTablesCfg
	}).MustOpen()
	t.Cleanup(db.Close)

	agg, err := state.NewAggregatorV3(context.Background(), dirs, aggStep, db, logger)
	require.NoError(t, err)
	t.Cleanup(agg.Close)
	err = agg.OpenFolder()
	agg.DisableFsync()
	require.NoError(t, err)

	// v3 setup
	err = db.Update(context.Background(), func(tx kv.RwTx) error {
		return kvcfg.HistoryV3.ForceWrite(tx, true)
	})
	require.NoError(t, err)

	chain := "unknown_testing"
	tdb, err := temporal.New(db, agg, systemcontracts.SystemContractCodeLookup[chain])
	require.NoError(t, err)
	db = tdb
	return db, agg, path
}

func Test_AggregatorV3_RestartOnDatadir_WithoutDB(t *testing.T) {
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
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()

	domCtx := agg.MakeContext()
	defer domCtx.Close()

	domains := agg.SharedDomains(domCtx)
	defer domains.Close()
	domains.SetTx(tx)

	rnd := rand.New(rand.NewSource(time.Now().Unix()))

	var (
		aux     [8]byte
		loc     = libcommon.Hash{}
		maxStep = uint64(20)
		txs     = aggStep*maxStep + aggStep/2 // we do 20.5 steps, 1.5 left in db.

		// list of hashes and txNum when i'th block was committed
		hashedTxs = make([]uint64, 0)
		hashes    = make([][]byte, 0)

		// list of inserted accounts and storage locations
		addrs = make([]libcommon.Address, 0)
		accs  = make([]*accounts.Account, 0)
		locs  = make([]libcommon.Hash, 0)

		writer = state2.NewWriterV4(tx.(*temporal.Tx), domains)
	)

	for txNum := uint64(1); txNum <= txs; txNum++ {
		domains.SetTxNum(txNum)
		domains.SetBlockNum(txNum / blockSize)
		binary.BigEndian.PutUint64(aux[:], txNum)

		n, err := rnd.Read(loc[:])
		require.NoError(t, err)
		require.EqualValues(t, length.Hash, n)

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

		err = writer.WriteAccountStorage(addr, 0, &loc, &uint256.Int{}, uint256.NewInt(txNum))
		//err = domains.WriteAccountStorage(addr, loc, sbuf, nil)
		require.NoError(t, err)
		if txNum%blockSize == 0 {
			err = rawdbv3.TxNums.Append(tx, domains.BlockNum(), domains.TxNum())
			require.NoError(t, err)
		}

		if txNum%blockSize == 0 && interesting {
			rh, err := writer.Commitment(true, false)
			require.NoError(t, err)
			fmt.Printf("tx %d bn %d rh %x\n", txNum, txNum/blockSize, rh)

			hashes = append(hashes, rh)
			hashedTxs = append(hashedTxs, txNum) //nolint
		}
	}

	rh, err := writer.Commitment(true, false)
	require.NoError(t, err)
	t.Logf("executed tx %d root %x datadir %q\n", txs, rh, datadir)

	err = domains.Flush(ctx, tx)
	require.NoError(t, err)

	//COMS := make(map[string][]byte)
	//{
	//	cct := domains.Commitment.MakeContext()
	//	err = cct.IteratePrefix(tx, []byte("state"), func(k, v []byte) {
	//		COMS[string(k)] = v
	//		//fmt.Printf("k %x v %x\n", k, v)
	//	})
	//	cct.Close()
	//}

	err = tx.Commit()
	require.NoError(t, err)
	tx = nil

	err = agg.BuildFiles(txs)
	require.NoError(t, err)

	domains.Close()
	agg.Close()
	db.Close()
	db = nil

	// ======== delete DB, reset domains ========
	ffs := os.DirFS(datadir)
	dirs, err := fs.ReadDir(ffs, ".")
	require.NoError(t, err)
	for _, d := range dirs {
		if strings.HasPrefix(d.Name(), "db") {
			err = os.RemoveAll(path.Join(datadir, d.Name()))
			t.Logf("remove DB %q err %v", d.Name(), err)
			require.NoError(t, err)
			break
		}
	}

	db, agg, _ = testDbAndAggregatorv3(t, datadir, aggStep)

	domCtx = agg.MakeContext()
	domains = agg.SharedDomains(domCtx)

	tx, err = db.BeginRw(ctx)
	require.NoError(t, err)

	//{
	//	cct := domains.Commitment.MakeContext()
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

	_, err = domains.SeekCommitment(0, math.MaxUint64)
	require.NoError(t, err)
	tx.Rollback()

	domCtx.Close()
	domains.Close()

	err = reset2.ResetExec(ctx, db, "", "", domains.BlockNum())
	require.NoError(t, err)
	// ======== reset domains end ========

	domCtx = agg.MakeContext()
	domains = agg.SharedDomains(domCtx)
	defer domCtx.Close()
	defer domains.Close()

	tx, err = db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	domains.SetTx(tx)
	writer = state2.NewWriterV4(tx.(*temporal.Tx), domains)

	_, err = domains.SeekCommitment(0, math.MaxUint64)
	require.NoError(t, err)

	txToStart := domains.TxNum()

	rh, err = writer.Commitment(false, false)
	require.NoError(t, err)
	t.Logf("restart hash %x\n", rh)

	var i, j int
	for txNum := txToStart; txNum <= txs; txNum++ {
		domains.SetTxNum(txNum)
		domains.SetBlockNum(txNum / blockSize)
		binary.BigEndian.PutUint64(aux[:], txNum)

		//fmt.Printf("tx+ %d addr %x\n", txNum, addrs[i])
		err = writer.UpdateAccountData(addrs[i], &accounts.Account{}, accs[i])
		require.NoError(t, err)

		err = writer.WriteAccountStorage(addrs[i], 0, &locs[i], &uint256.Int{}, uint256.NewInt(txNum))
		require.NoError(t, err)
		i++

		if txNum%blockSize == 0 /*&& txNum >= txs-aggStep */ {
			rh, err := writer.Commitment(true, false)
			require.NoError(t, err)
			fmt.Printf("tx %d rh %x\n", txNum, rh)
			require.EqualValues(t, hashes[j], rh)
			j++
		}
	}
}

func Test_AggregatorV3_RestartOnDatadir_WithoutAnything(t *testing.T) {
	// generate some updates on domains.
	// record all roothashes on those updates after some POINT which will be stored in db and never fall to files
	// remove whole datadir
	// start aggregator on datadir
	// evaluate commitment after restart
	// restart from beginning and compare hashes when `block` ends

	aggStep := uint64(100)
	blockSize := uint64(10) // lets say that each block contains 10 tx, after each block we do commitment
	ctx := context.Background()

	db, agg, datadir := testDbAndAggregatorv3(t, "", aggStep)
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()

	domCtx := agg.MakeContext()
	defer domCtx.Close()

	domains := agg.SharedDomains(domCtx)
	defer domains.Close()

	domains.SetTx(tx)

	rnd := rand.New(rand.NewSource(time.Now().Unix()))

	var (
		aux     [8]byte
		loc     = libcommon.Hash{}
		maxStep = uint64(20)
		txs     = aggStep*maxStep + aggStep/2 // we do 20.5 steps, 1.5 left in db.

		// list of hashes and txNum when i'th block was committed
		hashedTxs = make([]uint64, 0)
		hashes    = make([][]byte, 0)

		// list of inserted accounts and storage locations
		addrs = make([]libcommon.Address, 0)
		accs  = make([]*accounts.Account, 0)
		locs  = make([]libcommon.Hash, 0)

		writer = state2.NewWriterV4(tx.(*temporal.Tx), domains)
	)

	testStartedFromTxNum := uint64(1)
	for txNum := testStartedFromTxNum; txNum <= txs; txNum++ {
		domains.SetTxNum(txNum)
		domains.SetBlockNum(txNum / blockSize)
		binary.BigEndian.PutUint64(aux[:], txNum)

		n, err := rnd.Read(loc[:])
		require.NoError(t, err)
		require.EqualValues(t, length.Hash, n)

		acc, addr := randomAccount(t)
		addrs = append(addrs, addr)
		accs = append(accs, acc)
		locs = append(locs, loc)

		err = writer.UpdateAccountData(addr, &accounts.Account{}, acc)
		require.NoError(t, err)

		err = writer.WriteAccountStorage(addr, 0, &loc, &uint256.Int{}, uint256.NewInt(txNum))
		require.NoError(t, err)

		if txNum%blockSize == 0 {
			rh, err := writer.Commitment(true, false)
			require.NoError(t, err)

			hashes = append(hashes, rh)
			hashedTxs = append(hashedTxs, txNum) //nolint
			err = rawdbv3.TxNums.Append(tx, domains.BlockNum(), domains.TxNum())
			require.NoError(t, err)
		}
	}

	latestHash, err := writer.Commitment(true, false)
	require.NoError(t, err)
	t.Logf("executed tx %d root %x datadir %q\n", txs, latestHash, datadir)

	err = domains.Flush(ctx, tx)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)
	tx = nil

	err = agg.BuildFiles(txs)
	require.NoError(t, err)

	domains.Close()
	agg.Close()
	db.Close()
	db = nil

	// ======== delete datadir and restart domains ========
	err = os.RemoveAll(datadir)
	require.NoError(t, err)
	t.Logf("datadir has been removed")

	db, agg, _ = testDbAndAggregatorv3(t, datadir, aggStep)

	domCtx = agg.MakeContext()
	domains = agg.SharedDomains(domCtx)

	tx, err = db.BeginRw(ctx)
	require.NoError(t, err)

	_, err = domains.SeekCommitment(0, math.MaxUint64)
	tx.Rollback()
	require.NoError(t, err)

	domCtx.Close()
	domains.Close()

	err = reset2.ResetExec(ctx, db, "", "", domains.BlockNum())
	require.NoError(t, err)
	// ======== reset domains end ========

	domCtx = agg.MakeContext()
	domains = agg.SharedDomains(domCtx)
	defer domCtx.Close()
	defer domains.Close()

	tx, err = db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	domains.SetTx(tx)
	writer = state2.NewWriterV4(tx.(*temporal.Tx), domains)

	_, err = domains.SeekCommitment(0, math.MaxUint64)
	require.NoError(t, err)

	txToStart := domains.TxNum()
	require.EqualValues(t, txToStart, 0)
	txToStart = testStartedFromTxNum

	rh, err := writer.Commitment(false, false)
	require.NoError(t, err)
	require.EqualValues(t, rh, types.EmptyRootHash)

	var i, j int
	for txNum := txToStart; txNum <= txs; txNum++ {
		domains.SetTxNum(txNum)
		domains.SetBlockNum(txNum / blockSize)
		binary.BigEndian.PutUint64(aux[:], txNum)

		err = writer.UpdateAccountData(addrs[i], &accounts.Account{}, accs[i])
		require.NoError(t, err)

		err = writer.WriteAccountStorage(addrs[i], 0, &locs[i], &uint256.Int{}, uint256.NewInt(txNum))
		require.NoError(t, err)
		i++

		if txNum%blockSize == 0 {
			rh, err := writer.Commitment(true, false)
			require.NoError(t, err)
			//fmt.Printf("tx %d rh %x\n", txNum, rh)
			require.EqualValues(t, hashes[j], rh)
			j++
		}
	}
}

func randomAccount(t *testing.T) (*accounts.Account, libcommon.Address) {
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
