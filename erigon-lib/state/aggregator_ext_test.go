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
	"encoding/hex"
	"fmt"
	randOld "math/rand"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/types/accounts"
)

type rndGen struct {
	*rand.Rand
	oldGen *randOld.Rand
}

func newRnd(seed uint64) *rndGen {
	return &rndGen{
		Rand:   rand.New(rand.NewChaCha8([32]byte{byte(seed)})),
		oldGen: randOld.New(randOld.NewSource(int64(seed))),
	}
}
func (r *rndGen) IntN(n int) int                   { return int(r.Uint64N(uint64(n))) }
func (r *rndGen) Read(p []byte) (n int, err error) { return r.oldGen.Read(p) } // seems `go1.22` doesn't have `Read` method on `math/v2` generator

func TestAggregatorV3_Merge(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()
	db, agg := testDbAndAggregatorv3(t, 10)
	rwTx, err := db.BeginRwNosync(context.Background())
	require.NoError(t, err)
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()

	ac := agg.BeginFilesRo()
	defer ac.Close()
	domains, err := state.NewSharedDomains(wrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()

	txs := uint64(1000)
	rnd := randOld.New(randOld.NewSource(time.Now().UnixNano()))

	var (
		commKey1 = []byte("someCommKey")
		commKey2 = []byte("otherCommKey")
	)

	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	var maxWrite, otherMaxWrite uint64
	for txNum := uint64(1); txNum <= txs; txNum++ {
		domains.SetTxNum(txNum)

		addr, loc := make([]byte, length.Addr), make([]byte, length.Hash)

		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.Equal(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.Equal(t, length.Hash, n)
		acc := accounts.Account{
			Nonce:       1,
			Balance:     *uint256.NewInt(0),
			CodeHash:    common.Hash{},
			Incarnation: 0,
		}
		buf := accounts.SerialiseV3(&acc)
		err = domains.DomainPut(kv.AccountsDomain, addr, nil, buf, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.StorageDomain, addr, loc, []byte{addr[0], loc[0]}, nil, 0)
		require.NoError(t, err)

		var v [8]byte
		binary.BigEndian.PutUint64(v[:], txNum)
		if txNum%135 == 0 {
			pv, step, err := domains.GetLatest(kv.CommitmentDomain, commKey2)
			require.NoError(t, err)

			err = domains.DomainPut(kv.CommitmentDomain, commKey2, nil, v[:], pv, step)
			require.NoError(t, err)
			otherMaxWrite = txNum
		} else {
			pv, step, err := domains.GetLatest(kv.CommitmentDomain, commKey1)
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
	_, err = ac.PruneSmallBatches(context.Background(), time.Hour, rwTx)
	require.NoError(t, err)

	err = rwTx.Commit()
	require.NoError(t, err)

	err = agg.MergeLoop(context.Background())
	require.NoError(t, err)

	// Check the history
	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	dc := agg.BeginFilesRo()

	v, _, ex, err := dc.GetLatest(kv.CommitmentDomain, commKey1, roTx)
	require.NoError(t, err)
	require.Truef(t, ex, "key %x not found", commKey1)

	require.Equal(t, maxWrite, binary.BigEndian.Uint64(v[:]))

	v, _, ex, err = dc.GetLatest(kv.CommitmentDomain, commKey2, roTx)
	require.NoError(t, err)
	require.Truef(t, ex, "key %x not found", commKey2)
	dc.Close()

	require.Equal(t, otherMaxWrite, binary.BigEndian.Uint64(v[:]))
}

func TestAggregatorV3_RestartOnDatadir(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()

	t.Run("BPlus", func(t *testing.T) {
		rc := runCfg{
			aggStep:  50,
			useBplus: true,
		}
		aggregatorV3_RestartOnDatadir(t, rc)
	})
	t.Run("B", func(t *testing.T) {
		rc := runCfg{
			aggStep: 50,
		}
		aggregatorV3_RestartOnDatadir(t, rc)
	})

}

type runCfg struct {
	aggStep      uint64
	useBplus     bool
	compressVals bool
	largeVals    bool
}

// here we create a bunch of updates for further aggregation.
// FinishTx should merge underlying files several times
// Expected that:
// - we could close first aggregator and open another with previous data still available
// - new aggregator SeekCommitment must return txNum equal to amount of total txns
func aggregatorV3_RestartOnDatadir(t *testing.T, rc runCfg) {
	t.Helper()
	ctx := context.Background()
	logger := log.New()
	aggStep := rc.aggStep
	db, agg := testDbAndAggregatorv3(t, aggStep)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	ac := agg.BeginFilesRo()
	defer ac.Close()

	domains, err := state.NewSharedDomains(wrapTxWithCtx(tx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()

	var latestCommitTxNum uint64
	rnd := newRnd(0)

	someKey := []byte("somekey")
	txs := (aggStep / 2) * 19
	t.Logf("step=%d tx_count=%d", aggStep, txs)
	var aux [8]byte
	// keys are encodings of numbers 1..31
	// each key changes value on every txNum which is multiple of the key
	var maxWrite uint64
	addr, loc := make([]byte, length.Addr), make([]byte, length.Hash)
	for txNum := uint64(1); txNum <= txs; txNum++ {
		domains.SetTxNum(txNum)
		binary.BigEndian.PutUint64(aux[:], txNum)

		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.Equal(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.Equal(t, length.Hash, n)
		//keys[txNum-1] = append(addr, loc...)
		acc := accounts.Account{
			Nonce:       1,
			Balance:     *uint256.NewInt(rnd.Uint64()),
			CodeHash:    common.Hash{},
			Incarnation: 0,
		}
		buf := accounts.SerialiseV3(&acc)
		err = domains.DomainPut(kv.AccountsDomain, addr, nil, buf, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.StorageDomain, addr, loc, []byte{addr[0], loc[0]}, nil, 0)
		require.NoError(t, err)

		err = domains.DomainPut(kv.CommitmentDomain, someKey, nil, aux[:], nil, 0)
		require.NoError(t, err)
		maxWrite = txNum
	}
	_, err = domains.ComputeCommitment(ctx, true, domains.BlockNum(), "")
	require.NoError(t, err)

	err = domains.Flush(context.Background(), tx)
	require.NoError(t, err)
	err = tx.Commit()
	require.NoError(t, err)
	tx = nil

	err = agg.BuildFiles(txs)
	require.NoError(t, err)

	agg.Close()

	// Start another aggregator on same datadir
	anotherAgg, err := state.NewAggregator(context.Background(), agg.Dirs(), aggStep, db, logger)
	require.NoError(t, err)
	defer anotherAgg.Close()

	require.NoError(t, anotherAgg.OpenFolder())

	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()

	//anotherAgg.SetTx(rwTx)
	startTx := anotherAgg.EndTxNumMinimax()
	ac2 := anotherAgg.BeginFilesRo()
	defer ac2.Close()
	dom2, err := state.NewSharedDomains(wrapTxWithCtx(rwTx, ac2), log.New())
	require.NoError(t, err)
	defer dom2.Close()

	_, err = dom2.SeekCommitment(ctx, wrapTxWithCtx(rwTx, ac2))
	sstartTx := dom2.TxNum()

	require.NoError(t, err)
	require.GreaterOrEqual(t, sstartTx, startTx)
	require.GreaterOrEqual(t, sstartTx, latestCommitTxNum)
	_ = sstartTx
	rwTx.Rollback()
	rwTx = nil

	// Check the history
	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	dc := anotherAgg.BeginFilesRo()
	v, _, ex, err := dc.GetLatest(kv.CommitmentDomain, someKey, roTx)
	require.NoError(t, err)
	require.True(t, ex)
	dc.Close()

	require.Equal(t, maxWrite, binary.BigEndian.Uint64(v[:]))
}

func TestAggregatorV3_ReplaceCommittedKeys(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	t.Parallel()
	ctx := context.Background()
	aggStep := uint64(500)

	db, agg := testDbAndAggregatorv3(t, aggStep)

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()

	ac := agg.BeginFilesRo()
	defer ac.Close()
	domains, err := state.NewSharedDomains(wrapTxWithCtx(tx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()

	var latestCommitTxNum uint64
	commit := func(txn uint64) error {
		err = domains.Flush(ctx, tx)
		require.NoError(t, err)
		ac.Close()
		err = tx.Commit()
		require.NoError(t, err)

		tx, err = db.BeginRw(context.Background())
		require.NoError(t, err)
		ac = agg.BeginFilesRo()
		domains, err = state.NewSharedDomains(wrapTxWithCtx(tx, ac), log.New())
		require.NoError(t, err)
		atomic.StoreUint64(&latestCommitTxNum, txn)
		return nil
	}

	txs := (aggStep) * config3.StepsInFrozenFile
	t.Logf("step=%d tx_count=%d", aggStep, txs)

	rnd := newRnd(0)
	keys := make([][]byte, txs/2)

	var prev1, prev2 []byte
	var txNum uint64
	for txNum = uint64(1); txNum <= txs/2; txNum++ {
		domains.SetTxNum(txNum)

		addr, loc := make([]byte, length.Addr), make([]byte, length.Hash)
		n, err := rnd.Read(addr)
		require.NoError(t, err)
		require.Equal(t, length.Addr, n)

		n, err = rnd.Read(loc)
		require.NoError(t, err)
		require.Equal(t, length.Hash, n)
		keys[txNum-1] = append(addr, loc...)

		acc := accounts.Account{
			Nonce:       1,
			Balance:     *uint256.NewInt(0),
			CodeHash:    common.Hash{},
			Incarnation: 0,
		}
		buf := accounts.SerialiseV3(&acc)

		err = domains.DomainPut(kv.AccountsDomain, addr, nil, buf, prev1, 0)
		require.NoError(t, err)
		prev1 = buf

		err = domains.DomainPut(kv.StorageDomain, addr, loc, []byte{addr[0], loc[0]}, prev2, 0)
		require.NoError(t, err)
		prev2 = []byte{addr[0], loc[0]}

	}
	require.NoError(t, commit(txNum))

	half := txs / 2
	for txNum = txNum + 1; txNum <= txs; txNum++ {
		domains.SetTxNum(txNum)

		addr, loc := keys[txNum-1-half][:length.Addr], keys[txNum-1-half][length.Addr:]

		prev, step, _, err := ac.GetLatest(kv.StorageDomain, keys[txNum-1-half], tx)
		require.NoError(t, err)
		err = domains.DomainPut(kv.StorageDomain, addr, loc, []byte{addr[0], loc[0]}, prev, step)
		require.NoError(t, err)
	}

	ac.Close()
	err = tx.Commit()
	tx = nil

	tx, err = db.BeginRw(context.Background())
	require.NoError(t, err)

	aggCtx2 := agg.BeginFilesRo()
	defer aggCtx2.Close()

	for i, key := range keys {
		storedV, _, found, err := aggCtx2.GetLatest(kv.StorageDomain, key, tx)
		require.Truef(t, found, "key %x not found %d", key, i)
		require.NoError(t, err)
		require.Equal(t, key[0], storedV[0])
		require.Equal(t, key[length.Addr], storedV[1])
	}
	require.NoError(t, err)
}

func testDbAndAggregatorv3(tb testing.TB, aggStep uint64) (kv.RwDB, *state.Aggregator) {
	tb.Helper()
	require, logger := require.New(tb), log.New()
	dirs := datadir.New(tb.TempDir())
	db := mdbx.New(kv.ChainDB, logger).InMem(dirs.Chaindata).GrowthStep(32 * datasize.MB).MapSize(2 * datasize.GB).MustOpen()
	tb.Cleanup(db.Close)

	agg, err := state.NewAggregator(context.Background(), dirs, aggStep, db, logger)
	require.NoError(err)
	tb.Cleanup(agg.Close)
	err = agg.OpenFolder()
	require.NoError(err)
	agg.DisableFsync()
	return db, agg
}

// generate test data for table tests, containing n; n < 20 keys of length 20 bytes and values of length <= 16 bytes
func generateInputData(tb testing.TB, keySize, valueSize, keyCount int) ([][]byte, [][]byte) {
	tb.Helper()

	rnd := newRnd(0)
	values := make([][]byte, keyCount)
	keys := make([][]byte, keyCount)

	bk, bv := make([]byte, keySize), make([]byte, valueSize)
	for i := 0; i < keyCount; i++ {
		n, err := rnd.Read(bk[:])
		require.Equal(tb, keySize, n)
		require.NoError(tb, err)
		keys[i] = common.Copy(bk[:n])

		n, err = rnd.Read(bv[:rnd.IntN(valueSize)+1])
		require.NoError(tb, err)

		values[i] = common.Copy(bv[:n])
	}
	return keys, values
}

func TestAggregatorV3_SharedDomains(t *testing.T) {
	t.Parallel()
	db, agg := testDbAndAggregatorv3(t, 20)
	ctx := context.Background()

	ac := agg.BeginFilesRo()
	defer ac.Close()

	rwTx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := state.NewSharedDomains(wrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()
	changesetAt5 := &state.StateChangeSet{}
	changesetAt3 := &state.StateChangeSet{}

	keys, vals := generateInputData(t, 20, 16, 10)
	keys = keys[:2]

	var i int
	roots := make([][]byte, 0, 10)
	var pruneFrom uint64 = 5

	mc := agg.BeginFilesRo()
	defer mc.Close()

	for i = 0; i < len(vals); i++ {
		domains.SetTxNum(uint64(i))
		if i == 3 {
			domains.SetChangesetAccumulator(changesetAt3)
		}
		if i == 5 {
			domains.SetChangesetAccumulator(changesetAt5)
		}

		for j := 0; j < len(keys); j++ {
			acc := accounts.Account{
				Nonce:       uint64(i),
				Balance:     *uint256.NewInt(uint64(i * 100_000)),
				CodeHash:    common.Hash{},
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			prev, step, err := domains.GetLatest(kv.AccountsDomain, keys[j])
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, keys[j], nil, buf, prev, step)
			//err = domains.UpdateAccountCode(keys[j], vals[i], nil)
			require.NoError(t, err)
		}
		rh, err := domains.ComputeCommitment(ctx, true, domains.BlockNum(), "")
		require.NoError(t, err)
		require.NotEmpty(t, rh)
		roots = append(roots, rh)
	}

	err = domains.Flush(context.Background(), rwTx)
	require.NoError(t, err)
	ac.Close()

	ac = agg.BeginFilesRo()
	defer ac.Close()
	domains, err = state.NewSharedDomains(wrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()
	diffs := [kv.DomainLen][]kv.DomainEntryDiff{}
	for idx := range changesetAt5.Diffs {
		diffs[idx] = changesetAt5.Diffs[idx].GetDiffSet()
	}
	err = domains.Unwind(context.Background(), wrapTxWithCtx(rwTx, ac), 0, pruneFrom, &diffs)
	require.NoError(t, err)

	domains.SetChangesetAccumulator(changesetAt3)
	for i = int(pruneFrom); i < len(vals); i++ {
		domains.SetTxNum(uint64(i))

		for j := 0; j < len(keys); j++ {
			acc := accounts.Account{
				Nonce:       uint64(i),
				Balance:     *uint256.NewInt(uint64(i * 100_000)),
				CodeHash:    common.Hash{},
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			prev, step, _, err := mc.GetLatest(kv.AccountsDomain, keys[j], rwTx)
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, keys[j], nil, buf, prev, step)
			require.NoError(t, err)
			//err = domains.UpdateAccountCode(keys[j], vals[i], nil)
			//require.NoError(t, err)
		}

		rh, err := domains.ComputeCommitment(ctx, true, domains.BlockNum(), "")
		require.NoError(t, err)
		require.NotEmpty(t, rh)
		require.Equal(t, roots[i], rh)
	}

	err = domains.Flush(context.Background(), rwTx)
	require.NoError(t, err)
	ac.Close()

	pruneFrom = 3

	ac = agg.BeginFilesRo()
	defer ac.Close()
	domains, err = state.NewSharedDomains(wrapTxWithCtx(rwTx, ac), log.New())
	require.NoError(t, err)
	defer domains.Close()
	for idx := range changesetAt3.Diffs {
		diffs[idx] = changesetAt3.Diffs[idx].GetDiffSet()
	}
	err = domains.Unwind(context.Background(), wrapTxWithCtx(rwTx, ac), 0, pruneFrom, &diffs)
	require.NoError(t, err)

	for i = int(pruneFrom); i < len(vals); i++ {
		domains.SetTxNum(uint64(i))

		for j := 0; j < len(keys); j++ {
			acc := accounts.Account{
				Nonce:       uint64(i),
				Balance:     *uint256.NewInt(uint64(i * 100_000)),
				CodeHash:    common.Hash{},
				Incarnation: 0,
			}
			buf := accounts.SerialiseV3(&acc)
			prev, step, _, err := mc.GetLatest(kv.AccountsDomain, keys[j], rwTx)
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, keys[j], nil, buf, prev, step)
			require.NoError(t, err)
			//err = domains.UpdateAccountCode(keys[j], vals[i], nil)
			//require.NoError(t, err)
		}

		rh, err := domains.ComputeCommitment(ctx, true, domains.BlockNum(), "")
		require.NoError(t, err)
		require.NotEmpty(t, rh)
		require.Equal(t, roots[i], rh)
	}
}

// also useful to decode given input into v3 account
func Test_helper_decodeAccountv3Bytes(t *testing.T) {
	t.Parallel()
	input, err := hex.DecodeString("000114000101")
	require.NoError(t, err)

	acc := accounts.Account{}
	_ = accounts.DeserialiseV3(&acc, input)
	fmt.Printf("input %x nonce %d balance %d codeHash %d\n", input, acc.Nonce, acc.Balance.Uint64(), acc.CodeHash.Bytes())
}

// wrapTxWithCtx - deprecated copy of kv_temporal.go - visible only in tests
// need to move non-unit-tests to own package
func wrapTxWithCtx(tx kv.Tx, aggTx *state.AggregatorRoTx) *state.Tx {
	return &state.Tx{MdbxTx: tx.(*mdbx.MdbxTx), Aggtx: aggTx}
}
