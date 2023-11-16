package state

import (
	"context"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/types"
)

func TestSharedDomain_SeekCommitment(t *testing.T) {
	stepSize := uint64(100)
	db, agg := testDbAndAggregatorv3(t, stepSize)

	ctx := context.Background()
	rwTx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	ac := agg.MakeContext()
	defer ac.Close()

	domains := NewSharedDomains(WrapTxWithCtx(rwTx, ac))
	defer domains.Close()

	maxTx := stepSize
	hashes := make([][]byte, maxTx)
	rnd := rand.New(rand.NewSource(0))
	ac.Close()
	err = rwTx.Commit()
	require.NoError(t, err)

	rwTx, err = db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	ac = agg.MakeContext()
	defer ac.Close()
	domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac))
	defer domains.Close()

	k0 := make([]byte, length.Addr)
	commitStep := 3
	keys := make([][]byte, 256)

	for k := 0; k < 256; k++ {
		kk := make([]byte, length.Addr)
		rnd.Read(kk)

		keys[k] = kk
	}

	i := 0
	for ; i < int(maxTx); i++ {
		domains.SetBlockNum(uint64(i / commitStep))
		domains.SetTxNum(ctx, uint64(i))
		for accs := 0; accs < 256; accs++ {
			v := types.EncodeAccountBytesV3(uint64(i), uint256.NewInt(uint64(i*10e6)+uint64(accs*10e2)), nil, 0)
			k0[0] = byte(accs)
			pv, err := domains.LatestAccount(k0)
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, k0, nil, v, pv)
			require.NoError(t, err)
		}

		if i%commitStep == 0 {
			rh, err := domains.ComputeCommitment(ctx, true, false, domains.BlockNum())
			require.NoError(t, err)
			if hashes[uint64(i)] != nil {
				require.Equal(t, hashes[uint64(i)], rh)
			}
			require.NotNil(t, rh)
			hashes[uint64(i)] = rh
		}
	}

	err = domains.Flush(ctx, rwTx)
	require.NoError(t, err)

	err = rwTx.Commit()
	require.NoError(t, err)

	latestState, err := domains.Commitment.PatriciaState()
	latestBlock := domains.BlockNum()
	latestTxNum := domains.TxNum()
	require.NoError(t, err)

	domains.FinishWrites()
	domains.Close()
	ac.Close()

	rwTx, err = db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	acu := agg.MakeContext()
	defer acu.Close()

	otherDomains := NewSharedDomains(WrapTxWithCtx(rwTx, acu))
	defer otherDomains.Close()
	restoredState, err := otherDomains.Commitment.PatriciaState()
	require.NoError(t, err)
	require.EqualValues(t, latestState, restoredState)
	require.EqualValues(t, latestBlock, otherDomains.BlockNum())
	require.EqualValues(t, latestTxNum, otherDomains.TxNum())

	offt, err := otherDomains.SeekCommitment(ctx, rwTx)
	require.NoError(t, err)
	require.EqualValues(t, latestState, restoredState)
	require.EqualValues(t, latestBlock, otherDomains.BlockNum())
	require.EqualValues(t, latestTxNum, otherDomains.TxNum())

	_ = offt
}

func TestSharedDomain_Unwind(t *testing.T) {
	stepSize := uint64(100)
	db, agg := testDbAndAggregatorv3(t, stepSize)

	ctx := context.Background()
	rwTx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	ac := agg.MakeContext()
	defer ac.Close()

	domains := NewSharedDomains(WrapTxWithCtx(rwTx, ac))
	defer domains.Close()

	maxTx := stepSize
	hashes := make([][]byte, maxTx)
	count := 10
	rnd := rand.New(rand.NewSource(0))
	ac.Close()
	err = rwTx.Commit()
	require.NoError(t, err)

Loop:
	rwTx, err = db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	ac = agg.MakeContext()
	defer ac.Close()
	domains = NewSharedDomains(WrapTxWithCtx(rwTx, ac))
	defer domains.Close()

	i := 0
	k0 := make([]byte, length.Addr)
	commitStep := 3

	for ; i < int(maxTx); i++ {
		domains.SetTxNum(ctx, uint64(i))
		for accs := 0; accs < 256; accs++ {
			v := types.EncodeAccountBytesV3(uint64(i), uint256.NewInt(uint64(i*10e6)+uint64(accs*10e2)), nil, 0)
			k0[0] = byte(accs)
			pv, err := domains.LatestAccount(k0)
			require.NoError(t, err)

			err = domains.DomainPut(kv.AccountsDomain, k0, nil, v, pv)
			require.NoError(t, err)
		}

		if i%commitStep == 0 {
			rh, err := domains.ComputeCommitment(ctx, true, false, domains.BlockNum())
			require.NoError(t, err)
			if hashes[uint64(i)] != nil {
				require.Equal(t, hashes[uint64(i)], rh)
			}
			require.NotNil(t, rh)
			hashes[uint64(i)] = rh
			domains.SetBlockNum(uint64(i / commitStep))
		}
	}

	err = domains.Flush(ctx, rwTx)
	require.NoError(t, err)

	unwindTo := uint64(commitStep * rnd.Intn(int(maxTx)/commitStep))

	acu := agg.MakeContext()
	err = domains.Unwind(ctx, rwTx, unwindTo)
	require.NoError(t, err)
	acu.Close()

	err = rwTx.Commit()
	require.NoError(t, err)
	if count > 0 {
		count--
	}
	domains.FinishWrites()
	domains.Close()
	ac.Close()
	if count == 0 {
		return
	}

	goto Loop
}
