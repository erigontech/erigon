package state

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common/length"
)

func TestSharedDomain_Unwind(t *testing.T) {
	stepSize := uint64(100)
	db, agg := testDbAndAggregatorv3(t, stepSize)

	ctx := context.Background()
	rwTx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	agg.StartWrites()
	defer agg.FinishWrites()

	ac := agg.MakeContext()
	defer ac.Close()
	d := agg.SharedDomains(ac)
	d.SetTx(rwTx)

	maxTx := stepSize
	hashes := make([][]byte, maxTx)
	count := 10
	rnd := rand.New(rand.NewSource(0))
	rwTx.Commit()

Loop:
	rwTx, err = db.BeginRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	d.SetTx(rwTx)

	i := 0
	k0 := make([]byte, length.Addr)
	commitStep := 3

	for ; i < int(maxTx); i++ {
		d.SetTxNum(uint64(i))
		for accs := 0; accs < 256; accs++ {
			v := EncodeAccountBytes(uint64(i), uint256.NewInt(uint64(i*10e6)+uint64(accs*10e2)), nil, 0)
			k0[0] = byte(accs)
			pv, err := d.LatestAccount(k0)

			err = d.UpdateAccountData(k0, v, pv)
			require.NoError(t, err)
		}

		if i%commitStep == 0 {
			rh, err := d.Commit(true, false)
			require.NoError(t, err)
			fmt.Printf("Commit %d %x\n", i, rh)
			if hashes[uint64(i)] != nil {
				require.Equal(t, hashes[uint64(i)], rh)
			}
			require.NotNil(t, rh)
			hashes[uint64(i)] = rh
		}
	}

	err = agg.Flush(ctx, rwTx)
	require.NoError(t, err)

	unwindTo := uint64(commitStep * rnd.Intn(int(maxTx)/commitStep))

	acu := agg.MakeContext()
	err = acu.Unwind(ctx, unwindTo, rwTx)
	require.NoError(t, err)
	acu.Close()

	err = rwTx.Commit()
	require.NoError(t, err)
	if count > 0 {
		count--
	}
	if count == 0 {
		return
	}

	goto Loop
}
