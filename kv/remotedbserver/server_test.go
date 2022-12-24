package remotedbserver

import (
	"context"
	"runtime"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestKvServer_renew(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}

	require, ctx, db := require.New(t), context.Background(), memdb.NewTestDB(t)
	require.NoError(db.Update(ctx, func(tx kv.RwTx) error {
		wc, err := tx.RwCursorDupSort(kv.PlainState)
		require.NoError(err)
		require.NoError(wc.Append([]byte{1}, []byte{1}))
		require.NoError(wc.Append([]byte{1}, []byte{2}))
		require.NoError(wc.Append([]byte{2}, []byte{1}))
		require.NoError(wc.Append([]byte{3}, []byte{1}))
		return nil
	}))

	s := NewKvServer(ctx, db, nil, nil)
	g, ctx := errgroup.WithContext(ctx)
	testCase := func() error {
		id, err := s.begin(ctx)
		if err != nil {
			return err
		}
		var c, c2 kv.Cursor
		if err = s.with(id, func(tx kv.Tx) error {
			c, err = tx.Cursor(kv.PlainState)
			return err
		}); err != nil {
			return err
		}
		k, v, err := c.First()
		require.NoError(err)
		require.Equal([]byte{1}, k)
		require.Equal([]byte{1}, v)

		if err = s.renew(ctx, id); err != nil {
			return err
		}

		if err = s.with(id, func(tx kv.Tx) error {
			c, err = tx.Cursor(kv.PlainState)
			if err != nil {
				return err
			}
			c2, err = tx.Cursor(kv.PlainState)
			return err
		}); err != nil {
			return err
		}

		k, v, err = c.Next()
		require.NoError(err)
		require.Equal([]byte{1}, k)
		require.Equal([]byte{1}, v)
		k, v, err = c2.Next()
		require.NoError(err)
		require.Equal([]byte{1}, k)
		require.Equal([]byte{1}, v)

		s.rollback(id)
		return nil
	}
	for i := 0; i < 100; i++ {
		g.Go(testCase)
	}
	require.NoError(g.Wait())
}
