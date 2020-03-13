package ethdb_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagedTx(t *testing.T) {
	ctx := context.Background()

	t.Run("Bolt", func(t *testing.T) {
		var db ethdb.DB
		var errOpen error
		db, errOpen = ethdb.NewBolt().InMem(true).Open(ctx)
		assert.NoError(t, errOpen)

		if err := db.Update(ctx, func(tx ethdb.Tx) error {
			b := tx.Bucket(dbutils.AccountsBucket)

			if err := b.Put([]byte("key1"), []byte("val1")); err != nil {
				return err
			}

			if err := b.Put([]byte("key2"), []byte("val2")); err != nil {
				return err
			}

			return nil
		}); err != nil {
			assert.NoError(t, err)
		}

		if err := db.View(ctx, func(tx ethdb.Tx) error {
			b := tx.Bucket(dbutils.AccountsBucket)
			c := b.Cursor().Prefetch(1000)

			// b.Cursor().Prefetch(1000)
			// c := tx.Bucket(dbutils.AccountsBucket).Cursor().Prefetch(1000)

			for k, v, err := c.First(); k != nil || err != nil; k, v, err = c.Next() {
				if err != nil {
					return err
				}
				_ = v
			}

			c2 := c.NoValues()
			for k, vSize, err := c2.First(); k != nil || err != nil; k, vSize, err = c2.Next() {
				if err != nil {
					return err
				}
				_ = vSize
			}

			for k, v, err := c.Seek([]byte("prefix")); k != nil || err != nil; k, v, err = c.Next() {
				if err != nil {
					return err
				}
				_ = v
			}

			return nil
		}); err != nil {
			assert.NoError(t, err)
		}
	})

	t.Run("Badger", func(t *testing.T) {
		var db ethdb.DB
		var errOpen error
		db, errOpen = ethdb.NewBadger().InMem(true).Open(ctx)
		assert.NoError(t, errOpen)

		if err := db.Update(ctx, func(tx ethdb.Tx) error {
			b := tx.Bucket(dbutils.AccountsBucket)
			if err := b.Put([]byte{0, 1}, []byte{1}); err != nil {
				return err
			}
			if err := b.Put([]byte{0, 0, 1}, []byte{1}); err != nil {
				return err
			}
			if err := b.Put([]byte{2}, []byte{1}); err != nil {
				return err
			}
			if err := b.Put([]byte{1}, []byte{1}); err != nil {
				return err
			}

			return nil
		}); err != nil {
			assert.NoError(t, err)
		}

		if err := db.View(ctx, func(tx ethdb.Tx) error {
			b := tx.Bucket(dbutils.AccountsBucket)

			//err := b.Iter().From(key).MatchBits(common.HashLength * 8).Walk()
			//err := b.Cursor().From(key).MatchBits(common.HashLength * 8).Walk(func(k, v []byte) (bool, error) {
			//})

			//err := b.Cursor().From(key).MatchBits(common.HashLength * 8).Walk(func(k, v []byte) (bool, error) {
			//})

			//
			//c, err := b.IterOpts().From(key).MatchBits(common.HashLength * 8).Iter()
			//c, err := tx.Bucket(dbutil.AccountBucket).CursorOpts().From(key).Cursor()
			//
			//c, err := b.Cursor(b.CursorOpts().From(key).MatchBits(common.HashLength * 8))
			c := b.Cursor().NoValues()

			for k, v, err := c.First(); k != nil || err != nil; k, v, err = c.Next() {
				if err != nil {
					return err
				}
				_ = v
			}

			for k, vSize, err := c.First(); k != nil || err != nil; k, vSize, err = c.Next() {
				if err != nil {
					return err
				}
				_ = vSize
			}

			for k, v, err := c.Seek([]byte("prefix")); k != nil || err != nil; k, v, err = c.Next() {
				if err != nil {
					return err
				}
				_ = v
			}

			k, _, err := c.First()
			assert.NoError(t, err)
			assert.Equal(t, []byte{0, 0, 1}, k)
			k, _, err = c.Next()
			assert.NoError(t, err)
			assert.Equal(t, []byte{0, 1}, k)
			k, _, err = c.Next()
			assert.NoError(t, err)
			assert.Equal(t, []byte{1}, k)
			k, _, err = c.Next()
			assert.NoError(t, err)
			assert.Equal(t, []byte{2}, k)

			return nil
		}); err != nil {
			assert.NoError(t, err)
		}
	})
}

func TestCancelTest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
	defer cancel()

	var db ethdb.DB
	var errOpen error
	db, errOpen = ethdb.NewBolt().InMem(true).Open(ctx)
	assert.NoError(t, errOpen)
	if err := db.Update(ctx, func(tx ethdb.Tx) error {
		b := tx.Bucket(dbutils.AccountsBucket)
		if err := b.Put([]byte{1}, []byte{1}); err != nil {
			return err
		}

		c := b.Cursor()
		for {
			for k, _, err := c.First(); k != nil || err != nil; k, _, err = c.Next() {
				if err != nil {
					return err
				}
			}
		}
	}); err != nil {
		require.True(t, errors.Is(context.DeadlineExceeded, err))
	}
}

func TestFilterTest(t *testing.T) {
	ctx := context.Background()

	var db ethdb.DB
	var errOpen error
	db, errOpen = ethdb.NewBolt().InMem(true).Open(ctx)
	assert.NoError(t, errOpen)
	if err := db.Update(ctx, func(tx ethdb.Tx) error {
		b := tx.Bucket(dbutils.AccountsBucket)
		if err := b.Put(common.FromHex("10"), []byte{1}); err != nil {
			return err
		}
		if err := b.Put(common.FromHex("20"), []byte{1}); err != nil {
			return nil
		}

		c := b.Cursor().Prefix(common.FromHex("2"))
		counter := 0
		for k, _, err := c.First(); k != nil || err != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			counter++
		}
		assert.Equal(t, 1, counter)

		counter = 0
		if err := c.Walk(func(_, _ []byte) (bool, error) {
			counter++
			return true, nil
		}); err != nil {
			return err
		}
		assert.Equal(t, 1, counter)

		c = b.Cursor()
		counter = 0
		for k, _, err := c.First(); k != nil || err != nil; k, _, err = c.Next() {
			if err != nil {
				return err
			}
			counter++
		}
		assert.Equal(t, 2, counter)

		counter = 0
		if err := c.Walk(func(_, _ []byte) (bool, error) {
			counter++
			return true, nil
		}); err != nil {
			return err
		}
		assert.Equal(t, 2, counter)

		return nil
	}); err != nil {
		require.NoError(t, err)
	}
}

func TestUnmanagedTx(t *testing.T) {
	ctx := context.Background()

	t.Run("Bolt", func(t *testing.T) {
		var db ethdb.DB
		var errOpen error
		db, errOpen = ethdb.NewBolt().InMem(true).Open(ctx)
		assert.NoError(t, errOpen)
		_ = db
		// db.Begin()
	})
}
