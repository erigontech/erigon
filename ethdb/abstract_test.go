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
		db, errOpen := ethdb.Open(ctx, ethdb.ProviderOpts(ethdb.Bolt).InMemory(true))
		assert.NoError(t, errOpen)

		if err := db.Update(ctx, func(tx *ethdb.Tx) error {
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

		if err := db.View(ctx, func(tx *ethdb.Tx) error {
			b := tx.Bucket(dbutils.AccountsBucket)
			c := b.CursorOpts().Prefetch(1000).Cursor()

			// b.Cursor().Prefetch(1000)
			// c := tx.Bucket(dbutils.AccountsBucket).Cursor().Prefetch(1000)

			for k, v, err := c.First(); k != nil || err != nil; k, v, err = c.Next() {
				if err != nil {
					return err
				}
				_ = v
			}

			for k, vSize, err := c.FirstKey(); k != nil || err != nil; k, vSize, err = c.NextKey() {
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
		db, errOpen := ethdb.Open(ctx, ethdb.ProviderOpts(ethdb.Badger).InMemory(true))
		assert.NoError(t, errOpen)

		if err := db.Update(ctx, func(tx *ethdb.Tx) error {
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

		if err := db.View(ctx, func(tx *ethdb.Tx) error {
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
			c := b.Cursor(b.CursorOpts())

			for k, v, err := c.First(); k != nil || err != nil; k, v, err = c.Next() {
				if err != nil {
					return err
				}
				_ = v
			}

			for k, vSize, err := c.FirstKey(); k != nil || err != nil; k, vSize, err = c.NextKey() {
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
}

func TestCancelTest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
	defer cancel()

	db, errOpen := ethdb.Open(ctx, ethdb.ProviderOpts(ethdb.Bolt).InMemory(true))
	assert.NoError(t, errOpen)
	if err := db.Update(ctx, func(tx *ethdb.Tx) error {
		b := tx.Bucket(dbutils.AccountsBucket)
		if err := b.Put([]byte{1}, []byte{1}); err != nil {
			return err
		}

		c := b.Cursor(b.CursorOpts())
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

	db, errOpen := ethdb.Open(ctx, ethdb.ProviderOpts(ethdb.Bolt).InMemory(true))
	assert.NoError(t, errOpen)
	if err := db.Update(ctx, func(tx *ethdb.Tx) error {
		b := tx.Bucket(dbutils.AccountsBucket)
		if err := b.Put(common.FromHex("10"), []byte{1}); err != nil {
			return err
		}
		if err := b.Put(common.FromHex("20"), []byte{1}); err != nil {
			return nil
		}

		c := b.CursorOpts().Prefix(common.FromHex("2")).Cursor()
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

		c = b.CursorOpts().Cursor()
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
		db, errOpen := ethdb.Open(ctx, ethdb.ProviderOpts(ethdb.Bolt).InMemory(true))
		assert.NoError(t, errOpen)
		_ = db
		// db.Begin()
	})
}
