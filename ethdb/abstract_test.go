package ethdb_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagedTx(t *testing.T) {
	ctx := context.Background()

	t.Run("Bolt", func(t *testing.T) {
		db, err := ethdb.Open(ctx, ethdb.ProviderOpts(ethdb.Bolt).InMemory(true))
		assert.NoError(t, err)

		if err := db.Update(ctx, func(tx *ethdb.Tx) error {
			b, err := tx.Bucket(dbutils.IntermediateTrieHashBucket)
			if err != nil {
				return err
			}

			err = b.Put([]byte("key1"), []byte("val1"))
			if err != nil {
				return err
			}
			err = b.Put([]byte("key2"), []byte("val2"))
			if err != nil {
				return err
			}

			return nil
		}); err != nil {
			assert.NoError(t, err)
		}

		if err := db.View(ctx, func(tx *ethdb.Tx) error {
			b, err := tx.Bucket(dbutils.IntermediateTrieHashBucket)
			if err != nil {
				return err
			}

			c, err := b.CursorOpts().Prefetch(1000).Cursor()
			if err != nil {
				return err
			}

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
		db, err := ethdb.Open(ctx, ethdb.ProviderOpts(ethdb.Badger).InMemory(true))
		assert.NoError(t, err)

		if err := db.Update(ctx, func(tx *ethdb.Tx) error {
			b, err := tx.Bucket(dbutils.IntermediateTrieHashBucket)
			if err != nil {
				return err
			}

			err = b.Put([]byte("key1"), []byte("val1"))
			if err != nil {
				return err
			}
			err = b.Put([]byte("key2"), []byte("val2"))
			if err != nil {
				return err
			}

			return nil
		}); err != nil {
			assert.NoError(t, err)
		}

		if err := db.View(ctx, func(tx *ethdb.Tx) error {
			b, err := tx.Bucket(dbutils.IntermediateTrieHashBucket)
			if err != nil {
				return err
			}

			//err := b.Iter().From(key).MatchBits(common.HashLength * 8).ForEach()
			//err := b.Cursor().From(key).MatchBits(common.HashLength * 8).ForEach(func(k, v []byte) (bool, error) {
			//})

			//err := b.Cursor().From(key).To(common.HashLength * 8).ForEach(func(k, v []byte) (bool, error) {
			//})

			//
			//c, err := b.IterOpts().From(key).MatchBits(common.HashLength * 8).Iter()
			//c, err := tx.Bucket(dbutil.AccountBucket).CursorOpts().From(key).Cursor()
			//
			//c, err := b.Cursor(b.CursorOpts().From(key).MatchBits(common.HashLength * 8))
			c, err := b.Cursor(b.CursorOpts())
			if err != nil {
				return err
			}

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

	db, err := ethdb.Open(ctx, ethdb.ProviderOpts(ethdb.Bolt).InMemory(true))
	assert.NoError(t, err)
	err = db.Update(ctx, func(tx *ethdb.Tx) error {
		b, err := tx.Bucket(dbutils.IntermediateTrieHashBucket)
		if err != nil {
			return err
		}
		err = b.Put([]byte{1}, []byte{1})
		if err != nil {
			return err
		}

		c, err := b.Cursor(b.CursorOpts())
		for {
			for k, _, err := c.First(); k != nil || err != nil; k, _, err = c.Next() {
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	require.True(t, errors.Is(context.DeadlineExceeded, err))
}

func TestUnmanagedTx(t *testing.T) {
	ctx := context.Background()

	t.Run("Bolt", func(t *testing.T) {
		db, err := ethdb.Open(ctx, ethdb.ProviderOpts(ethdb.Bolt).InMemory(true))
		assert.NoError(t, err)
		_ = db
		// db.Begin()
	})
}
