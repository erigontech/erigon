package ethdb_test

import (
	"context"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/stretchr/testify/assert"
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

			c, err := b.Cursor(b.CursorOpts().PrefetchSize(1000))
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

			c, err := b.Cursor(b.CursorOpts().PrefetchSize(1000))
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

func TestUnmanagedTx(t *testing.T) {
	ctx := context.Background()

	t.Run("Bolt", func(t *testing.T) {
		db, err := ethdb.Open(ctx, ethdb.ProviderOpts(ethdb.Bolt).InMemory(true))
		assert.NoError(t, err)
		_ = db
		// db.Begin()
	})
}
