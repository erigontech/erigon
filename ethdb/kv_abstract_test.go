package ethdb_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote/remotedbserver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagedTx(t *testing.T) {
	ctx := context.Background()

	writeDBs := []ethdb.KV{
		ethdb.NewBolt().InMem().MustOpen(ctx),
		ethdb.NewBadger().InMem().MustOpen(ctx),
		ethdb.NewBolt().InMem().MustOpen(ctx), // for remote db
	}

	serverIn, clientOut := io.Pipe()
	clientIn, serverOut := io.Pipe()

	readDBs := []ethdb.KV{
		writeDBs[0],
		writeDBs[1],
		ethdb.NewRemote().InMem(clientIn, clientOut).MustOpen(ctx),
	}
	go func() {
		if err := remotedbserver.Server(ctx, writeDBs[2], serverIn, serverOut, nil); err != nil {
			require.NoError(t, err)
		}
	}()

	for _, db := range writeDBs {
		if err := db.Update(ctx, func(tx ethdb.Tx) error {
			b := tx.Bucket(dbutils.AccountsBucket)
			for i := uint8(0); i < 10; i++ {
				require.NoError(t, b.Put([]byte{i}, []byte{1}))
			}
			require.NoError(t, b.Put([]byte{0, 1}, []byte{1}))
			require.NoError(t, b.Put([]byte{0, 0, 1}, []byte{1}))
			require.NoError(t, b.Put([]byte{2}, []byte{1}))
			return nil
		}); err != nil {
			require.NoError(t, err)
		}
	}

	for _, db := range readDBs {
		msg := fmt.Sprintf("%T", db)

		t.Run("NoValues iterator "+msg, func(t *testing.T) {
			assert := assert.New(t)

			if err := db.View(ctx, func(tx ethdb.Tx) error {
				b := tx.Bucket(dbutils.AccountsBucket)
				c := b.Cursor().NoValues()

				k, _, err := c.First()
				assert.NoError(err)
				assert.Equal([]byte{0}, k)
				k, _, err = c.Next()
				assert.NoError(err)
				assert.Equal([]byte{0, 0, 1}, k)
				k, _, err = c.Next()
				assert.NoError(err)
				assert.Equal([]byte{0, 1}, k)
				k, _, err = c.Next()
				assert.NoError(err)
				assert.Equal([]byte{1}, k)

				return nil
			}); err != nil {
				assert.NoError(err)
			}
		})

		t.Run("ctx cancel "+msg, func(t *testing.T) {
			assert := assert.New(t)
			ctx, cancel := context.WithTimeout(ctx, time.Microsecond)
			defer cancel()

			if err := db.View(ctx, func(tx ethdb.Tx) error {
				c := tx.Bucket(dbutils.AccountsBucket).Cursor()
				for {
					for k, _, err := c.First(); k != nil || err != nil; k, _, err = c.Next() {
						if err != nil {
							return err
						}
					}
				}
			}); err != nil {
				assert.True(errors.Is(context.DeadlineExceeded, err))
			}
		})

		t.Run("filter "+msg, func(t *testing.T) {
			assert := assert.New(t)

			if err := db.View(ctx, func(tx ethdb.Tx) error {
				b := tx.Bucket(dbutils.AccountsBucket)
				c := b.Cursor().Prefix([]byte{2})
				counter := 0
				for k, _, err := c.First(); k != nil || err != nil; k, _, err = c.Next() {
					if err != nil {
						return err
					}
					counter++
				}
				assert.Equal(1, counter)

				counter = 0
				if err := c.Walk(func(_, _ []byte) (bool, error) {
					counter++
					return true, nil
				}); err != nil {
					return err
				}
				assert.Equal(1, counter)

				c = b.Cursor()
				counter = 0
				for k, _, err := c.First(); k != nil || err != nil; k, _, err = c.Next() {
					if err != nil {
						return err
					}
					counter++
				}
				assert.Equal(12, counter)

				counter = 0
				if err := c.Walk(func(_, _ []byte) (bool, error) {
					counter++
					return true, nil
				}); err != nil {
					return err
				}
				assert.Equal(12, counter)

				return nil
			}); err != nil {
				assert.NoError(err)
			}
		})
	}

}
