/*
   Copyright 2022 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package mdbx

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func TestSeekBothRange(t *testing.T) {
	path := t.TempDir()
	logger := log.New()
	table := "Table"
	db := NewMDBX(logger).Path(path).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			table: kv.TableCfgItem{Flags: kv.DupSort},
		}
	}).MustOpen()
	defer db.Close()

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	c, err := tx.RwCursorDupSort(table)
	require.NoError(t, err)
	defer c.Close()

	// Insert some dupsorted records
	require.NoError(t, c.Put([]byte("key1"), []byte("value1.1")))
	require.NoError(t, c.Put([]byte("key3"), []byte("value3.1")))
	require.NoError(t, c.Put([]byte("key1"), []byte("value1.3")))
	require.NoError(t, c.Put([]byte("key3"), []byte("value3.3")))

	v, err := c.SeekBothRange([]byte("key2"), []byte("value1.2"))
	require.NoError(t, err)
	// SeekBothRange does extact match of the key, but range match of the value, so we get nil here
	require.Nil(t, v)

	v, err = c.SeekBothRange([]byte("key3"), []byte("value3.2"))
	require.NoError(t, err)
	require.Equal(t, "value3.3", string(v))
}

func TestLastDup(t *testing.T) {
	path := t.TempDir()
	logger := log.New()
	table := "Table"
	db := NewMDBX(logger).Path(path).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			table: kv.TableCfgItem{Flags: kv.DupSort},
		}
	}).MustOpen()
	defer db.Close()

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	c, err := tx.RwCursorDupSort(table)
	require.NoError(t, err)
	defer c.Close()

	// Insert some dupsorted records
	require.NoError(t, c.Put([]byte("key1"), []byte("value1.1")))
	require.NoError(t, c.Put([]byte("key3"), []byte("value3.1")))
	require.NoError(t, c.Put([]byte("key1"), []byte("value1.3")))
	require.NoError(t, c.Put([]byte("key3"), []byte("value3.3")))

	err = tx.Commit()
	require.NoError(t, err)
	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	roC, err := roTx.CursorDupSort(table)
	require.NoError(t, err)
	defer roC.Close()

	var keys, vals []string
	var k, v []byte
	for k, _, err = roC.First(); err == nil && k != nil; k, _, err = roC.NextNoDup() {
		v, err = roC.LastDup()
		require.NoError(t, err)
		keys = append(keys, string(k))
		vals = append(vals, string(v))
	}
	require.NoError(t, err)
	require.Equal(t, []string{"key1", "key3"}, keys)
	require.Equal(t, []string{"value1.3", "value3.3"}, vals)
}

func TestPutGet(t *testing.T) {
	path := t.TempDir()
	logger := log.New()
	table := "Table"
	db := NewMDBX(logger).Path(path).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			table: kv.TableCfgItem{Flags: kv.DupSort},
		}
	}).MustOpen()
	defer db.Close()

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	c, err := tx.RwCursorDupSort(table)
	require.NoError(t, err)
	defer c.Close()

	// Insert some dupsorted records
	require.NoError(t, c.Put([]byte("key1"), []byte("value1.1")))
	require.NoError(t, c.Put([]byte("key3"), []byte("value3.1")))
	require.Error(t, c.Put([]byte(""), []byte("value1.1")))

	var v []byte
	v, err = tx.GetOne(table, []byte("key1"))
	require.Nil(t, err)
	require.Equal(t, v, []byte("value1.1"))

	v, err = tx.GetOne("RANDOM", []byte("key1"))
	require.Error(t, err) // Error from non-existent bucket returns error
	require.Nil(t, v)
}

func TestIncrementSequence(t *testing.T) {
	path := t.TempDir()
	logger := log.New()
	table := "Table"
	db := NewMDBX(logger).Path(path).WithTablessCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
		return kv.TableCfg{
			table:       kv.TableCfgItem{Flags: kv.DupSort},
			kv.Sequence: kv.TableCfgItem{},
		}
	}).MustOpen()
	defer db.Close()

	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	c, err := tx.RwCursorDupSort(table)
	require.NoError(t, err)
	defer c.Close()

	// Insert some dupsorted records
	require.NoError(t, tx.Put(table, []byte("key1"), []byte("value1.1")))
	require.NoError(t, tx.Put(table, []byte("key2"), []byte("value2.1")))
	require.NoError(t, tx.Put(table, []byte("key3"), []byte("value3.1")))
	require.NoError(t, tx.Put(table, []byte("key4"), []byte("value4.1")))
	require.NoError(t, tx.Put(table, []byte("key5"), []byte("value5.1")))

	tx.IncrementSequence(table, uint64(12))
	chaV, err := tx.ReadSequence(table)
	require.Nil(t, err)
	require.Equal(t, chaV, uint64(0xc))
}
