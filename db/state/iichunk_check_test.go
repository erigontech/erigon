package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
)

// II_CHECK_CHAINDATA=<datadir>/chaindata: the values table must be the exact inverse of the keys table.
func TestIIChunksMatchKeysTable(t *testing.T) {
	path := os.Getenv("II_CHECK_CHAINDATA")
	if path == "" {
		return
	}
	tables := [][2]string{
		{kv.TblLogAddressKeys, kv.TblLogAddressIdx}, {kv.TblLogTopicsKeys, kv.TblLogTopicsIdx},
		{kv.TblTracesFromKeys, kv.TblTracesFromIdx}, {kv.TblTracesToKeys, kv.TblTracesToIdx},
	}
	db := mdbx.New(dbcfg.ChainDB, log.New()).Path(path).Readonly(true).Accede(true).WithTableCfg(func(kv.TableCfg) kv.TableCfg {
		cfg := kv.TableCfg{}
		for _, tbl := range tables {
			cfg[tbl[0]], cfg[tbl[1]] = kv.TableCfgItem{Flags: kv.DupSort}, kv.TableCfgItem{Flags: kv.DupSort}
		}
		return cfg
	}).MustOpen()
	defer db.Close()
	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	bad := 0
	for _, tbl := range tables {
		want := map[string][]uint64{}
		minKeyTx, nKeys := uint64(1<<63), 0
		kc, err := tx.Cursor(tbl[0])
		require.NoError(t, err)
		for k, v, err := kc.First(); k != nil; k, v, err = kc.Next() {
			require.NoError(t, err)
			n := binary.BigEndian.Uint64(k)
			minKeyTx = min(minKeyTx, n)
			want[string(v)] = append(want[string(v)], n)
			nKeys++
		}
		kc.Close()

		got := map[string][]uint64{}
		nChunks, nLegacy, nVals, belowKeys := 0, 0, 0, 0
		vc, err := tx.Cursor(tbl[1])
		require.NoError(t, err)
		var prevK []byte
		var prevMax uint64
		for k, v, err := vc.First(); k != nil; k, v, err = vc.Next() {
			require.NoError(t, err)
			txNums, err := decodeIIChunk(nil, v)
			if err != nil || len(v) > 8+iiChunkGapsLimit+binary.MaxVarintLen64 {
				bad++
				t.Errorf("%s key %x: bad chunk len=%d err=%v", tbl[1], k, len(v), err)
				continue
			}
			nChunks++
			if len(v) == 8 {
				nLegacy++
			}
			slices.Sort(txNums)
			if bytes.Equal(k, prevK) && txNums[0] <= prevMax {
				bad++
				t.Errorf("%s key %x: chunk min %d <= previous chunk max %d", tbl[1], k, txNums[0], prevMax)
			}
			prevK, prevMax = bytes.Clone(k), txNums[len(txNums)-1]
			for _, n := range txNums {
				nVals++
				if n < minKeyTx {
					belowKeys++
					continue
				}
				got[string(k)] = append(got[string(k)], n)
			}
		}
		vc.Close()

		missing, extra := 0, 0
		for k, w := range want {
			slices.Sort(w)
			g := got[k]
			slices.Sort(g)
			if !slices.Equal(w, g) {
				missing += len(setMinus(w, g))
				extra += len(setMinus(g, w))
				if bad < 20 {
					t.Errorf("%s key %x: keys table %d txNums, values table %d", tbl[1], k, len(w), len(g))
				}
				bad++
			}
		}
		for k, g := range got {
			if _, ok := want[k]; !ok {
				extra += len(g)
				bad++
			}
		}
		fmt.Printf("%-14s keys=%d minTx=%d | chunks=%d legacy=%d txNums=%d belowKeys=%d | missing=%d extra=%d\n",
			tbl[1], nKeys, minKeyTx, nChunks, nLegacy, nVals, belowKeys, missing, extra)
	}
	require.Zero(t, bad)
	fmt.Println("II_CHECK_OK")
}

func setMinus(a, b []uint64) []uint64 {
	var out []uint64
	for _, x := range a {
		if _, ok := slices.BinarySearch(b, x); !ok {
			out = append(out, x)
		}
	}
	return out
}
