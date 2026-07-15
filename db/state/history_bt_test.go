// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datastruct/btindex"
	"github.com/erigontech/erigon/db/seg"
)

// TestVIBt_VarLenKeys covers variable-length keys with prefix relationships
// (like commitment's trie-node prefixes). Raw key||txNum concatenation is only
// order-preserving for equal-length keys; when one key is a prefix of another,
// the txNum bytes collide with the longer key's extra bytes and bt.Seek lands on
// the wrong page. The order-preserving anchor/probe encoding must handle this.
func TestVIBt_VarLenKeys(t *testing.T) {
	t.Parallel()
	logger := log.New()
	tmpDir := t.TempDir()
	vPath := filepath.Join(tmpDir, "test.v")
	const pageSize = 4
	const pageCompressed = true

	rawKeys := [][]byte{
		{0x01}, {0x01, 0x00}, {0x01, 0x00, 0x00}, {0x01, 0xff},
		{0x02}, {0x02, 0x00, 0x05}, {0x02, 0x05},
		{0x00}, {0x00, 0x00},
		{0xff}, {0xff, 0x00}, {0xff, 0xff, 0xff},
	}
	type entry struct {
		key   []byte
		txNum uint64
		val   []byte
	}
	var entries []entry
	for _, k := range rawKeys {
		for _, tx := range []uint64{5, 50, 500, 5000} {
			entries = append(entries, entry{append([]byte{}, k...), tx, []byte(fmt.Sprintf("v-%x-%d", k, tx))})
		}
	}
	// physical .v order = (key, txNum) key-major tuple order
	sort.Slice(entries, func(i, j int) bool {
		if c := bytes.Compare(entries[i].key, entries[j].key); c != 0 {
			return c < 0
		}
		return entries[i].txNum < entries[j].txNum
	})

	c, err := seg.NewCompressor(t.Context(), "v", vPath, tmpDir, seg.DefaultCfg.WithValuesOnCompressedPage(pageSize), log.LvlTrace, logger)
	require.NoError(t, err)
	pw := seg.NewPagedWriter(t.Context(), seg.NewWriter(c, seg.CompressNone), pageCompressed, 1)
	for _, e := range entries {
		sk := make([]byte, 8+len(e.key))
		binary.BigEndian.PutUint64(sk, e.txNum)
		copy(sk[8:], e.key)
		require.NoError(t, pw.Add(sk, e.val))
	}
	require.NoError(t, pw.Flush())
	require.NoError(t, pw.Compress())
	c.Close()

	vDecomp, err := seg.NewDecompressor(vPath)
	require.NoError(t, err)
	defer vDecomp.Close()
	anchorPath := filepath.Join(tmpDir, "test.vanchor")
	btPath := filepath.Join(tmpDir, "test.vbt")
	require.NoError(t, buildVIBt(t.Context(), vDecomp, pageCompressed, anchorPath, btPath, tmpDir, 1, background.NewProgressSet(), logger, true))

	ad, err := seg.NewDecompressor(anchorPath)
	require.NoError(t, err)
	defer ad.Close()
	bt, err := btindex.OpenBtreeIndexWithDecompressor(btPath, btindex.DefaultBtreeM, seg.NewReader(ad.MakeGetter(), seg.CompressNone))
	require.NoError(t, err)
	defer bt.Close()

	anchorGetter := seg.NewReader(ad.MakeGetter(), seg.CompressNone)
	vGetter := seg.NewReader(vDecomp.MakeGetter(), seg.CompressNone)
	var pb, sb, pgb []byte
	for _, e := range entries {
		var val []byte
		var found bool
		val, pb, sb, pgb, found, err = vibtSeek(bt, anchorGetter, vGetter, e.key, e.txNum, pageCompressed, pb, sb, pgb)
		require.NoError(t, err)
		require.Truef(t, found, "key=%x tx=%d not found", e.key, e.txNum)
		require.Equalf(t, e.val, val, "key=%x tx=%d", e.key, e.txNum)
	}
}

func TestVIBt_RoundTrip(t *testing.T) {
	t.Parallel()
	logger := log.New()
	tmpDir := t.TempDir()
	vPath := filepath.Join(tmpDir, "test.v")
	const pageSize = 4
	const pageCompressed = true

	type entry struct {
		key   []byte
		txNum uint64
		val   []byte
	}
	var entries []entry

	c, err := seg.NewCompressor(t.Context(), "v", vPath, tmpDir, seg.DefaultCfg.WithValuesOnCompressedPage(pageSize), log.LvlTrace, logger)
	require.NoError(t, err)
	pw := seg.NewPagedWriter(t.Context(), seg.NewWriter(c, seg.CompressNone), pageCompressed, 1)
	// key-major, txNum-ascending — mirrors the physical order buildVI produces.
	for keyNum := uint64(1); keyNum <= 7; keyNum++ {
		accKey := make([]byte, 4)
		binary.BigEndian.PutUint32(accKey, uint32(keyNum))
		for _, txNum := range []uint64{keyNum, keyNum * 10, keyNum * 100, keyNum * 1000} {
			val := []byte(fmt.Sprintf("val-%d-%d", keyNum, txNum))
			sk := make([]byte, 8+len(accKey))
			binary.BigEndian.PutUint64(sk, txNum)
			copy(sk[8:], accKey)
			require.NoError(t, pw.Add(sk, val))
			entries = append(entries, entry{key: accKey, txNum: txNum, val: val})
		}
	}
	require.NoError(t, pw.Flush())
	require.NoError(t, pw.Compress())
	c.Close()

	vDecomp, err := seg.NewDecompressor(vPath)
	require.NoError(t, err)
	defer vDecomp.Close()

	anchorPath := filepath.Join(tmpDir, "test.vef")
	btPath := filepath.Join(tmpDir, "test.vbt")
	require.NoError(t, buildVIBt(t.Context(), vDecomp, pageCompressed, anchorPath, btPath, tmpDir, 1, background.NewProgressSet(), logger, true))

	ad, err := seg.NewDecompressor(anchorPath)
	require.NoError(t, err)
	defer ad.Close()
	bt, err := btindex.OpenBtreeIndexWithDecompressor(btPath, btindex.DefaultBtreeM, seg.NewReader(ad.MakeGetter(), seg.CompressNone))
	require.NoError(t, err)
	defer bt.Close()

	anchorGetter := seg.NewReader(ad.MakeGetter(), seg.CompressNone)
	vGetter := seg.NewReader(vDecomp.MakeGetter(), seg.CompressNone)
	var pb, sb, pgb []byte
	for _, e := range entries {
		var val []byte
		var found bool
		val, pb, sb, pgb, found, err = vibtSeek(bt, anchorGetter, vGetter, e.key, e.txNum, pageCompressed, pb, sb, pgb)
		require.NoError(t, err)
		require.True(t, found, "key=%x tx=%d not found", e.key, e.txNum)
		require.Equal(t, e.val, val, "key=%x tx=%d", e.key, e.txNum)
	}

	// a (key, txNum) that was never written must not resolve to a wrong value
	missKey := make([]byte, 4)
	binary.BigEndian.PutUint32(missKey, 3)
	val, _, _, _, found, err := vibtSeek(bt, anchorGetter, vGetter, missKey, 999999, pageCompressed, pb, sb, pgb)
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, val)
}
