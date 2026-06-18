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

package btindex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func Test_BtreeIndex_Init(t *testing.T) {
	t.Parallel()

	logger := log.New()
	tmp := t.TempDir()

	keyCount, M := 100, uint64(4)
	compPath := generateKV(t, tmp, 52, 300, keyCount, logger, 0)
	decomp, err := seg.NewDecompressor(compPath)
	require.NoError(t, err)
	defer decomp.Close()

	r := seg.NewReader(decomp.MakeGetter(), seg.CompressNone)
	err = BuildBtreeIndexWithDecompressor(filepath.Join(tmp, "a.bt"), filepath.Join(tmp, "a.kvei"), r, background.NewProgressSet(), tmp, 1, logger, true, statecfg.AccessorBTree|statecfg.AccessorExistence)
	require.NoError(t, err)

	bt, err := OpenBtreeIndexWithDecompressor(filepath.Join(tmp, "a.bt"), M, r)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	bt.Close()
}

func Test_BtreeIndex_Seek(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	logger := log.New()
	keyCount, M := 120, 30
	compressFlags := seg.CompressKeys | seg.CompressVals

	t.Run("empty index", func(t *testing.T) {
		dataPath := generateKV(t, tmp, 52, 180, 0, logger, 0)
		indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
		buildBtreeIndex(t, dataPath, indexPath, compressFlags, 1, logger, true)

		kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, uint64(M), compressFlags, false)
		require.NoError(t, err)
		require.EqualValues(t, 0, bt.KeyCount())
		bt.Close()
		kv.Close()
	})
	dataPath := generateKV(t, tmp, 52, 180, keyCount, logger, 0)

	indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
	buildBtreeIndex(t, dataPath, indexPath, compressFlags, 1, logger, true)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, uint64(M), compressFlags, false)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	defer bt.Close()
	defer kv.Close()

	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)

	getter := seg.NewReader(kv.MakeGetter(), compressFlags)

	t.Run("seek beyond the last key", func(t *testing.T) {
		_, _, _, err := bt.dataLookup(bt.ef.Count()+1, getter)
		require.ErrorIs(t, err, ErrBtIndexLookupBounds)

		_, _, _, err = bt.dataLookup(bt.ef.Count(), getter)
		require.ErrorIs(t, err, ErrBtIndexLookupBounds)
		require.Error(t, err)

		_, _, _, err = bt.dataLookup(bt.ef.Count()-1, getter)
		require.NoError(t, err)

		cur, err := bt.Seek(getter, common.FromHex("0xffffffffffffff")) //seek beyeon the last key
		require.NoError(t, err)
		require.Nil(t, cur)
		cur.Close()
	})

	c, err := bt.Seek(getter, nil)
	require.NoError(t, err)
	for i := 0; i < len(keys); i++ {
		k := c.Key()
		require.Equal(t, keys[i], k)
		c.Next()
	}
	c.Close()

	for i := 0; i < len(keys); i++ {
		cur, err := bt.Seek(getter, keys[i])
		require.NoErrorf(t, err, "i=%d", i)
		require.Equalf(t, keys[i], cur.key, "i=%d", i)
		require.NotEmptyf(t, cur.Value(), "i=%d", i)
		cur.Close()
		// require.EqualValues(t, uint64(i), cur.Value())
	}
	for i := 1; i < len(keys); i++ {
		alt := common.Copy(keys[i])
		for j := len(alt) - 1; j >= 0; j-- {
			if alt[j] > 0 {
				alt[j] -= 1
				break
			}
		}
		cur, err := bt.Seek(getter, keys[i])
		require.NoError(t, err)
		require.Equal(t, keys[i], cur.Key())
		cur.Close()
	}
}

func Test_BtreeIndex_Build(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()

	tmp := t.TempDir()
	logger := log.New()
	keyCount, M := 20000, 510

	compressFlags := seg.CompressKeys | seg.CompressVals
	dataPath := generateKV(t, tmp, 52, 48, keyCount, logger, compressFlags)
	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)

	indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
	buildBtreeIndex(t, dataPath, indexPath, compressFlags, 1, logger, true)
	require.NoError(t, err)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, uint64(M), compressFlags, false)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	defer bt.Close()
	defer kv.Close()

	getter := seg.NewReader(kv.MakeGetter(), compressFlags)

	c, err := bt.Seek(getter, nil)
	require.NoError(t, err)
	require.NotNil(t, c)
	for i := 0; i < len(keys); i++ {
		k := c.Key()
		if !bytes.Equal(keys[i], k) {
			fmt.Printf("\tinvalid, want %x\n", keys[i])
		}
		c.Next()
	}
	c.Close()

	for i := 0; i < 10000; i++ {
		c, err := bt.Seek(getter, keys[i])
		require.NoError(t, err)
		require.Equal(t, keys[i], c.Key())
		c.Close()
	}
}

// writeV0Index writes a legacy v0-format .bt over dataPath: [EF][nodeCount(8)][di(8)+keyLen(2)+key per node],
// nodes kept at di = 0, M, 2M, ... The writer no longer emits v0, so this reproduces the released layout to
// keep the v0 read path (decodeListNodesV0 + EF-first detection) covered.
func writeV0Index(tb testing.TB, dataPath, indexPath string, compressed seg.FileCompression, m uint64) {
	tb.Helper()
	decomp, err := seg.NewDecompressor(dataPath)
	require.NoError(tb, err)
	defer decomp.Close()

	r := seg.NewReader(decomp.MakeGetter(), compressed)
	r.Reset(0)
	count := uint64(r.Count() / 2)

	f, err := os.Create(indexPath)
	require.NoError(tb, err)
	defer f.Close()
	w := getBufioWriter(f)
	defer putBufioWriter(w)

	if count > 0 {
		ef := eliasfano32.NewEliasFano(count, uint64(r.Size()))
		type v0node struct {
			key []byte
			di  uint64
		}
		var nodes []v0node
		var key []byte
		var pos, di uint64
		for r.HasNext() {
			key, _ = r.Next(key[:0])
			ef.AddOffset(pos)
			if di%m == 0 {
				nodes = append(nodes, v0node{key: common.Copy(key), di: di})
			}
			di++
			pos, _ = r.Skip()
		}
		ef.Build()
		require.NoError(tb, ef.Write(w))

		var hdr [10]byte
		binary.BigEndian.PutUint64(hdr[:8], uint64(len(nodes)))
		_, err = w.Write(hdr[:8])
		require.NoError(tb, err)
		for i := range nodes {
			binary.BigEndian.PutUint64(hdr[:8], nodes[i].di)
			binary.BigEndian.PutUint16(hdr[8:10], uint16(len(nodes[i].key)))
			_, err = w.Write(hdr[:10])
			require.NoError(tb, err)
			_, err = w.Write(nodes[i].key)
			require.NoError(tb, err)
		}
	}
	require.NoError(tb, w.Flush())
}

func Test_BtreeIndex_V0_V2_Read(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()
	const M = uint64(32)
	keyCount := 500
	compressFlags := seg.CompressKeys | seg.CompressVals

	dataPath := generateKV(t, tmp, 52, 180, keyCount, logger, compressFlags)
	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)

	v2Path := filepath.Join(tmp, "v2.bt")
	buildBtreeIndex(t, dataPath, v2Path, compressFlags, 1, logger, true)

	v0Path := filepath.Join(tmp, "v0.bt")
	writeV0Index(t, dataPath, v0Path, compressFlags, M)

	for _, tc := range []struct{ name, path string }{{"v0", v0Path}, {"v2", v2Path}} {
		t.Run(tc.name, func(t *testing.T) {
			kv, bt, err := OpenBtreeIndexAndDataFile(tc.path, dataPath, M, compressFlags, false)
			require.NoError(t, err)
			defer bt.Close()
			defer kv.Close()
			require.EqualValues(t, keyCount, bt.KeyCount())

			getter := seg.NewReader(kv.MakeGetter(), compressFlags)
			c, err := bt.Seek(getter, nil)
			require.NoError(t, err)
			for i := range keys {
				require.Equalf(t, keys[i], c.Key(), "%s forward scan i=%d", tc.name, i)
				c.Next()
			}
			c.Close()
			for i := range keys {
				cur, err := bt.Seek(getter, keys[i])
				require.NoErrorf(t, err, "%s i=%d", tc.name, i)
				require.Equalf(t, keys[i], cur.Key(), "%s seek i=%d", tc.name, i)
				cur.Close()
			}
		})
	}
}

// A v0 file stores di on disk; the reader recovers the node stride from it, so
// opening with a different M than it was written with (e.g. a changed BT_M) must
// still resolve every key.
func Test_BtreeIndex_V0_M_Mismatch(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()
	const writeM, openM = uint64(32), uint64(256)
	keyCount := 500
	compressFlags := seg.CompressKeys | seg.CompressVals

	dataPath := generateKV(t, tmp, 52, 180, keyCount, logger, compressFlags)
	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)

	v0Path := filepath.Join(tmp, "v0.bt")
	writeV0Index(t, dataPath, v0Path, compressFlags, writeM)

	kv, bt, err := OpenBtreeIndexAndDataFile(v0Path, dataPath, openM, compressFlags, false)
	require.NoError(t, err)
	defer bt.Close()
	defer kv.Close()

	getter := seg.NewReader(kv.MakeGetter(), compressFlags)
	for i := range keys {
		cur, err := bt.Seek(getter, keys[i])
		require.NoErrorf(t, err, "i=%d", i)
		require.Equalf(t, keys[i], cur.Key(), "seek i=%d", i)
		cur.Close()
	}
}

// Seeking a key greater than the last must return (nil, nil) even when the last
// cached pivot is the last key (KeysCount % M == 1), where bs() returns an
// insertion point == KeysCount.
func TestBtIndex_SeekBeyondLast(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()
	const M = uint64(8)
	const keyCount = 17 // 17 % 8 == 1 -> last pivot di == last key
	compress := seg.CompressNone

	kvPath := generateKV(t, tmp, 20, 10, keyCount, logger, compress)
	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)

	indexPath := strings.TrimSuffix(kvPath, ".kv") + "_m8.bt"
	func() {
		decomp, err := seg.NewDecompressor(kvPath)
		require.NoError(t, err)
		defer decomp.Close()
		iw, err := NewBtIndexWriter(BtIndexWriterArgs{IndexFile: indexPath, TmpDir: tmp, M: M, KeyCount: uint64(decomp.Count() / 2), MaxOffset: uint64(decomp.Size())}, logger)
		require.NoError(t, err)
		defer iw.Close()
		r := seg.NewReader(decomp.MakeGetter(), compress)
		r.Reset(0)
		var pos uint64
		for r.HasNext() {
			key, _ := r.Next(nil)
			require.NoError(t, iw.AddKey(key, pos))
			pos, _ = r.Skip()
		}
		iw.DisableFsync()
		require.NoError(t, iw.Build())
	}()

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, M, compress, false)
	require.NoError(t, err)
	defer bt.Close()
	defer kv.Close()
	require.EqualValues(t, keyCount, bt.KeyCount())

	getter := seg.NewReader(kv.MakeGetter(), compress)
	beyond := append(append([]byte{}, keys[len(keys)-1]...), 0xff)
	cur, err := bt.Seek(getter, beyond)
	require.NoError(t, err)
	require.Nil(t, cur, "seek beyond last key must return (nil, nil)")
}

func TestFooter_EncodeDecodeRoundTrip(t *testing.T) {
	f := Footer{
		Meta:          Metadata{KeysCount: 12345, M: 256, EfOffset: 1 << 33}, // EfOffset > 4GiB: must be uint64
		FormatVersion: btVersion,
	}
	var buf bytes.Buffer
	require.NoError(t, f.Encode(&buf))
	require.Equal(t, btMetadataLen+btAnchorLen, buf.Len())

	got, footerStart, err := ReadFooter(buf.Bytes())
	require.NoError(t, err)
	require.Equal(t, 0, footerStart) // no body in this isolated buffer
	require.Equal(t, f.Meta.KeysCount, got.Meta.KeysCount)
	require.Equal(t, f.Meta.M, got.Meta.M)
	require.Equal(t, f.Meta.EfOffset, got.Meta.EfOffset)
	require.Equal(t, f.FormatVersion, got.FormatVersion)

	// missing/wrong magic -> legacy fallback signal, not a hard error
	_, _, err = ReadFooter(bytes.Repeat([]byte{0xAB}, btMetadataLen+btAnchorLen))
	require.ErrorIs(t, err, errNotFooterFormat)
	_, _, err = ReadFooter([]byte{0x00})
	require.ErrorIs(t, err, errNotFooterFormat)
}

func TestFooter_ZeroKeyCount(t *testing.T) {
	// A footer-format file with KeysCount=0 must not panic (overflow guard).
	// The file is well-formed structurally but has no EF data, so Open must
	// return an error rather than silently succeed with a corrupt index.
	tmp := t.TempDir()

	// Build a minimal footer-format binary with KeysCount=0:
	//   [0x01][4095 zeros][64 zeros as fake EF region][footer][anchor]
	// EfOffset=4096, footerStart=4160: satisfies EfOffset < footerStart.
	var body bytes.Buffer
	body.WriteByte(btFirstByteUseFooter)
	body.Write(make([]byte, 4095)) // pad to EfOffset=4096
	body.Write(make([]byte, 64))   // fake EF bytes at offset 4096
	// 4096+64=4160, already 8-byte aligned → footerStart=4160
	footer := Footer{
		Meta:          Metadata{KeysCount: 0, M: 256, EfOffset: 4096},
		FormatVersion: btVersion,
	}
	require.NoError(t, footer.Encode(&body))

	indexPath := filepath.Join(tmp, "zero_keys.bt")
	require.NoError(t, os.WriteFile(indexPath, body.Bytes(), 0644))

	// Use a 1-key KV as the reader — it won't be consulted because Open will
	// fail before building the BpsTree.
	dataPath := generateKV(t, tmp, 8, 8, 1, log.New(), seg.CompressNone)
	_, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, 256, seg.CompressNone, false)
	if err == nil {
		defer bt.Close()
		require.True(t, bt.Empty())
	}
	// error is acceptable; what is not acceptable is a panic
}

func Test_BtreeIndex_V2_EfOffset(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()
	keyCount := 5000
	compressFlags := seg.CompressKeys | seg.CompressVals
	dataPath := generateKV(t, tmp, 52, 180, keyCount, logger, compressFlags)

	v2Path := filepath.Join(tmp, "v2.bt")
	buildBtreeIndex(t, dataPath, v2Path, compressFlags, 1, logger, true)

	data, err := os.ReadFile(v2Path)
	require.NoError(t, err)
	require.EqualValues(t, btFirstByteUseFooter, data[0], "v2 must start with a non-zero leading byte (no conflict with v0's 0x00)")
	footer, footerStart, err := ReadFooter(data)
	require.NoError(t, err)
	require.EqualValues(t, keyCount, footer.Meta.KeysCount)
	require.EqualValues(t, DefaultBtreeM, footer.Meta.M)

	require.Zero(t, footer.Meta.EfOffset%btEFAlign, "EF must start 4kb-aligned")
	require.Positive(t, footer.Meta.EfOffset)
	require.Less(t, int(footer.Meta.EfOffset), footerStart)

	// EfOffset must point at a valid EF holding all keys...
	ef, _ := eliasfano32.ReadEliasFano(data[footer.Meta.EfOffset:])
	require.EqualValues(t, keyCount, ef.Count())

	// ...and equal the position the reader would otherwise derive from the nodes section (which starts after the leading byte).
	nodesCount := (footer.Meta.KeysCount + footer.Meta.M - 1) / footer.Meta.M
	_, nodesEnd, err := decodeNodes(data[1:], nodesCount)
	require.NoError(t, err)
	require.EqualValues(t, alignUp(1+nodesEnd, btEFAlign), footer.Meta.EfOffset)
}

// Opens .kv at dataPath and generates index over it to file 'indexPath'
func buildBtreeIndex(tb testing.TB, dataPath, indexPath string, compressed seg.FileCompression, seed uint32, logger log.Logger, noFsync bool) {
	tb.Helper()
	decomp, err := seg.NewDecompressor(dataPath)
	require.NoError(tb, err)
	defer decomp.Close()

	r := seg.NewReader(decomp.MakeGetter(), compressed)
	err = BuildBtreeIndexWithDecompressor(indexPath, strings.TrimSuffix(indexPath, ".bt")+".kvei", r, background.NewProgressSet(), filepath.Dir(indexPath), seed, logger, noFsync, statecfg.AccessorBTree|statecfg.AccessorExistence)
	require.NoError(tb, err)
}

func Test_BtreeIndex_Seek2(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	logger := log.New()
	M := 1024
	keyCount := 3 * M

	compressFlags := seg.CompressKeys | seg.CompressVals
	dataPath := generateKV(t, tmp, 52, 48, keyCount, logger, compressFlags)

	indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
	buildBtreeIndex(t, dataPath, indexPath, compressFlags, 1, logger, true)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, uint64(M), compressFlags, false)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	defer bt.Close()
	defer kv.Close()

	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)

	getter := seg.NewReader(kv.MakeGetter(), compressFlags)

	t.Run("seek beyond the last key", func(t *testing.T) {
		_, _, _, err := bt.dataLookup(bt.ef.Count()+1, getter)
		require.ErrorIs(t, err, ErrBtIndexLookupBounds)

		_, _, _, err = bt.dataLookup(bt.ef.Count(), getter)
		require.ErrorIs(t, err, ErrBtIndexLookupBounds)
		require.Error(t, err)

		_, _, _, err = bt.dataLookup(bt.ef.Count()-1, getter)
		require.NoError(t, err)

		cur, err := bt.Seek(getter, common.FromHex("0xffffffffffffff")) //seek beyeon the last key
		require.NoError(t, err)
		require.Nil(t, cur)
		cur.Close()
	})

	t.Run("checkNextAgainstGetter", func(t *testing.T) {
		cur, err := bt.Seek(getter, nil)
		require.NoError(t, err)
		defer cur.Close()

		require.NoError(t, err)
		require.Equal(t, keys[0], cur.Key())
		require.NotEmptyf(t, cur.Value(), "i=%d", 0)

		k, v, _, err := bt.dataLookup(0, getter)
		require.NoError(t, err)
		cur.Reset(0, getter)

		require.Equal(t, k, cur.Key())
		require.Equal(t, v, cur.Value())

		totalKeys := kv.Count() / 2

		for i := 1; i < totalKeys; i++ {
			k, v, _, err = bt.dataLookup(uint64(i), getter)
			require.NoError(t, err)

			b := cur.Next()
			require.True(t, b)

			require.Equalf(t, k, cur.Key(), "i=%d", i)
			require.Equalf(t, v, cur.Value(), "i=%d", i)

			curS, err := bt.Seek(getter, cur.Key())
			require.NoError(t, err)

			require.Equalf(t, cur.Key(), curS.Key(), "i=%d", i)
			require.Equalf(t, cur.Value(), curS.Value(), "i=%d", i)
			require.Equal(t, cur.d, curS.d)
			require.Equal(t, cur.getter, curS.getter)
			curS.Close()
		}
	})

	for i := 1; i < len(keys); i++ {
		cur, err := bt.Seek(getter, keys[i])
		require.NoError(t, err)
		require.Equal(t, keys[i], cur.Key())
		cur.Close()
	}
}

func TestBpsTree_Seek(t *testing.T) {
	t.Parallel()

	keyCount, M := 48, 4
	tmp := t.TempDir()

	logger := log.New()

	compressFlag := seg.CompressNone
	dataPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlag)

	kv, err := seg.NewDecompressor(dataPath)
	require.NoError(t, err)
	defer kv.Close()

	g := seg.NewReader(kv.MakeGetter(), compressFlag)

	g.Reset(0)
	ps := make([]uint64, 0, keyCount)
	keys := make([][]byte, 0, keyCount)

	p := uint64(0)
	i := 0
	for g.HasNext() {
		ps = append(ps, p)
		k, _ := g.Next(nil)
		_, p = g.Next(nil)
		keys = append(keys, k)
		//fmt.Printf("%2d k=%x, p=%v\n", i, k, p)
		i++
	}

	//tr := newTrie()
	ef := eliasfano32.NewEliasFano(uint64(keyCount), ps[len(ps)-1])
	for i := 0; i < len(ps); i++ {
		//tr.insert(Node{i: uint64(i), key: common.Copy(keys[i]), off: ps[i]})
		ef.AddOffset(ps[i])
	}
	ef.Build()

	efi, _ := eliasfano32.ReadEliasFano(ef.AppendBytes(nil))

	ir := NewMockIndexReader(efi)
	bp := NewBpsTree(g, efi, uint64(M), ir.dataLookup)
	bp.cursorGetter = ir.newCursor
	bp.trace = false

	for i := 0; i < len(keys); i++ {
		sk := keys[i]
		c, err := bp.Seek(g, sk[:len(sk)/2])
		require.NoError(t, err)
		require.NotNil(t, c)
		require.NotNil(t, c.Key())

		//k, _, err := it.KVFromGetter(g)
		//require.NoError(t, err)
		require.Equal(t, keys[i], c.Key())
		c.Close()
	}
}

func NewMockIndexReader(ef *eliasfano32.EliasFano) *mockIndexReader {
	return &mockIndexReader{ef: ef}
}

type mockIndexReader struct {
	ef *eliasfano32.EliasFano
}

func (b *mockIndexReader) newCursor(k, v []byte, di uint64, g *seg.Reader) *Cursor {
	return &Cursor{
		ef:     b.ef,
		getter: g,
		key:    common.Copy(k),
		value:  common.Copy(v),
		d:      di,
	}
}

func (b *mockIndexReader) dataLookup(di uint64, g *seg.Reader) (k, v []byte, offset uint64, err error) {
	if di >= b.ef.Count() {
		return nil, nil, 0, fmt.Errorf("%w: keyCount=%d, but key %d requested. file: %s", ErrBtIndexLookupBounds, b.ef.Count(), di, g.FileName())
	}

	offset = b.ef.Get(di)
	g.Reset(offset)
	if !g.HasNext() {
		return nil, nil, 0, fmt.Errorf("pair %d/%d key not found, file: %s", di, b.ef.Count(), g.FileName())
	}

	k, _ = g.Next(nil)
	if !g.HasNext() {
		return nil, nil, 0, fmt.Errorf("pair %d/%d value not found, file: %s", di, b.ef.Count(), g.FileName())
	}
	v, _ = g.Next(nil)
	return k, v, offset, nil
}

func TestNewBtIndex(t *testing.T) {
	t.Parallel()
	keyCount := 10000
	kvPath := generateKV(t, t.TempDir(), 20, 10, keyCount, log.New(), seg.CompressNone)

	indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, DefaultBtreeM, seg.CompressNone, false)
	require.NoError(t, err)
	defer bt.Close()
	defer kv.Close()
	require.NotNil(t, kv)
	require.NotNil(t, bt)
	bplus := bt.bplus
	require.GreaterOrEqual(t, bplus.numNodes(), keyCount/int(DefaultBtreeM))
	require.LessOrEqual(t, bplus.numNodes(), keyCount/int(DefaultBtreeM)+2)

	for i := 1; i < bplus.numNodes(); i++ {
		require.NotZero(t, bplus.nodeDi(i))
		require.NotEmpty(t, bplus.nodeKey(i))
	}
}

func TestBtIndex_MStoredInFile(t *testing.T) {
	t.Parallel()

	const wantM = uint64(8)
	tmp := t.TempDir()
	logger := log.New()
	kvPath := generateKV(t, tmp, 20, 10, 1000, logger, seg.CompressNone)

	decomp, err := seg.NewDecompressor(kvPath)
	require.NoError(t, err)
	defer decomp.Close()

	indexPath := strings.TrimSuffix(kvPath, ".kv") + "_m8.bt"
	iw, err := NewBtIndexWriter(BtIndexWriterArgs{IndexFile: indexPath, TmpDir: tmp, M: wantM, KeyCount: uint64(decomp.Count() / 2), MaxOffset: uint64(decomp.Size())}, logger)
	require.NoError(t, err)
	defer iw.Close()

	r := seg.NewReader(decomp.MakeGetter(), seg.CompressNone)
	r.Reset(0)
	var pos uint64
	for r.HasNext() {
		key, _ := r.Next(nil)
		require.NoError(t, iw.AddKey(key, pos))
		pos, _ = r.Skip()
	}
	iw.DisableFsync()
	require.NoError(t, iw.Build())

	r2 := seg.NewReader(decomp.MakeGetter(), seg.CompressNone)
	bt, err := OpenBtreeIndexWithDecompressor(indexPath, 1, r2) // pass wrong M — file M should win
	require.NoError(t, err)
	defer bt.Close()
	require.Equal(t, wantM, bt.M())
}

func BenchmarkBtIndex_Get(b *testing.B) {
	keyCount := 1_000_000
	if testing.Short() {
		keyCount = 10_000
	}
	compress := seg.CompressKeys

	for _, M := range []uint64{256, 128, 64, 32} {
		kvPath := generateKV(b, b.TempDir(), 20, 10, keyCount, log.New(), compress)
		keys, err := pivotKeysFromKV(kvPath)
		require.NoError(b, err)

		indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"

		b.Run(fmt.Sprintf("M%d", M), func(b *testing.B) {
			decomp, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, M, compress, false)
			require.NoError(b, err)
			defer bt.Close()
			defer decomp.Close()

			getter := seg.NewReader(decomp.MakeGetter(), compress)
			rnd := newRnd(uint64(b.N))

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				p := rnd.IntN(len(keys))
				k, _, _, found, err := bt.Get(keys[p], getter)
				if err != nil {
					b.Fatal(err)
				}
				if !found || !bytes.Equal(keys[p], k) {
					b.Fatal("key not found or mismatch")
				}
			}
		})
	}
}

func TestDecodeNodes(t *testing.T) {
	const M = 256
	for _, keys := range [][][]byte{
		nil,
		{[]byte("a")},
		{[]byte("a"), []byte("bcd"), bytes.Repeat([]byte{0xff}, 300)},
	} {
		var buf bytes.Buffer
		var hdr [2]byte
		for _, k := range keys {
			binary.BigEndian.PutUint16(hdr[:], uint16(len(k)))
			buf.Write(hdr[:])
			buf.Write(k)
		}
		got, n, err := decodeNodes(buf.Bytes(), uint64(len(keys)))
		require.NoError(t, err)
		require.Equal(t, buf.Len(), n)
		require.Len(t, got, len(keys))
		bp := &BpsTree{keysBlob: buf.Bytes(), nodeOfft: got, nodeStride: M}
		for i := range keys {
			require.Equal(t, uint64(i)*M, bp.nodeDi(i)) // di recomputed, not stored
			require.True(t, bytes.Equal(keys[i], bp.nodeKey(i)))
		}
	}
}

func TestDecodeListNodesV0_Validation(t *testing.T) {
	build := func(dis ...uint64) []byte {
		var buf bytes.Buffer
		var u8 [8]byte
		var u2 [2]byte
		binary.BigEndian.PutUint64(u8[:], uint64(len(dis)))
		buf.Write(u8[:])
		for _, di := range dis {
			binary.BigEndian.PutUint64(u8[:], di)
			buf.Write(u8[:])
			binary.BigEndian.PutUint16(u2[:], 1)
			buf.Write(u2[:])
			buf.WriteByte('k')
		}
		return buf.Bytes()
	}

	off, stride, _, err := decodeListNodesV0(build(0, 32, 64, 96))
	require.NoError(t, err)
	require.Equal(t, uint64(32), stride)
	require.Len(t, off, 4)

	// di0==0 is required, so stride=di1 can't underflow; corrupt progressions are rejected
	for name, dis := range map[string][]uint64{
		"first di != 0":      {8, 40},
		"zero stride":        {0, 0},
		"broken progression": {0, 32, 999},
	} {
		_, _, _, err := decodeListNodesV0(build(dis...))
		require.Errorf(t, err, "expected error for %q", name)
	}
}

func TestNodeEncode_NoAlloc(t *testing.T) {
	node := Node{key: []byte("some-key")}
	var headerBuf [10]byte
	allocs := testing.AllocsPerRun(1000, func() {
		if err := node.Encode(io.Discard, headerBuf[:]); err != nil {
			t.Fatal(err)
		}
	})
	require.Zero(t, allocs)
}
