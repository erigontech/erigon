package state

import (
	"bytes"
	"context"
	"path"
	"path/filepath"
	"sort"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/compress"
)

func TestArchiveWriter(t *testing.T) {
	tmp := t.TempDir()
	logger := log.New()

	td := generateTestData(t, 20, 52, 1, 1, 100000)

	openWriter := func(t testing.TB, tmp, name string, compFlags FileCompression) ArchiveWriter {
		t.Helper()
		file := filepath.Join(tmp, name)
		comp, err := compress.NewCompressor(context.Background(), "", file, tmp, 100, 1, log.LvlDebug, logger)
		require.NoError(t, err)
		return NewArchiveWriter(comp, compFlags)
	}
	keys := make([][]byte, 0, len(td))
	for k := range td {
		keys = append(keys, []byte(k))
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })

	writeLatest := func(t testing.TB, w ArchiveWriter, td map[string][]upd) {
		t.Helper()

		for _, k := range keys {
			upd := td[string(k)]

			err := w.AddWord(k)
			require.NoError(t, err)
			err = w.AddWord(upd[0].value)
			require.NoError(t, err)
		}
		err := w.Compress()
		require.NoError(t, err)
	}

	checkLatest := func(t testing.TB, g ArchiveGetter, td map[string][]upd) {
		t.Helper()

		for _, k := range keys {
			upd := td[string(k)]

			fk, _ := g.Next(nil)
			fv, _ := g.Next(nil)
			require.EqualValues(t, k, fk)
			require.EqualValues(t, upd[0].value, fv)
		}
	}

	t.Run("Uncompressed", func(t *testing.T) {
		w := openWriter(t, tmp, "uncompressed", CompressNone)
		writeLatest(t, w, td)
		w.Close()

		decomp, err := compress.NewDecompressor(path.Join(tmp, "uncompressed"))
		require.NoError(t, err)
		defer decomp.Close()

		ds := (datasize.B * datasize.ByteSize(decomp.Size())).HR()
		t.Logf("keys %d, fsize %v compressed fully", len(keys), ds)

		r := NewArchiveGetter(decomp.MakeGetter(), CompressNone)
		checkLatest(t, r, td)
	})
	t.Run("Compressed", func(t *testing.T) {
		w := openWriter(t, tmp, "compressed", CompressKeys|CompressVals)
		writeLatest(t, w, td)
		w.Close()

		decomp, err := compress.NewDecompressor(path.Join(tmp, "compressed"))
		require.NoError(t, err)
		defer decomp.Close()
		ds := (datasize.B * datasize.ByteSize(decomp.Size())).HR()
		t.Logf("keys %d, fsize %v compressed fully", len(keys), ds)

		r := NewArchiveGetter(decomp.MakeGetter(), CompressKeys|CompressVals)
		checkLatest(t, r, td)
	})

	t.Run("Compressed Keys", func(t *testing.T) {
		w := openWriter(t, tmp, "compressed-keys", CompressKeys)
		writeLatest(t, w, td)
		w.Close()

		decomp, err := compress.NewDecompressor(path.Join(tmp, "compressed-keys"))
		require.NoError(t, err)
		defer decomp.Close()
		ds := (datasize.B * datasize.ByteSize(decomp.Size())).HR()
		t.Logf("keys %d, fsize %v compressed keys", len(keys), ds)

		r := NewArchiveGetter(decomp.MakeGetter(), CompressKeys)
		checkLatest(t, r, td)
	})

	t.Run("Compressed Vals", func(t *testing.T) {
		w := openWriter(t, tmp, "compressed-vals", CompressVals)
		writeLatest(t, w, td)
		w.Close()

		decomp, err := compress.NewDecompressor(path.Join(tmp, "compressed-vals"))
		require.NoError(t, err)
		defer decomp.Close()
		ds := (datasize.B * datasize.ByteSize(decomp.Size())).HR()
		t.Logf("keys %d, fsize %v compressed vals", len(keys), ds)

		r := NewArchiveGetter(decomp.MakeGetter(), CompressVals)
		checkLatest(t, r, td)
	})

}
