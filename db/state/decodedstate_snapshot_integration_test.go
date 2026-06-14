package state

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	log "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

func writeSnapshotWords(t *testing.T, file string, compression seg.FileCompression, words ...[]byte) {
	t.Helper()

	comp, err := seg.NewCompressor(context.Background(), "decodedstorage-test", file, filepath.Dir(file), seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)

	writer := seg.NewWriter(comp, compression)
	for _, word := range words {
		_, err = writer.Write(word)
		require.NoError(t, err)
	}
	require.NoError(t, writer.Compress())
	writer.Close()
}

func readSnapshotWords(t *testing.T, file string, compression seg.FileCompression, wordCount int) [][]byte {
	t.Helper()

	decomp, err := seg.NewDecompressor(file)
	require.NoError(t, err)
	t.Cleanup(decomp.Close)

	reader := seg.NewReader(decomp.MakeGetter(), compression)
	words := make([][]byte, 0, wordCount)
	for range wordCount {
		word, _ := reader.Next(nil)
		words = append(words, append([]byte(nil), word...))
	}
	return words
}

func TestS_SNAP_05_DecodedStorageSnapshotSchemaProducesRealFrozenFiles(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	stepSize := uint64(config3.DefaultStepSize)
	domain, history, ii := SnapSchemaFromDomainCfg(statecfg.Schema.DecodedStorageDomain, dirs, stepSize)
	from, to := RootNum(stepSize*2), RootNum(stepSize*3)

	domainData, err := domain.DataFile(version.ZeroVersion, from, to)
	require.NoError(t, err)
	domainBT, err := domain.BtIdxFile(version.ZeroVersion, from, to)
	require.NoError(t, err)
	domainExistence, err := domain.ExistenceFile(version.ZeroVersion, from, to)
	require.NoError(t, err)

	historyData, err := history.DataFile(version.ZeroVersion, from, to)
	require.NoError(t, err)
	historyAccessor, err := history.AccessorIdxFile(version.ZeroVersion, from, to, 0)
	require.NoError(t, err)

	iiData, err := ii.DataFile(version.ZeroVersion, from, to)
	require.NoError(t, err)
	iiAccessor, err := ii.AccessorIdxFile(version.ZeroVersion, from, to, 0)
	require.NoError(t, err)

	require.Equal(t, dirs.SnapDomain, filepath.Dir(domainData))
	require.Equal(t, dirs.SnapDomain, filepath.Dir(domainBT))
	require.Equal(t, dirs.SnapDomain, filepath.Dir(domainExistence))
	require.Equal(t, dirs.SnapHistory, filepath.Dir(historyData))
	require.Equal(t, dirs.SnapAccessors, filepath.Dir(historyAccessor))
	require.Equal(t, dirs.SnapIdx, filepath.Dir(iiData))
	require.Equal(t, dirs.SnapAccessors, filepath.Dir(iiAccessor))

	require.Contains(t, filepath.Base(domainData), "decodedstorage.")
	require.Contains(t, filepath.Base(historyData), "decodedstorage.")
	require.Contains(t, filepath.Base(iiData), "decodedstorage.")

	writeSnapshotWords(t, domainData, domain.DataFileCompression(), []byte("key"), []byte("value"))
	words := readSnapshotWords(t, domainData, domain.DataFileCompression(), 2)
	require.Equal(t, [][]byte{[]byte("key"), []byte("value")}, words)
}

func TestS_SNAP_06_DecodedStorageSnapshotSearchResolvesFrozenRangeFiles(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	stepSize := uint64(config3.DefaultStepSize)
	domain, history, ii := SnapSchemaFromDomainCfg(statecfg.Schema.DecodedStorageDomain, dirs, stepSize)
	from, to := RootNum(stepSize*4), RootNum(stepSize*5)

	domainData, err := domain.DataFile(version.ZeroVersion, from, to)
	require.NoError(t, err)
	domainBT, err := domain.BtIdxFile(version.ZeroVersion, from, to)
	require.NoError(t, err)
	domainExistence, err := domain.ExistenceFile(version.ZeroVersion, from, to)
	require.NoError(t, err)
	historyData, err := history.DataFile(version.ZeroVersion, from, to)
	require.NoError(t, err)
	historyAccessor, err := history.AccessorIdxFile(version.ZeroVersion, from, to, 0)
	require.NoError(t, err)
	iiData, err := ii.DataFile(version.ZeroVersion, from, to)
	require.NoError(t, err)
	iiAccessor, err := ii.AccessorIdxFile(version.ZeroVersion, from, to, 0)
	require.NoError(t, err)

	for _, file := range []string{domainData, domainBT, domainExistence, historyData, historyAccessor, iiData, iiAccessor} {
		require.NoError(t, os.WriteFile(file, nil, 0o644))
	}

	resolvedDomainData, err := domain.DataFile(version.SearchVersion, from, to)
	require.NoError(t, err)
	resolvedDomainBT, err := domain.BtIdxFile(version.SearchVersion, from, to)
	require.NoError(t, err)
	resolvedDomainExistence, err := domain.ExistenceFile(version.SearchVersion, from, to)
	require.NoError(t, err)
	resolvedHistoryData, err := history.DataFile(version.SearchVersion, from, to)
	require.NoError(t, err)
	resolvedHistoryAccessor, err := history.AccessorIdxFile(version.SearchVersion, from, to, 0)
	require.NoError(t, err)
	resolvedIIData, err := ii.DataFile(version.SearchVersion, from, to)
	require.NoError(t, err)
	resolvedIIAccessor, err := ii.AccessorIdxFile(version.SearchVersion, from, to, 0)
	require.NoError(t, err)

	require.Equal(t, domainData, resolvedDomainData)
	require.Equal(t, domainBT, resolvedDomainBT)
	require.Equal(t, domainExistence, resolvedDomainExistence)
	require.Equal(t, historyData, resolvedHistoryData)
	require.Equal(t, historyAccessor, resolvedHistoryAccessor)
	require.Equal(t, iiData, resolvedIIData)
	require.Equal(t, iiAccessor, resolvedIIAccessor)
}
