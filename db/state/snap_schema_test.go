package state

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"
)

func setup(tb testing.TB) datadir.Dirs {
	tb.Helper()
	return datadir.New(tb.TempDir())
}

func TestE2SnapSchema(t *testing.T) {
	t.Parallel()
	dirs := setup(t)
	e2version := NewE2SnapSchemaVersion(version.V1_0_standart, version.V1_0_standart)
	p := NewE2SnapSchema(dirs, "bodies", e2version)

	info, ok := p.Parse("v1.0-000100-000500-bodies.seg")
	require.True(t, ok)

	require.Equal(t, "v1.0-000100-000500-bodies.seg", info.Name)
	require.Equal(t, version.V1_0, info.Version)
	require.Equal(t, uint64(100*1000), info.From)
	require.Equal(t, uint64(500*1000), info.To)
	require.Equal(t, "bodies", info.FileType)
	require.Equal(t, string(DataExtensionSeg), info.Ext)

	info, ok = p.Parse("v1.0-000100-000500-bodies.idx")
	require.True(t, ok)
	require.Equal(t, "v1.0-000100-000500-bodies.idx", info.Name)
	require.Equal(t, version.V1_0, info.Version)
	require.Equal(t, uint64(100*1000), info.From)
	require.Equal(t, uint64(500*1000), info.To)
	require.Equal(t, "bodies", info.FileType)
	require.Equal(t, string(AccessorExtensionIdx), info.Ext)

	_, ok = p.Parse("v1.0-000100-000500-headers.seg")
	require.False(t, ok)

	_, ok = p.Parse("v1.0-000100-000500-bodies.dat")
	require.False(t, ok)

	// for transactions
	p = NewE2SnapSchemaWithIndexTag(dirs, "transactions", []string{"transactions", "transactions-to-block"}, e2version)
	_, ok = p.Parse("v1.0-000100-000500-transactions.seg")
	require.True(t, ok)

	info, ok = p.Parse("v1.0-000100-000500-transactions-to-block.idx")
	require.True(t, ok)
	require.Equal(t, "v1.0-000100-000500-transactions-to-block.idx", info.Name)
	require.Equal(t, version.V1_0, info.Version)
	require.Equal(t, uint64(100*1000), info.From)
	require.Equal(t, uint64(500*1000), info.To)
	require.Equal(t, "transactions-to-block", info.FileType)
	require.Equal(t, string(AccessorExtensionIdx), info.Ext)

	info, ok = p.Parse("v1.0-000100-000500-transactions.idx")
	require.True(t, ok)
	require.Equal(t, "v1.0-000100-000500-transactions.idx", info.Name)
	require.Equal(t, version.V1_0, info.Version)
	require.Equal(t, uint64(100*1000), info.From)
	require.Equal(t, uint64(500*1000), info.To)
	require.Equal(t, "transactions", info.FileType)
	require.Equal(t, string(AccessorExtensionIdx), info.Ext)

	stepFrom, stepTo := RootNum(5*1000), RootNum(20*1000)
	dataFileFull, _ := p.DataFile(version.V1_0, stepFrom, stepTo)
	_, fName := filepath.Split(dataFileFull)
	require.Equal(t, "v1.0-000005-000020-transactions.seg", fName)

	acc1Full, _ := p.AccessorIdxFile(version.V1_0, stepFrom, stepTo, 0)
	_, fName = filepath.Split(acc1Full)
	require.Equal(t, "v1.0-000005-000020-transactions.idx", fName)

	acc2Full, _ := p.AccessorIdxFile(version.V1_0, stepFrom, stepTo, 1)
	_, fName = filepath.Split(acc2Full)
	require.Equal(t, "v1.0-000005-000020-transactions-to-block.idx", fName)

	require.Panics(t, func() {
		p.BtIdxFile(version.V1_0, stepFrom, stepTo)
	})

	require.Panics(t, func() {
		p.ExistenceFile(version.V1_0, stepFrom, stepTo)
	})

	require.True(t, p.AccessorIdxFileMetadata().Supported())
	require.False(t, p.BtIdxFileMetadata().Supported())
	require.False(t, p.ExistenceFileMetadata().Supported())

}

func TestISSet(t *testing.T) {
	t.Parallel()
	require.True(t, DataExtension(".seg").IsSet())
	require.False(t, DataExtension(".dat").IsSet())
}

func TestE3SnapSchemaForDomain1(t *testing.T) {
	t.Parallel()
	// account domain test

	dirs := setup(t)
	ver := version.V1_0_standart
	stepSize := uint64(config3.DefaultStepSize)
	p := NewE3SnapSchemaBuilder(statecfg.AccessorBTree|statecfg.AccessorExistence, stepSize).
		Data(dirs.SnapDomain, "accounts", DataExtensionKv, seg.CompressKeys, ver).
		BtIndex(ver).
		Existence(ver).Build()

	stepFrom := RootNum(stepSize * 288)
	stepTo := RootNum(stepSize * 296)
	info, ok := p.Parse("v1.0-accounts.288-296.bt")
	require.True(t, ok)
	require.Equal(t, "v1.0-accounts.288-296.bt", info.Name)
	require.Equal(t, version.V1_0, info.Version)
	require.Equal(t, uint64(stepFrom), info.From)
	require.Equal(t, uint64(stepTo), info.To)
	require.Equal(t, "accounts", info.FileType)
	require.Equal(t, ".bt", info.Ext)

	info, ok = p.Parse("v1.0-accounts.288-296.kv")
	require.True(t, ok)
	require.Equal(t, "v1.0-accounts.288-296.kv", info.Name)
	require.Equal(t, version.V1_0, info.Version)
	require.Equal(t, uint64(stepFrom), info.From)
	require.Equal(t, uint64(stepTo), info.To)
	require.Equal(t, "accounts", info.FileType)
	require.Equal(t, string(DataExtensionKv), info.Ext)

	info, ok = p.Parse("v1.0-accounts.288-296.kvei")
	require.True(t, ok)
	require.Equal(t, "v1.0-accounts.288-296.kvei", info.Name)
	require.Equal(t, version.V1_0, info.Version)
	require.Equal(t, uint64(stepFrom), info.From)
	require.Equal(t, uint64(stepTo), info.To)
	require.Equal(t, "accounts", info.FileType)
	require.Equal(t, ".kvei", info.Ext)

	dataFileFull, _ := p.DataFile(ver.Current, stepFrom, stepTo)
	_, fName := filepath.Split(dataFileFull)
	require.Equal(t, "v1.0-accounts.288-296.kv", fName)
	accFull, _ := p.BtIdxFile(version.V1_0, stepFrom, stepTo)
	_, fName = filepath.Split(accFull)
	require.Equal(t, "v1.0-accounts.288-296.bt", fName)

	require.Panics(t, func() {
		p.AccessorIdxFile(version.V1_0, stepFrom, stepTo, 0)
	})

	exFull, _ := p.ExistenceFile(ver.Current, stepFrom, stepTo)
	_, fName = filepath.Split(exFull)
	require.Equal(t, "v1.0-accounts.288-296.kvei", fName)

	require.True(t, p.btIdxFileMetadata.supported)
	require.True(t, p.existenceFileMetadata.supported)
	require.False(t, p.indexFileMetadata.supported)
}

func TestE3SnapSchemaForCommitmentDomain(t *testing.T) {
	t.Parallel()
	dirs := setup(t)
	stepSize := uint64(config3.DefaultStepSize)
	ver := version.V1_0_standart
	p := NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
		Data(dirs.SnapDomain, "commitments", DataExtensionKv, seg.CompressKeys, ver).
		Accessor(dirs.SnapDomain, ver).Build()

	stepFrom := RootNum(stepSize * 288)
	stepTo := RootNum(stepSize * 296)
	info, ok := p.Parse("v1.0-commitments.288-296.kv")
	require.True(t, ok)
	require.Equal(t, "v1.0-commitments.288-296.kv", info.Name)
	require.Equal(t, version.V1_0, info.Version)
	require.Equal(t, uint64(stepFrom), info.From)
	require.Equal(t, uint64(stepTo), info.To)
	require.Equal(t, "commitments", info.FileType)
	require.Equal(t, string(DataExtensionKv), info.Ext)

	info, ok = p.Parse("v1.0-commitments.288-296.kvi")
	require.True(t, ok)
	require.Equal(t, "v1.0-commitments.288-296.kvi", info.Name)
	require.Equal(t, version.V1_0, info.Version)
	require.Equal(t, uint64(stepFrom), info.From)
	require.Equal(t, uint64(stepTo), info.To)
	require.Equal(t, "commitments", info.FileType)
	require.Equal(t, string(AccessorExtensionKvi), info.Ext)

	dataFileFull, _ := p.DataFile(ver.Current, stepFrom, stepTo)
	_, fName := filepath.Split(dataFileFull)
	require.Equal(t, "v1.0-commitments.288-296.kv", fName)
	accFull, _ := p.AccessorIdxFile(ver.Current, stepFrom, stepTo, 0)
	_, fName = filepath.Split(accFull)
	require.Equal(t, "v1.0-commitments.288-296.kvi", fName)

	require.Panics(t, func() {
		p.BtIdxFile(version.V1_0, stepFrom, stepTo)
	})
	require.Panics(t, func() {
		p.ExistenceFile(version.V1_0, stepFrom, stepTo)
	})

	require.True(t, p.indexFileMetadata.supported)
	require.False(t, p.existenceFileMetadata.supported)
	require.False(t, p.btIdxFileMetadata.supported)
}

func TestE3SnapSchemaForHistory(t *testing.T) {
	t.Parallel()
	dirs := setup(t)
	stepSize := uint64(config3.DefaultStepSize)
	ver := version.V1_0_standart
	p := NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
		Data(dirs.SnapHistory, "accounts", DataExtensionV, seg.CompressKeys, ver).
		Accessor(dirs.SnapAccessors, ver).Build()

	stepFrom, stepTo := RootNum(stepSize*192), RootNum(stepSize*256)
	info, ok := p.Parse("v1.0-accounts.192-256.v")
	require.True(t, ok)
	require.Equal(t, "v1.0-accounts.192-256.v", info.Name)
	require.Equal(t, version.V1_0, info.Version)
	require.Equal(t, uint64(stepFrom), info.From)
	require.Equal(t, uint64(stepTo), info.To)
	require.Equal(t, "accounts", info.FileType)
	require.Equal(t, string(DataExtensionV), info.Ext)

	info, ok = p.Parse("v1.0-accounts.192-256.vi")
	require.True(t, ok)
	require.Equal(t, "v1.0-accounts.192-256.vi", info.Name)
	require.Equal(t, version.V1_0, info.Version)
	require.Equal(t, uint64(stepFrom), info.From)
	require.Equal(t, uint64(stepTo), info.To)
	require.Equal(t, "accounts", info.FileType)
	require.Equal(t, string(AccessorExtensionVi), info.Ext)

	dataFileFull, _ := p.DataFile(ver.Current, stepFrom, stepTo)
	path, fName := filepath.Split(dataFileFull)
	require.Equal(t, filepath.Clean(path), filepath.Clean(dirs.SnapHistory))
	require.Equal(t, "v1.0-accounts.192-256.v", fName)

	accFull, _ := p.AccessorIdxFile(ver.Current, stepFrom, stepTo, 0)
	path, fName = filepath.Split(accFull)
	require.Equal(t, filepath.Clean(path), filepath.Clean(dirs.SnapAccessors))
	require.Equal(t, "v1.0-accounts.192-256.vi", fName)

	require.Panics(t, func() {
		p.BtIdxFile(version.V1_0, stepFrom, stepTo)
	})

	require.Panics(t, func() {
		p.ExistenceFile(version.V1_0, stepFrom, stepTo)
	})

	require.True(t, p.indexFileMetadata.supported)
	require.False(t, p.btIdxFileMetadata.supported)
	require.False(t, p.existenceFileMetadata.supported)
}

func TestE3SnapSchemaForII(t *testing.T) {
	t.Parallel()
	dirs := setup(t)
	stepSize := uint64(config3.DefaultStepSize)
	ver := version.V1_0_standart
	p := NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
		Data(dirs.SnapIdx, "logaddrs", DataExtensionEf, seg.CompressNone, ver).
		Accessor(dirs.SnapAccessors, ver).Build()

	stepFrom, stepTo := RootNum(stepSize*128), RootNum(stepSize*192)
	info, ok := p.Parse("v1.0-logaddrs.128-192.ef")
	require.True(t, ok)
	require.Equal(t, "v1.0-logaddrs.128-192.ef", info.Name)
	require.Equal(t, version.V1_0, info.Version)
	require.Equal(t, uint64(stepFrom), info.From)
	require.Equal(t, uint64(stepTo), info.To)
	require.Equal(t, "logaddrs", info.FileType)
	require.Equal(t, string(DataExtensionEf), info.Ext)

	info, ok = p.Parse("v1.0-logaddrs.128-192.efi")
	require.True(t, ok)
	require.Equal(t, "v1.0-logaddrs.128-192.efi", info.Name)
	require.Equal(t, version.V1_0, info.Version)
	require.Equal(t, uint64(stepFrom), info.From)
	require.Equal(t, uint64(stepTo), info.To)
	require.Equal(t, "logaddrs", info.FileType)
	require.Equal(t, string(AccessorExtensionEfi), info.Ext)

	_, ok = p.Parse("v1.0-logaddrs.128-192.crazy")
	require.False(t, ok)

	fileName, _ := p.DataFile(ver.Current, stepFrom, stepTo)
	path, fName := filepath.Split(fileName)
	require.Equal(t, filepath.Clean(path), filepath.Clean(dirs.SnapIdx))
	require.Equal(t, "v1.0-logaddrs.128-192.ef", fName)

	accFull, _ := p.AccessorIdxFile(ver.Current, stepFrom, stepTo, 0)
	path, fName = filepath.Split(accFull)
	require.Equal(t, filepath.Clean(path), filepath.Clean(dirs.SnapAccessors))
	require.Equal(t, "v1.0-logaddrs.128-192.efi", fName)

	require.Panics(t, func() {
		p.BtIdxFile(version.V1_0, stepFrom, stepTo)
	})

	require.Panics(t, func() {
		p.ExistenceFile(version.V1_0, stepFrom, stepTo)
	})

	require.True(t, p.indexFileMetadata.supported)
	require.False(t, p.btIdxFileMetadata.supported)
	require.False(t, p.existenceFileMetadata.supported)
}

// TestE3SnapSchema_TxNumNamingConvention pins the v4.0+ raw-txnum
// state-file naming convention. Versions >= TxNumNamingPivot encode
// the file's [startTxNum, endTxNum) range directly in the filename
// (e.g. v4.0-accounts.0-1000.kv covers txnums 0..999). Versions
// below TxNumNamingPivot use the legacy step-indexed naming
// (e.g. v1.0-accounts.0-1.kv covers step 0 only). Both conventions
// remain readable so existing datadirs survive the cutover.
func TestE3SnapSchema_TxNumNamingConvention(t *testing.T) {
	t.Parallel()
	dirs := setup(t)
	const stepSize uint64 = 1000

	// Use a Versions span that covers BOTH conventions: MinSupported
	// = V1_0 (legacy), Current = V4_0 (new txnum convention).
	ver := version.Versions{Current: version.V4_0, MinSupported: version.V1_0}
	p := NewE3SnapSchemaBuilder(statecfg.AccessorBTree|statecfg.AccessorExistence, stepSize).
		Data(dirs.SnapDomain, "accounts", DataExtensionKv, seg.CompressKeys, ver).
		BtIndex(ver).
		Existence(ver).Build()

	// Cover txnums [10000, 11000) — one step with stepSize=1000.
	from := RootNum(10000)
	to := RootNum(11000)

	// --- Writer side: emits v4.0 raw-txnum filenames ---
	dataPath, err := p.DataFile(version.V4_0, from, to)
	require.NoError(t, err)
	_, fName := filepath.Split(dataPath)
	require.Equal(t, "v4.0-accounts.10000-11000.kv", fName,
		"v4.0 writer must emit raw exclusive txnums in the filename, not step indices")

	btPath, err := p.BtIdxFile(version.V4_0, from, to)
	require.NoError(t, err)
	_, fName = filepath.Split(btPath)
	require.Equal(t, "v4.0-accounts.10000-11000.bt", fName)

	exPath, err := p.ExistenceFile(version.V4_0, from, to)
	require.NoError(t, err)
	_, fName = filepath.Split(exPath)
	require.Equal(t, "v4.0-accounts.10000-11000.kvei", fName)

	// --- Writer side: legacy versions still emit step-indexed names ---
	legacyDataPath, err := p.DataFile(version.V1_0, from, to)
	require.NoError(t, err)
	_, fName = filepath.Split(legacyDataPath)
	require.Equal(t, "v1.0-accounts.10-11.kv", fName,
		"legacy v1.0 writer must emit step-indexed filename (10000/1000=10, 11000/1000=11)")

	// --- Parser side: both conventions parse back to the same raw txnum range ---
	info, ok := p.Parse("v4.0-accounts.10000-11000.kv")
	require.True(t, ok)
	require.Equal(t, version.V4_0, info.Version)
	require.Equal(t, uint64(10000), info.From, "v4.0 parser must read raw txnums directly")
	require.Equal(t, uint64(11000), info.To)
	require.Equal(t, "accounts", info.FileType)
	require.Equal(t, string(DataExtensionKv), info.Ext)

	info, ok = p.Parse("v1.0-accounts.10-11.kv")
	require.True(t, ok)
	require.Equal(t, version.V1_0, info.Version)
	require.Equal(t, uint64(10000), info.From,
		"legacy v1.0 parser must multiply step indices by stepSize (10*1000)")
	require.Equal(t, uint64(11000), info.To)
}

// TestE3SnapSchema_TxNumNamingMultiStepRange pins that v4.0 raw-txnum
// naming correctly represents files spanning multiple steps. The
// motivating "variable step sizes" concern: with raw txnums in the
// filename, the file's coverage is unambiguous without knowing
// stepSize at parse time.
func TestE3SnapSchema_TxNumNamingMultiStepRange(t *testing.T) {
	t.Parallel()
	dirs := setup(t)
	const stepSize uint64 = 1000
	ver := version.Versions{Current: version.V4_0, MinSupported: version.V1_0}
	p := NewE3SnapSchemaBuilder(statecfg.AccessorBTree, stepSize).
		Data(dirs.SnapDomain, "storage", DataExtensionKv, seg.CompressKeys, ver).
		BtIndex(ver).Build()

	// 5-step range: txnums [0, 5000) under v4.0 → "0-5000".
	from := RootNum(0)
	to := RootNum(5 * stepSize)

	dataPath, err := p.DataFile(version.V4_0, from, to)
	require.NoError(t, err)
	_, fName := filepath.Split(dataPath)
	require.Equal(t, "v4.0-storage.0-5000.kv", fName)

	// Adjacent file [5000, 10000) shares the boundary "5000" — the
	// natural visual representation of half-open [from, to) intervals.
	from2 := RootNum(5 * stepSize)
	to2 := RootNum(10 * stepSize)
	dataPath2, err := p.DataFile(version.V4_0, from2, to2)
	require.NoError(t, err)
	_, fName = filepath.Split(dataPath2)
	require.Equal(t, "v4.0-storage.5000-10000.kv", fName)

	// Parser round-trip.
	info, ok := p.Parse("v4.0-storage.0-5000.kv")
	require.True(t, ok)
	require.Equal(t, uint64(0), info.From)
	require.Equal(t, uint64(5000), info.To)

	info, ok = p.Parse("v4.0-storage.5000-10000.kv")
	require.True(t, ok)
	require.Equal(t, uint64(5000), info.From)
	require.Equal(t, uint64(10000), info.To)
}
