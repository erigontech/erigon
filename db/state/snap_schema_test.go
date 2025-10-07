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
	dirs := setup(t)
	p := NewE2SnapSchema(dirs, "bodies")

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
	p = NewE2SnapSchemaWithIndexTag(dirs, "transactions", []string{"transactions", "transactions-to-block"})
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
	dataFileFull := p.DataFile(version.V1_0, stepFrom, stepTo)
	_, fName := filepath.Split(dataFileFull)
	require.Equal(t, "v1.0-000005-000020-transactions.seg", fName)

	acc1Full := p.AccessorIdxFile(version.V1_0, stepFrom, stepTo, 0)
	_, fName = filepath.Split(acc1Full)
	require.Equal(t, "v1.0-000005-000020-transactions.idx", fName)

	acc2Full := p.AccessorIdxFile(version.V1_0, stepFrom, stepTo, 1)
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
	require.True(t, DataExtension(".seg").IsSet())
	require.False(t, DataExtension(".dat").IsSet())
}

func TestE3SnapSchemaForDomain1(t *testing.T) {
	// account domain test

	dirs := setup(t)
	stepSize := uint64(config3.DefaultStepSize)
	p := NewE3SnapSchemaBuilder(statecfg.AccessorBTree|statecfg.AccessorExistence, stepSize).
		Data(dirs.SnapDomain, "accounts", DataExtensionKv, seg.CompressKeys).
		BtIndex().
		Existence().Build()

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

	dataFileFull := p.DataFile(version.V1_0, stepFrom, stepTo)
	_, fName := filepath.Split(dataFileFull)
	require.Equal(t, "v1.0-accounts.288-296.kv", fName)
	accFull := p.BtIdxFile(version.V1_0, stepFrom, stepTo)
	_, fName = filepath.Split(accFull)
	require.Equal(t, "v1.0-accounts.288-296.bt", fName)

	require.Panics(t, func() {
		p.AccessorIdxFile(version.V1_0, stepFrom, stepTo, 0)
	})

	exFull := p.ExistenceFile(version.V1_0, stepFrom, stepTo)
	_, fName = filepath.Split(exFull)
	require.Equal(t, "v1.0-accounts.288-296.kvei", fName)

	require.True(t, p.btIdxFileMetadata.supported)
	require.True(t, p.existenceFileMetadata.supported)
	require.False(t, p.indexFileMetadata.supported)
}

func TestE3SnapSchemaForCommitmentDomain(t *testing.T) {
	dirs := setup(t)
	stepSize := uint64(config3.DefaultStepSize)
	p := NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
		Data(dirs.SnapDomain, "commitments", DataExtensionKv, seg.CompressKeys).
		Accessor(dirs.SnapDomain).Build()

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

	dataFileFull := p.DataFile(version.V1_0, stepFrom, stepTo)
	_, fName := filepath.Split(dataFileFull)
	require.Equal(t, "v1.0-commitments.288-296.kv", fName)
	accFull := p.AccessorIdxFile(version.V1_0, stepFrom, stepTo, 0)
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
	dirs := setup(t)
	stepSize := uint64(config3.DefaultStepSize)
	p := NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
		Data(dirs.SnapHistory, "accounts", DataExtensionV, seg.CompressKeys).
		Accessor(dirs.SnapAccessors).Build()

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

	dataFileFull := p.DataFile(version.V1_0, stepFrom, stepTo)
	path, fName := filepath.Split(dataFileFull)
	require.Equal(t, filepath.Clean(path), filepath.Clean(dirs.SnapHistory))
	require.Equal(t, "v1.0-accounts.192-256.v", fName)

	accFull := p.AccessorIdxFile(version.V1_0, stepFrom, stepTo, 0)
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
	dirs := setup(t)
	stepSize := uint64(config3.DefaultStepSize)
	p := NewE3SnapSchemaBuilder(statecfg.AccessorHashMap, stepSize).
		Data(dirs.SnapIdx, "logaddrs", DataExtensionEf, seg.CompressNone).
		Accessor(dirs.SnapAccessors).Build()

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

	fileName := p.DataFile(version.V1_0, stepFrom, stepTo)
	path, fName := filepath.Split(fileName)
	require.Equal(t, filepath.Clean(path), filepath.Clean(dirs.SnapIdx))
	require.Equal(t, "v1.0-logaddrs.128-192.ef", fName)

	accFull := p.AccessorIdxFile(version.V1_0, stepFrom, stepTo, 0)
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
