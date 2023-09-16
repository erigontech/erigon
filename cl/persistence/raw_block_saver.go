package persistence

import (
	"context"
	"io"
	"os"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/spf13/afero"
)

type aferoRawBeaconBlockChain struct {
	fs  afero.Fs
	cfg *clparams.BeaconChainConfig
}

func NewAferoRawBlockSaver(fs afero.Fs, cfg *clparams.BeaconChainConfig) RawBeaconBlockChain {
	return aferoRawBeaconBlockChain{
		fs:  fs,
		cfg: cfg,
	}
}

func AferoRawBeaconBlockChainFromOsPath(cfg *clparams.BeaconChainConfig, path string) RawBeaconBlockChain {
	dataDirFs := afero.NewBasePathFs(afero.NewOsFs(), path)
	return NewAferoRawBlockSaver(dataDirFs, cfg)
}

func (a aferoRawBeaconBlockChain) BlockWriter(ctx context.Context, slot uint64, blockRoot libcommon.Hash) (io.WriteCloser, error) {
	folderPath, path := RootToPaths(slot, blockRoot, a.cfg)
	_ = a.fs.MkdirAll(folderPath, 0o755)
	return a.fs.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o755)
}

func (a aferoRawBeaconBlockChain) BlockReader(ctx context.Context, slot uint64, blockRoot libcommon.Hash) (io.ReadCloser, error) {
	_, path := RootToPaths(slot, blockRoot, a.cfg)
	return a.fs.OpenFile(path, os.O_RDONLY, 0o755)
}

func (a aferoRawBeaconBlockChain) DeleteBlock(ctx context.Context, slot uint64, blockRoot libcommon.Hash) error {
	_, path := RootToPaths(slot, blockRoot, a.cfg)
	return a.fs.Remove(path)
}
