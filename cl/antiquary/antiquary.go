package antiquary

import (
	"context"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/clpersist"
	"github.com/spf13/afero"
)

type Downloader struct {
	fs     afero.Fs
	source clpersist.BlockSource
	config *clparams.BeaconChainConfig
}

func NewDownloader(
	fs afero.Fs,
	source clpersist.BlockSource,
	config *clparams.BeaconChainConfig,
) *Downloader {
	return &Downloader{
		fs:     fs,
		source: source,
		config: config,
	}
}

func (d *Downloader) DownloadEpoch(ctx context.Context, epoch uint64) error {
	// convert the epoch to a block
	startBlock := epoch * d.config.SlotsPerEpoch
	blocks, err := d.source.GetRange(ctx, startBlock, d.config.SlotsPerEpoch)
	if err != nil {
		return err
	}
	// NOTE: the downloader does not perform any real verification on these blocks
	// validation must be done separately
	for _, v := range blocks {
		err := clpersist.SaveBlockWithConfig(d.fs, v.Data, d.config)
		if err != nil {
			return err
		}
	}

	return nil
}
