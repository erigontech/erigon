package antiquary

import (
	"context"
	"database/sql"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/persistence"
)

type Downloader struct {
	source    persistence.BlockSource
	config    *clparams.BeaconChainConfig
	beacondDB persistence.BeaconChainDatabase
}

func NewDownloader(
	beacondDB persistence.BeaconChainDatabase,
	source persistence.BlockSource,
	config *clparams.BeaconChainConfig,
) *Downloader {
	return &Downloader{
		beacondDB: beacondDB,
		source:    source,
		config:    config,
	}
}

func (d *Downloader) DownloadEpoch(tx *sql.Tx, ctx context.Context, epoch uint64) error {
	// convert the epoch to a block
	startBlock := epoch * d.config.SlotsPerEpoch
	blocks, err := d.source.GetRange(tx, ctx, startBlock, d.config.SlotsPerEpoch)
	if err != nil {
		return err
	}
	// NOTE: the downloader does not perform any real verification on these blocks
	// validation must be done separately
	for _, v := range blocks {
		err := d.beacondDB.WriteBlock(tx, ctx, v.Data, true)
		if err != nil {
			return err
		}
	}

	return nil
}
