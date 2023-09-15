package persistence

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication/ssz_snappy"
	"github.com/spf13/afero"
)

type beaconChainDatabaseFilesystem struct {
	fs  afero.Fs
	cfg *clparams.BeaconChainConfig

	executionEngine execution_client.ExecutionEngine
}

func NewBeaconChainDatabaseFilesystem(fs afero.Fs, executionEngine execution_client.ExecutionEngine, cfg *clparams.BeaconChainConfig) BeaconChainDatabase {
	return beaconChainDatabaseFilesystem{
		fs:              fs,
		cfg:             cfg,
		executionEngine: executionEngine,
	}
}

func (b beaconChainDatabaseFilesystem) GetRange(tx *sql.Tx, ctx context.Context, from uint64, count uint64) ([]*peers.PeeredObject[*cltypes.SignedBeaconBlock], error) {
	// Retrieve block roots for each ranged slot
	beaconBlockRooots, slots, err := beacon_indicies.ReadBeaconBlockRootsInSlotRange(ctx, tx, from, count)
	if err != nil {
		return nil, err
	}

	if len(beaconBlockRooots) == 0 {
		return nil, nil
	}

	blocks := []*peers.PeeredObject[*cltypes.SignedBeaconBlock]{}
	for idx, blockRoot := range beaconBlockRooots {
		slot := slots[idx]
		_, path := RootToPaths(blockRoot, b.cfg)

		fp, err := b.fs.OpenFile(path, os.O_RDONLY, 0o755)
		if err != nil {
			return nil, err
		}
		defer fp.Close()
		block := cltypes.NewSignedBeaconBlock(b.cfg)
		version := b.cfg.GetCurrentStateVersion(slot / b.cfg.SlotsPerEpoch)
		if err := ssz_snappy.DecodeAndReadNoForkDigest(fp, block, version); err != nil {
			return nil, err
		}

		blocks = append(blocks, &peers.PeeredObject[*cltypes.SignedBeaconBlock]{Data: block})
	}
	return blocks, nil

}

func (b beaconChainDatabaseFilesystem) PurgeRange(tx *sql.Tx, ctx context.Context, from uint64, count uint64) error {
	if err := beacon_indicies.IterateBeaconIndicies(ctx, tx, from, from+count, func(_ uint64, beaconBlockRoot, _, _ libcommon.Hash, _ bool) bool {
		_, path := RootToPaths(beaconBlockRoot, b.cfg)
		_ = b.fs.Remove(path)
		return true
	}); err != nil {
		return err
	}

	if err := beacon_indicies.PruneIndicies(ctx, tx, from, from+count); err != nil {
		return err
	}

	return nil
}

func (b beaconChainDatabaseFilesystem) WriteBlock(tx *sql.Tx, ctx context.Context, block *cltypes.SignedBeaconBlock, canonical bool) error {
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return err
	}
	folderPath, path := RootToPaths(blockRoot, b.cfg)
	// ignore this error... reason: windows
	_ = b.fs.MkdirAll(folderPath, 0o755)
	fp, err := b.fs.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o755)
	if err != nil {
		return err
	}
	defer fp.Close()
	err = fp.Truncate(0)
	if err != nil {
		return err
	}
	_, err = fp.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	err = ssz_snappy.EncodeAndWrite(fp, block)
	if err != nil {
		return err
	}

	err = fp.Sync()
	if err != nil {
		return err
	}

	if err := beacon_indicies.GenerateBlockIndicies(ctx, tx, block, canonical); err != nil {
		return err
	}
	return nil
}

// SlotToPaths define the file structure to store a block
//
// superEpoch = floor(slot / (epochSize ^ 2))
// epoch =  floot(slot / epochSize)
// file is to be stored at
// "/signedBeaconBlock/{superEpoch}/{epoch}/{slot}.ssz_snappy"
func RootToPaths(root libcommon.Hash, config *clparams.BeaconChainConfig) (folderPath string, filePath string) {
	folderPath = path.Clean(fmt.Sprintf("%02x/%02x", root[0], root[1]))
	filePath = path.Clean(fmt.Sprintf("%s/%x.sz", folderPath, root))
	return
}

func ValidateEpoch(fs afero.Fs, epoch uint64, config *clparams.BeaconChainConfig) error {
	superEpoch := epoch / (config.SlotsPerEpoch)

	// the folder path is superEpoch/epoch
	folderPath := path.Clean(fmt.Sprintf("%d/%d", superEpoch, epoch))

	fi, err := afero.ReadDir(fs, folderPath)
	if err != nil {
		return err
	}
	for _, fn := range fi {
		fn.Name()
	}
	return nil
}
