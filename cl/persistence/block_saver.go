package persistence

import (
	"context"
	"database/sql"
	"fmt"
	"io"
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
	rawDB RawBeaconBlockChain
	cfg   *clparams.BeaconChainConfig

	executionEngine execution_client.ExecutionEngine
}

func NewBeaconChainDatabaseFilesystem(rawDB RawBeaconBlockChain, executionEngine execution_client.ExecutionEngine, cfg *clparams.BeaconChainConfig) BeaconChainDatabase {
	return beaconChainDatabaseFilesystem{
		rawDB:           rawDB,
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

		r, err := b.rawDB.BlockReader(ctx, slot, blockRoot)
		if err != nil {
			return nil, err
		}
		defer r.Close()

		block := cltypes.NewSignedBeaconBlock(b.cfg)
		version := b.cfg.GetCurrentStateVersion(slot / b.cfg.SlotsPerEpoch)
		if err := ssz_snappy.DecodeAndReadNoForkDigest(r, block, version); err != nil {
			return nil, err
		}

		blocks = append(blocks, &peers.PeeredObject[*cltypes.SignedBeaconBlock]{Data: block})
	}
	return blocks, nil

}

func (b beaconChainDatabaseFilesystem) PurgeRange(tx *sql.Tx, ctx context.Context, from uint64, count uint64) error {
	if err := beacon_indicies.IterateBeaconIndicies(ctx, tx, from, from+count, func(slot uint64, beaconBlockRoot, _, _ libcommon.Hash, _ bool) bool {
		b.rawDB.DeleteBlock(ctx, slot, beaconBlockRoot)
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

	w, err := b.rawDB.BlockWriter(ctx, block.Block.Slot, blockRoot)
	if err != nil {
		return err
	}
	defer w.Close()

	if fp, ok := w.(afero.File); ok {
		err = fp.Truncate(0)
		if err != nil {
			return err
		}
		_, err = fp.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}
	}

	err = ssz_snappy.EncodeAndWrite(w, block)
	if err != nil {
		return err
	}

	if fp, ok := w.(afero.File); ok {
		err = fp.Sync()
		if err != nil {
			return err
		}
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
// "/signedBeaconBlock/{superEpoch}/{epoch}/{root}.ssz_snappy"
func RootToPaths(slot uint64, root libcommon.Hash, config *clparams.BeaconChainConfig) (folderPath string, filePath string) {
	folderPath = path.Clean(fmt.Sprintf("%d/%d", slot/(config.SlotsPerEpoch*config.SlotsPerEpoch), slot/config.SlotsPerEpoch))
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
