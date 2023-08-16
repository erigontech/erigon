package clpersist

import (
	"fmt"
	"io"
	"os"
	"path"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication/ssz_snappy"
	"github.com/spf13/afero"
)

// SlotToPaths define the file structure to store a block
//
// superEpoch = floor(slot / (epochSize ^ 2))
// epoch =  floot(slot / epochSize)
// file is to be stored at
// "/signedBeaconBlock/{superEpoch}/{epoch}/{slot}.ssz_snappy"
func SlotToPaths(slot uint64, config *clparams.BeaconChainConfig) (folderPath string, filePath string) {

	superEpoch := slot / (config.SlotsPerEpoch * config.SlotsPerEpoch)
	epoch := slot / config.SlotsPerEpoch

	folderPath = path.Clean(fmt.Sprintf("%d/%d", superEpoch, epoch))
	filePath = path.Clean(fmt.Sprintf("%s/%d.sz", folderPath, slot))
	return
}

func SaveBlockWithConfig(
	fs afero.Fs,
	block *cltypes.SignedBeaconBlock,
	config *clparams.BeaconChainConfig,
) error {
	folderPath, path := SlotToPaths(block.Block.Slot, config)
	// ignore this error... reason: windows
	_ = fs.MkdirAll(folderPath, 0o755)
	fp, err := fs.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o755)
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
	return nil
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
