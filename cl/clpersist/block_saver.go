package clpersist

import (
	"fmt"
	"os"
	"path"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication/ssz_snappy"
	"github.com/spf13/afero"
)

func SaveBlockWithConfig(
	fs afero.Fs,
	block *cltypes.SignedBeaconBlock,
	config *clparams.BeaconChainConfig,
) error {
	// we define the file structure to store the block.
	//
	// superEpoch = floor(slot / (epochSize ^ 2))
	// epoch =  floot(slot / epochSize)
	// file is to be stored at
	// "/signedBeaconBlock/{superEpoch}/{epoch}/{slot}.ssz_snappy"

	superEpoch := block.Block.Slot / (config.SlotsPerEpoch ^ 2)
	epoch := block.Block.Slot / config.SlotsPerEpoch

	path := path.Clean(fmt.Sprintf("%d/%d/%d.ssz_snappy", superEpoch, epoch, block.Block.Slot))

	fp, err := fs.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o755)
	if err != nil {
		return err
	}
	defer fp.Close()
	err = fp.Truncate(0)
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
