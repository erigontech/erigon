package consensustests

import (
	"bytes"
	"fmt"
	"os"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes/clonable"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"

	"gopkg.in/yaml.v2"
)

type unmarshalerMarshalerHashable interface {
	ssz.EncodableSSZ
	ssz.HashableSSZ
	clonable.Clonable
}

type Root struct {
	Root string `yaml:"root"`
}

const rootsFile = "roots.yaml"
const serializedFile = "serialized.ssz_snappy"

func getSSZStaticConsensusTest[T unmarshalerMarshalerHashable](ref T) testFunc {
	return func(context testContext) error {
		rootBytes, err := os.ReadFile(rootsFile)
		if err != nil {
			return err
		}
		root := Root{}
		if err := yaml.Unmarshal(rootBytes, &root); err != nil {
			return err
		}
		expectedRoot := libcommon.HexToHash(root.Root)
		object := ref.Clone().(unmarshalerMarshalerHashable)
		_, isBeaconState := object.(*state.BeaconState)

		snappyEncoded, err := os.ReadFile(serializedFile)
		if err != nil {
			return err
		}
		encoded, err := utils.DecompressSnappy(snappyEncoded)
		if err != nil {
			return err
		}

		if err := object.DecodeSSZWithVersion(encoded, int(context.version)); err != nil && !isBeaconState {
			return err
		}
		haveRoot, err := object.HashSSZ()
		if err != nil {
			return err
		}
		if libcommon.Hash(haveRoot) != expectedRoot {
			return fmt.Errorf("roots mismatch")
		}
		// Cannot test it without a config.
		if isBeaconState {
			return nil
		}
		haveEncoded, err := object.EncodeSSZ(nil)
		if err != nil {
			return err
		}
		if !bytes.Equal(haveEncoded, encoded) {
			return fmt.Errorf("re-encoding mismatch")
		}
		return nil
	}
}
