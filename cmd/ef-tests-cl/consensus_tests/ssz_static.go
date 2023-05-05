package consensus_tests

import (
	"io/fs"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes/clonable"
	"github.com/ledgerwatch/erigon/cl/cltypes/ssz"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/ef-tests-cl/spectest"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/stretchr/testify/require"
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

func getSSZStaticConsensusTest[T unmarshalerMarshalerHashable](ref T) spectest.Handler {
	return spectest.HandlerFunc(func(t *testing.T, fsroot fs.FS, c spectest.TestCase) (err error) {
		rootBytes, err := fs.ReadFile(fsroot, rootsFile)
		require.NoError(t, err)
		root := Root{}
		err = yaml.Unmarshal(rootBytes, &root)
		require.NoError(t, err)
		expectedRoot := libcommon.HexToHash(root.Root)
		object := ref.Clone().(unmarshalerMarshalerHashable)
		_, isBeaconState := object.(*state.BeaconState)

		snappyEncoded, err := fs.ReadFile(fsroot, serializedFile)
		require.NoError(t, err)
		encoded, err := utils.DecompressSnappy(snappyEncoded)
		require.NoError(t, err)
		if err := object.DecodeSSZ(encoded, int(c.Version())); err != nil && !isBeaconState {
			return err
		}
		haveRoot, err := object.HashSSZ()
		require.NoError(t, err)
		require.EqualValues(t, haveRoot, expectedRoot)
		// Cannot test it without a config.
		// TODO: parse and use config
		if isBeaconState {
			return nil
		}
		haveEncoded, err := object.EncodeSSZ(nil)
		require.NoError(t, err)
		require.EqualValues(t, haveEncoded, encoded)
		return nil
	})
}
