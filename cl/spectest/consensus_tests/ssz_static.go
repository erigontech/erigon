package consensus_tests

import (
	"bytes"
	"io/fs"
	"testing"

	"github.com/ledgerwatch/erigon/spectest"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/persistence/format/snapshot_format"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/clonable"
	"github.com/ledgerwatch/erigon-lib/types/ssz"

	"github.com/ledgerwatch/erigon/cl/utils"
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
		_, isBeaconState := object.(*state.CachingBeaconState)

		snappyEncoded, err := fs.ReadFile(fsroot, serializedFile)
		// fmt.Println("Read snappy encoded:", snappyEncoded, "err:", err)
		require.NoError(t, err)
		encoded, err := utils.DecompressSnappy(snappyEncoded)
		// fmt.Println("Decompressed:", encoded, "err:", err)
		require.NoError(t, err)

		if err := object.DecodeSSZ(encoded, int(c.Version())); err != nil && !isBeaconState {
			return err
		}

		// fmt.Println("Decoded:", object)
		haveRoot, err := object.HashSSZ()
		require.NoError(t, err)
		// fmt.Println("Have root:", haveRoot)
		// fmt.Println("Expected root:", expectedRoot)
		// haveRootHex := hex.EncodeToString(haveRoot[:])
		// fmt.Println("Have root hex:", haveRootHex)
		// fmt.Println("Object:", object)
		// hex_string1 := "42cc09f8351adfdf3aec0de8b968da414c88ccfa01f18634404295c2b3df3b770f1daf70d6f4ffa543efbb315e98177a8260f845547a4d66"
		// fmt.Println("Expected root hex:", len(hex_string1))
		require.EqualValues(t, expectedRoot, haveRoot)
		// Cannot test it without a config.
		if isBeaconState {
			return nil
		}
		haveEncoded, err := object.EncodeSSZ(nil)
		require.NoError(t, err)
		// fmt.Println("Encoded:", haveEncoded)
		require.EqualValues(t, haveEncoded, encoded)
		// Now let it do the encoding in snapshot format
		// fmt.Println("Encoding in snapshot format")
		if blk, ok := object.(*cltypes.SignedBeaconBlock); ok {
			var b bytes.Buffer
			_, err := snapshot_format.WriteBlockForSnapshot(&b, blk, nil)
			require.NoError(t, err)
			var br snapshot_format.MockBlockReader
			if blk.Version() >= clparams.BellatrixVersion {
				br = snapshot_format.MockBlockReader{Block: blk.Block.Body.ExecutionPayload}

			}

			blk2, err := snapshot_format.ReadBlockFromSnapshot(&b, &br, &clparams.MainnetBeaconConfig)
			require.NoError(t, err)

			haveRoot, err := blk2.HashSSZ()
			require.NoError(t, err)
			require.EqualValues(t, expectedRoot, haveRoot)
		}

		return nil
	})
}
