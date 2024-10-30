// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package consensus_tests

import (
	"bytes"
	"encoding/json"
	"io/fs"
	"testing"

	"github.com/erigontech/erigon/v3/cl/clparams"
	"github.com/erigontech/erigon/v3/cl/cltypes"
	"github.com/erigontech/erigon/v3/cl/cltypes/solid"
	"github.com/erigontech/erigon/v3/cl/persistence/format/snapshot_format"
	"github.com/erigontech/erigon/v3/spectest"

	"github.com/erigontech/erigon/v3/cl/phase1/core/state"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types/clonable"
	"github.com/erigontech/erigon-lib/types/ssz"

	"github.com/erigontech/erigon/v3/cl/utils"
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
		require.NoError(t, err)
		encoded, err := utils.DecompressSnappy(snappyEncoded)
		require.NoError(t, err)

		if err := object.DecodeSSZ(encoded, int(c.Version())); err != nil && !isBeaconState {
			return err
		}

		haveRoot, err := object.HashSSZ()
		require.NoError(t, err)
		require.EqualValues(t, expectedRoot, haveRoot)
		// Cannot test it without a config.
		if isBeaconState {
			return nil
		}
		haveEncoded, err := object.EncodeSSZ(nil)
		require.NoError(t, err)
		require.EqualValues(t, haveEncoded, encoded)
		// Now let it do the encoding in snapshot format
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
		if _, ok := object.(*solid.Checkpoint); ok {
			return nil
		}
		if _, ok := object.(*solid.AttestationData); ok {
			return nil
		}
		if _, ok := object.(solid.Validator); ok {
			return nil
		}

		obj2 := object.Clone()
		// test json
		jsonBlock, err := json.Marshal(object)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(jsonBlock, obj2))

		haveRoot, err = obj2.(unmarshalerMarshalerHashable).HashSSZ()
		require.NoError(t, err)
		require.Equal(t, expectedRoot, libcommon.Hash(haveRoot))

		return nil
	})
}
