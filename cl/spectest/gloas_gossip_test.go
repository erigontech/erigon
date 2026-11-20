// Copyright 2026 The Erigon Authors
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

package spectest

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/spectest/consensus_tests"
	"github.com/erigontech/erigon/cl/spectest/spectest"
	"github.com/erigontech/erigon/cl/utils"
)

// testGloasGossipTimeRewind replays a valid payload attestation after a later message, with the
// fixture clock moving backwards in between.
func testGloasGossipTimeRewind(t *testing.T) {
	files, meta := readGloasPayloadAttestationValidFixture(t)
	valid := meta["messages"].([]any)[0].(map[string]any)
	late := map[string]any{"current_time_ms": 36100, "message": valid["message"], "expected": "ignore"}
	meta["messages"] = []any{late, valid}
	runGloasPayloadAttestationFixture(t, files, meta)
}

// testGloasGossipFailedBlockFloor appends a setup block that fails import at a later slot; the
// valid payload attestation must still be accepted at its original time.
func testGloasGossipFailedBlockFloor(t *testing.T) {
	files, meta := readGloasPayloadAttestationValidFixture(t)
	blocks := meta["blocks"].([]any)
	parentName := blocks[len(blocks)-1].(map[string]any)["block"].(string)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	require.NoError(t, spectest.ReadSsz(files, clparams.GloasVersion, parentName+".ssz_snappy", block))
	block.Block.Slot += 2
	data, err := utils.EncodeSSZSnappy(block)
	require.NoError(t, err)
	files["block_failed.ssz_snappy"] = &fstest.MapFile{Data: data}
	meta["blocks"] = append(blocks, map[string]any{"block": "block_failed", "failed": true})
	runGloasPayloadAttestationFixture(t, files, meta)
}

var gloasPayloadAttestationValidCase = spectest.TestCase{
	ConfigName:    "mainnet",
	ForkPhaseName: "gloas",
	RunnerName:    "networking",
	HandlerName:   "gossip_payload_attestation_message",
	SuiteName:     "pyspec_tests",
	CaseName:      "gossip_payload_attestation_message__valid",
}

func readGloasPayloadAttestationValidFixture(t *testing.T) (fstest.MapFS, map[string]any) {
	c := gloasPayloadAttestationValidCase
	root := os.DirFS(filepath.Join(mainnetDir, c.ConfigName, c.ForkPhaseName, c.RunnerName, c.HandlerName, c.SuiteName, c.CaseName))
	files := fstest.MapFS{}
	require.NoError(t, fs.WalkDir(root, ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := fs.ReadFile(root, name)
		files[name] = &fstest.MapFile{Data: data}
		return err
	}))

	var meta map[string]any
	require.NoError(t, yaml.Unmarshal(files["meta.yaml"].Data, &meta))
	messages := meta["messages"].([]any)
	require.Len(t, messages, 1)
	valid := messages[0].(map[string]any)
	require.Equal(t, 12100, valid["current_time_ms"])
	require.Equal(t, "valid", valid["expected"])
	return files, meta
}

func runGloasPayloadAttestationFixture(t *testing.T, files fstest.MapFS, meta map[string]any) {
	data, err := yaml.Marshal(meta)
	require.NoError(t, err)
	files["meta.yaml"] = &fstest.MapFile{Data: data}

	c := gloasPayloadAttestationValidCase
	handler, err := consensus_tests.TestFormats[c.RunnerName].GetHandler(c.HandlerName)
	require.NoError(t, err)
	require.NoError(t, handler.Run(t, files, c))
}
