package main

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
)

func TestLoadEngineXGroupsReadsEachTestBack(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.json"), []byte(`{
		"one": {"network":"Prague","preHash":"0x01","lastblockhash":"0x000000000000000000000000000000000000000000000000000000000000000a","engineNewPayloads":[{"params":[{"x":1}],"newPayloadVersion":"4"}]},
		"two": {"network":"Prague","preHash":"0x02","lastblockhash":"0x000000000000000000000000000000000000000000000000000000000000000b"}
	}`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b.json"), []byte(`{"three": {"network":"Prague","preHash":"0x01","lastblockhash":"0x000000000000000000000000000000000000000000000000000000000000000c"}}`), 0o600))

	groups, total, err := loadEngineXGroups(dir, regexp.MustCompile(""))
	require.NoError(t, err)
	require.Equal(t, 3, total)
	require.Len(t, groups, 2)

	lastBlocks := make(map[string]common.Hash)
	for key, tests := range groups {
		for _, test := range tests {
			def, err := test.definition()
			require.NoError(t, err)
			require.Equal(t, key, engineXGroupKey{fork: def.Fork, hash: def.PreAllocHash})
			lastBlocks[test.name] = *def.LastBlockHash
			if test.name == "one" {
				require.Len(t, def.NewPayloads, 1)
				require.JSONEq(t, `{"x":1}`, string(def.NewPayloads[0].Params[0]))
			}
		}
	}
	require.Equal(t, map[string]common.Hash{
		"one":   common.HexToHash("0x0a"),
		"two":   common.HexToHash("0x0b"),
		"three": common.HexToHash("0x0c"),
	}, lastBlocks)
	require.Len(t, groups[engineXGroupKey{fork: engineapitester.Fork("Prague"), hash: engineapitester.PreAllocHash("0x01")}], 2)
}
