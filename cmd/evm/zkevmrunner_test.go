package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

func TestLoadZkevmWitnessesUsesValidMinimalReference(t *testing.T) {
	hash := common.HexToHash("0x01")
	success := "0x" + strings.Repeat("00", 32) + "01" + strings.Repeat("00", 10)
	failure := "0x" + strings.Repeat("00", 43)
	path := filepath.Join(t.TempDir(), "fixtures.json")
	data := fmt.Sprintf(`{
		"unused": {"network":"Amsterdam","blocks":[{"blockHeader":{"hash":%q},"statelessOutputBytes":%q,"executionWitness":{"state":["0x01","0x02"]}}]},
		"missing": {"network":"Amsterdam","blocks":[{"blockHeader":{"hash":%q},"statelessOutputBytes":%q,"executionWitness":{"state":[]}}]},
		"complete": {"network":"Amsterdam","blocks":[{"blockHeader":{"hash":%q},"statelessOutputBytes":%q,"executionWitness":{"state":["0x01"]}}]}
	}`, hash, success, hash, failure, hash, success)
	require.NoError(t, os.WriteFile(path, []byte(data), 0o600))
	witnesses, err := loadZkevmWitnesses([]string{path})
	require.NoError(t, err)
	require.Len(t, witnesses, 1)
	require.Equal(t, []hexutil.Bytes{{1}}, witnesses[zkevmWitnessKey{"Amsterdam", hash}].State)
}

func TestLoadZkevmWitnessesDropsReferenceEqualToEveryFixture(t *testing.T) {
	hash := common.HexToHash("0x01")
	success := "0x" + strings.Repeat("00", 32) + "01" + strings.Repeat("00", 10)
	path := filepath.Join(t.TempDir(), "fixtures.json")
	data := fmt.Sprintf(`{
		"a": {"network":"Amsterdam","blocks":[{"blockHeader":{"hash":%q},"statelessOutputBytes":%q,"executionWitness":{"state":["0x01","0x02"]}}]},
		"b": {"network":"Amsterdam","blocks":[{"blockHeader":{"hash":%q},"statelessOutputBytes":%q,"executionWitness":{"state":["0x02","0x01"]}}]}
	}`, hash, success, hash, success)
	require.NoError(t, os.WriteFile(path, []byte(data), 0o600))
	witnesses, err := loadZkevmWitnesses([]string{path})
	require.NoError(t, err)
	reference, ok := witnesses[zkevmWitnessKey{"Amsterdam", hash}]
	require.True(t, ok)
	require.Nil(t, reference)
}

func TestDecodeFixturesRejectsTrailingData(t *testing.T) {
	for _, trailer := range []string{` {"second":{}}`, ` garbage`} {
		path := filepath.Join(t.TempDir(), "fixtures.json")
		require.NoError(t, os.WriteFile(path, []byte(`{"first":{}}`+trailer), 0o600))
		require.Error(t, decodeFixtures(path, func(string, json.RawMessage) error { return nil }), trailer)
	}
}

func TestDecodeFixturesRejectsDuplicateNames(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fixtures.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"a":{},"a":{}}`), 0o600))
	require.Error(t, decodeFixtures(path, func(string, json.RawMessage) error { return nil }))
}

func TestLoadZkevmWitnessesKeepsReferenceWhenExcludedFixtureDiffers(t *testing.T) {
	hash := common.HexToHash("0x01")
	success := "0x" + strings.Repeat("00", 32) + "01" + strings.Repeat("00", 10)
	dir := t.TempDir()
	valid := filepath.Join(dir, "valid.json")
	require.NoError(t, os.WriteFile(valid, []byte(fmt.Sprintf(`{
		"a": {"network":"Amsterdam","blocks":[{"blockHeader":{"hash":%q},"statelessOutputBytes":%q,"executionWitness":{"state":["0x01"]}}]}
	}`, hash, success)), 0o600))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "witness_validation_state"), 0o700))
	excluded := filepath.Join(dir, "witness_validation_state", "state_missing_.json")
	require.NoError(t, os.WriteFile(excluded, []byte(fmt.Sprintf(`{
		"b": {"network":"Amsterdam","blocks":[{"blockHeader":{"hash":%q},"executionWitness":{"state":[]}}]}
	}`, hash)), 0o600))
	witnesses, err := loadZkevmWitnesses([]string{valid, excluded})
	require.NoError(t, err)
	require.Equal(t, []hexutil.Bytes{{1}}, witnesses[zkevmWitnessKey{"Amsterdam", hash}].State)
}
