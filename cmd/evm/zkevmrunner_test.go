package main

import (
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
