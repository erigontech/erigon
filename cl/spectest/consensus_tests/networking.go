package consensus_tests

import (
	"io/fs"
	"math/big"
	"testing"

	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/spectest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetCustodyGroups(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {
	var meta struct {
		NodeID            string   `yaml:"node_id"`
		CustodyGroupCount uint64   `yaml:"custody_group_count"`
		Result            []uint64 `yaml:"result"`
	}
	err = spectest.ReadMeta(root, "meta.yaml", &meta)
	require.NoError(t, err)

	// turn big int string to hex string
	bigInt, ok := new(big.Int).SetString(meta.NodeID, 10)
	require.True(t, ok)
	nodeIDBytes := make([]byte, 32)
	bigInt.FillBytes(nodeIDBytes)
	nodeID := enode.ID(nodeIDBytes)

	custodyGroups, err := peerdasutils.GetCustodyGroups(nodeID, meta.CustodyGroupCount)
	require.NoError(t, err)

	assert.Equal(t, meta.Result, custodyGroups)
	return nil
}

func TestComputeColumnsForCustodyGroup(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {
	var meta struct {
		CustodyGroup uint64   `yaml:"custody_group"`
		Result       []uint64 `yaml:"result"`
	}
	err = spectest.ReadMeta(root, "meta.yaml", &meta)
	require.NoError(t, err)

	columns, err := peerdasutils.ComputeColumnsForCustodyGroup(meta.CustodyGroup)
	require.NoError(t, err)

	assert.Equal(t, meta.Result, columns)
	return nil
}
