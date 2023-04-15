package initial_state_test

import (
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/clparams/initial_state"
	"github.com/stretchr/testify/assert"
)

func TestMainnet(t *testing.T) {
	state, err := initial_state.GetGenesisState(clparams.MainnetNetwork)
	assert.NoError(t, err)
	root, err := state.HashSSZ()
	assert.NoError(t, err)
	assert.Equal(t, libcommon.Hash(root), libcommon.HexToHash("7e76880eb67bbdc86250aa578958e9d0675e64e714337855204fb5abaaf82c2b"))
}

func TestGoerli(t *testing.T) {
	state, err := initial_state.GetGenesisState(clparams.GoerliNetwork)
	assert.NoError(t, err)
	root, err := state.HashSSZ()
	assert.NoError(t, err)
	assert.Equal(t, libcommon.Hash(root), libcommon.HexToHash("895390e92edc03df7096e9f51e51896e8dbe6e7e838180dadbfd869fdd77a659"))
}

func TestSepolia(t *testing.T) {
	state, err := initial_state.GetGenesisState(clparams.SepoliaNetwork)
	assert.NoError(t, err)
	root, err := state.HashSSZ()
	assert.NoError(t, err)
	assert.Equal(t, libcommon.Hash(root), libcommon.HexToHash("fb9afe32150fa39f4b346be2519a67e2a4f5efcd50a1dc192c3f6b3d013d2798"))
}
