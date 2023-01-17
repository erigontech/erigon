package aura

import (
	"encoding/json"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/consensus/aura/consensusconfig"
	"github.com/ledgerwatch/erigon/params/networkname"
)

func TestGnosisBlockRewardContractTransitions(t *testing.T) {
	config := consensusconfig.GetConfigByChain(networkname.GnosisChainName)
	spec := JsonSpec{}
	require.NoError(t, json.Unmarshal(config, &spec))

	param, err := FromJson(spec)
	require.NoError(t, err)

	require.Equal(t, 2, len(param.BlockRewardContractTransitions))
	assert.Equal(t, uint64(1310), param.BlockRewardContractTransitions[0].blockNum)
	assert.Equal(t, libcommon.HexToAddress("0x867305d19606aadba405ce534e303d0e225f9556"), param.BlockRewardContractTransitions[0].address)
	assert.Equal(t, uint64(9186425), param.BlockRewardContractTransitions[1].blockNum)
	assert.Equal(t, libcommon.HexToAddress("0x481c034c6d9441db23ea48de68bcae812c5d39ba"), param.BlockRewardContractTransitions[1].address)
}

func TestInvalidBlockRewardContractTransition(t *testing.T) {
	config := consensusconfig.GetConfigByChain(networkname.GnosisChainName)
	spec := JsonSpec{}
	require.NoError(t, json.Unmarshal(config, &spec))

	// blockRewardContractTransition should be smaller than any block number in blockRewardContractTransitions
	invalidTransition := uint64(10_000_000)
	spec.BlockRewardContractTransition = &invalidTransition

	_, err := FromJson(spec)
	assert.Error(t, err)
}
