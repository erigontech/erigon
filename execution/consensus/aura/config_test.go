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

package aura

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func TestGnosisBlockRewardContractTransitions(t *testing.T) {
	spec := chainspec.Gnosis.Config.Aura

	param, err := FromJson(spec)
	require.NoError(t, err)

	require.Len(t, param.BlockRewardContractTransitions, 2)
	assert.Equal(t, uint64(1310), param.BlockRewardContractTransitions[0].blockNum)
	assert.Equal(t, common.HexToAddress("0x867305d19606aadba405ce534e303d0e225f9556"), param.BlockRewardContractTransitions[0].address)
	assert.Equal(t, uint64(9186425), param.BlockRewardContractTransitions[1].blockNum)
	assert.Equal(t, common.HexToAddress("0x481c034c6d9441db23ea48de68bcae812c5d39ba"), param.BlockRewardContractTransitions[1].address)
}

func TestInvalidBlockRewardContractTransition(t *testing.T) {
	spec := *(chainspec.Gnosis.Config.Aura)

	// blockRewardContractTransition should be smaller than any block number in blockRewardContractTransitions
	invalidTransition := uint64(10_000_000)
	spec.BlockRewardContractTransition = &invalidTransition

	_, err := FromJson(&spec)
	assert.Error(t, err)
}
