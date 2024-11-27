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
	"fmt"
	"io/fs"
	"os"
	"testing"

	"github.com/erigontech/erigon/spectest"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ForksFork = spectest.HandlerFunc(func(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {

	preState, err := spectest.ReadBeaconState(root, c.Version()-1, spectest.PreSsz)
	require.NoError(t, err)

	postState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PostSsz)
	expectedError := os.IsNotExist(err)
	if !expectedError {
		require.NoError(t, err)
	}
	switch preState.Version() {
	case clparams.Phase0Version:
		err = preState.UpgradeToAltair()
	case clparams.AltairVersion:
		err = preState.UpgradeToBellatrix()
	case clparams.BellatrixVersion:
		err = preState.UpgradeToCapella()
	case clparams.CapellaVersion:
		err = preState.UpgradeToDeneb()
	case clparams.DenebVersion:
		err = preState.UpgradeToElectra()
	default:
		err = spectest.ErrHandlerNotImplemented(fmt.Sprintf("block state %v", preState.Version()))
	}
	if expectedError {
		assert.Error(t, err)
	}

	haveRoot, err := preState.HashSSZ()
	assert.NoError(t, err)

	expectedRoot, err := postState.HashSSZ()
	assert.NoError(t, err)
	assert.EqualValues(t, haveRoot, expectedRoot, "state root")

	return nil
})

/*
func print2(pre, post *state.CachingBeaconState) string {
	preHash, err := pre.ValidatorSet().HashSSZ()
	if err != nil {
		return fmt.Sprintf("pre hash err %v", err)
	}
	postHash, err := post.ValidatorSet().HashSSZ()
	if err != nil {
		return fmt.Sprintf("post hash err %v", err)
	}
	if preHash != postHash {
		return fmt.Sprintf("vs pre hash %v != post hash %v", preHash, postHash)
	}

	if pre.GetEarlistExitEpoch() != post.GetEarlistExitEpoch() {
		return fmt.Sprintf("earlistExitEpoch pre %d != post %d", pre.GetEarlistExitEpoch(), post.GetEarlistExitEpoch())
	}

	if pre.GetEarlistConsolidationEpoch() != post.GetEarlistConsolidationEpoch() {
		return fmt.Sprintf("earlistConsEpoch pre %d != post %d", pre.GetEarlistConsolidationEpoch(), post.GetEarlistConsolidationEpoch())
	}

	if pre.GetExitBalanceToConsume() != post.GetExitBalanceToConsume() {
		return fmt.Sprintf("exitBalance pre %d != post %d", pre.GetExitBalanceToConsume(), post.GetExitBalanceToConsume())
	}

	if pre.GetConsolidationBalanceToConsume() != post.GetConsolidationBalanceToConsume() {
		return fmt.Sprintf("consolidationBalance pre %d != post %d", pre.GetConsolidationBalanceToConsume(), post.GetConsolidationBalanceToConsume())
	}

	hash1, err := pre.GetPendingDeposits().HashSSZ()
	if err != nil {
		return fmt.Sprintf("pre hash err %v", err)
	}
	hash2, err := post.GetPendingDeposits().HashSSZ()
	if err != nil {
		return fmt.Sprintf("post hash err %v", err)
	}
	if hash1 != hash2 {
		return fmt.Sprintf("pre hash %v != post hash %v", hash1, hash2)
	}
	return "ok"
}

func print(pre, post *state.CachingBeaconState) string {
	d1 := []solid.PendingDeposit{}
	deposits := pre.GetPendingDeposits()
	deposits.Range(func(index int, value *solid.PendingDeposit, length int) bool {
		d1 = append(d1, *value)
		return true
	})
	//fmt.Println("pre", d)

	d2 := []solid.PendingDeposit{}
	deposits = post.GetPendingDeposits()
	deposits.Range(func(index int, value *solid.PendingDeposit, length int) bool {
		d2 = append(d2, *value)
		return true
	})
	//fmt.Println("post", d)
	if len(d1) != len(d2) {
		return fmt.Sprintf("pre len %d != post len %d", len(d1), len(d2))
	}
	for i := range d1 {
		if d1[i] != d2[i] {
			return fmt.Sprintf("pre %v != post %v", d1[i], d2[i])
		}
	}
	return "ok"
}
*/
