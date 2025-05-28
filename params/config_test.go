// Copyright 2017 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package params

import (
	"math/big"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
)

func TestCheckCompatible(t *testing.T) {
	type test struct {
		stored, new *chain.Config
		head        uint64
		wantErr     *chain.ConfigCompatError
	}
	tests := []test{
		{stored: chain.AllProtocolChanges, new: chain.AllProtocolChanges, head: 0, wantErr: nil},
		{stored: chain.AllProtocolChanges, new: chain.AllProtocolChanges, head: 100, wantErr: nil},
		{
			stored:  &chain.Config{TangerineWhistleBlock: big.NewInt(10)},
			new:     &chain.Config{TangerineWhistleBlock: big.NewInt(20)},
			head:    9,
			wantErr: nil,
		},
		{
			stored: chain.AllProtocolChanges,
			new:    &chain.Config{HomesteadBlock: nil},
			head:   3,
			wantErr: &chain.ConfigCompatError{
				What:         "Homestead fork block",
				StoredConfig: big.NewInt(0),
				NewConfig:    nil,
				RewindTo:     0,
			},
		},
		{
			stored: chain.AllProtocolChanges,
			new:    &chain.Config{HomesteadBlock: big.NewInt(1)},
			head:   3,
			wantErr: &chain.ConfigCompatError{
				What:         "Homestead fork block",
				StoredConfig: big.NewInt(0),
				NewConfig:    big.NewInt(1),
				RewindTo:     0,
			},
		},
		{
			stored: &chain.Config{HomesteadBlock: big.NewInt(30), TangerineWhistleBlock: big.NewInt(10)},
			new:    &chain.Config{HomesteadBlock: big.NewInt(25), TangerineWhistleBlock: big.NewInt(20)},
			head:   25,
			wantErr: &chain.ConfigCompatError{
				What:         "Tangerine Whistle fork block",
				StoredConfig: big.NewInt(10),
				NewConfig:    big.NewInt(20),
				RewindTo:     9,
			},
		},
		{
			stored:  &chain.Config{ConstantinopleBlock: big.NewInt(30)},
			new:     &chain.Config{ConstantinopleBlock: big.NewInt(30), PetersburgBlock: big.NewInt(30)},
			head:    40,
			wantErr: nil,
		},
		{
			stored: &chain.Config{ConstantinopleBlock: big.NewInt(30)},
			new:    &chain.Config{ConstantinopleBlock: big.NewInt(30), PetersburgBlock: big.NewInt(31)},
			head:   40,
			wantErr: &chain.ConfigCompatError{
				What:         "Petersburg fork block",
				StoredConfig: nil,
				NewConfig:    big.NewInt(31),
				RewindTo:     30,
			},
		},
	}

	for _, test := range tests {
		err := test.stored.CheckCompatible(test.new, test.head)
		if !reflect.DeepEqual(err, test.wantErr) {
			t.Errorf("error mismatch:\nstored: %v\nnew: %v\nhead: %v\nerr: %v\nwant: %v", test.stored, test.new, test.head, err, test.wantErr)
		}
	}
}

func TestGetBurntContract(t *testing.T) {
	// Ethereum
	assert.Nil(t, MainnetChainConfig.GetBurntContract(0))
	assert.Nil(t, MainnetChainConfig.GetBurntContract(10_000_000))

	// Gnosis Chain
	addr := GnosisChainConfig.GetBurntContract(19_040_000)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x6BBe78ee9e474842Dbd4AB4987b3CeFE88426A92"), *addr)
	addr = GnosisChainConfig.GetBurntContract(19_040_001)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x6BBe78ee9e474842Dbd4AB4987b3CeFE88426A92"), *addr)

	// Bor Mainnet
	addr = BorMainnetChainConfig.GetBurntContract(23850000)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x70bcA57F4579f58670aB2d18Ef16e02C17553C38"), *addr)
	addr = BorMainnetChainConfig.GetBurntContract(23850000 + 1)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x70bcA57F4579f58670aB2d18Ef16e02C17553C38"), *addr)
	addr = BorMainnetChainConfig.GetBurntContract(50523000 - 1)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x70bcA57F4579f58670aB2d18Ef16e02C17553C38"), *addr)
	addr = BorMainnetChainConfig.GetBurntContract(50523000)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x7A8ed27F4C30512326878652d20fC85727401854"), *addr)
	addr = BorMainnetChainConfig.GetBurntContract(50523000 + 1)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x7A8ed27F4C30512326878652d20fC85727401854"), *addr)

	// Amoy
	addr = AmoyChainConfig.GetBurntContract(0)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x000000000000000000000000000000000000dead"), *addr)
}

func TestMainnetBlobSchedule(t *testing.T) {
	// Original EIP-4844 values
	assert.Equal(t, uint64(6), MainnetChainConfig.GetMaxBlobsPerBlock(0))
	assert.Equal(t, uint64(786432), MainnetChainConfig.GetMaxBlobGasPerBlock(0))
	assert.Equal(t, uint64(393216), MainnetChainConfig.GetTargetBlobGasPerBlock(0))
	assert.Equal(t, uint64(3338477), MainnetChainConfig.GetBlobGasPriceUpdateFraction(0))

	b := MainnetChainConfig.BlobSchedule
	isPrague := false
	assert.Equal(t, uint64(3), b.TargetBlobsPerBlock(isPrague))
	assert.Equal(t, uint64(6), b.MaxBlobsPerBlock(isPrague))
	assert.Equal(t, uint64(3338477), b.BaseFeeUpdateFraction(isPrague))

	// EIP-7691: Blob throughput increase
	isPrague = true
	assert.Equal(t, uint64(6), b.TargetBlobsPerBlock(isPrague))
	assert.Equal(t, uint64(9), b.MaxBlobsPerBlock(isPrague))
	assert.Equal(t, uint64(5007716), b.BaseFeeUpdateFraction(isPrague))
}

func TestGnosisBlobSchedule(t *testing.T) {
	b := GnosisChainConfig.BlobSchedule

	// Cancun values
	isPrague := false
	assert.Equal(t, uint64(1), b.TargetBlobsPerBlock(isPrague))
	assert.Equal(t, uint64(2), b.MaxBlobsPerBlock(isPrague))
	assert.Equal(t, uint64(1112826), b.BaseFeeUpdateFraction(isPrague))

	// should remain the same in Pectra for Gnosis
	isPrague = true
	assert.Equal(t, uint64(1), b.TargetBlobsPerBlock(isPrague))
	assert.Equal(t, uint64(2), b.MaxBlobsPerBlock(isPrague))
	assert.Equal(t, uint64(1112826), b.BaseFeeUpdateFraction(isPrague))
}
