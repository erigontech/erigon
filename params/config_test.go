// Copyright 2017 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package params

import (
	"math/big"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
)

func TestCheckCompatible(t *testing.T) {
	type test struct {
		stored, new *chain.Config
		head        uint64
		wantErr     *chain.ConfigCompatError
	}
	tests := []test{
		{stored: AllProtocolChanges, new: AllProtocolChanges, head: 0, wantErr: nil},
		{stored: AllProtocolChanges, new: AllProtocolChanges, head: 100, wantErr: nil},
		{
			stored:  &chain.Config{TangerineWhistleBlock: big.NewInt(10)},
			new:     &chain.Config{TangerineWhistleBlock: big.NewInt(20)},
			head:    9,
			wantErr: nil,
		},
		{
			stored: AllProtocolChanges,
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
			stored: AllProtocolChanges,
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

	// Mumbai
	addr = MumbaiChainConfig.GetBurntContract(22_640_000)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x70bcA57F4579f58670aB2d18Ef16e02C17553C38"), *addr)
	addr = MumbaiChainConfig.GetBurntContract(22_640_001)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x70bcA57F4579f58670aB2d18Ef16e02C17553C38"), *addr)
	addr = MumbaiChainConfig.GetBurntContract(41_824_607)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x70bcA57F4579f58670aB2d18Ef16e02C17553C38"), *addr)
	addr = MumbaiChainConfig.GetBurntContract(41_824_608)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x617b94CCCC2511808A3C9478ebb96f455CF167aA"), *addr)
	addr = MumbaiChainConfig.GetBurntContract(41_824_609)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x617b94CCCC2511808A3C9478ebb96f455CF167aA"), *addr)
}
