// Copyright 2025 The Erigon Authors
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

package blockgen

import (
	"crypto/ecdsa"
	"math/big"
	"testing"

	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/types"
)

func DefaultEngineApiTesterGenesis(t *testing.T) (*types.Genesis, *ecdsa.PrivateKey) {
	coinbasePrivKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	coinbaseAddr := crypto.PubkeyToAddress(coinbasePrivKey.PublicKey)
	var consolidationRequestCode hexutil.Bytes
	err = consolidationRequestCode.UnmarshalText([]byte("0x3373fffffffffffffffffffffffffffffffffffffffe1460d35760115f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff1461019a57600182026001905f5b5f82111560685781019083028483029004916001019190604d565b9093900492505050366060146088573661019a573461019a575f5260205ff35b341061019a57600154600101600155600354806004026004013381556001015f358155600101602035815560010160403590553360601b5f5260605f60143760745fa0600101600355005b6003546002548082038060021160e7575060025b5f5b8181146101295782810160040260040181607402815460601b815260140181600101548152602001816002015481526020019060030154905260010160e9565b910180921461013b5790600255610146565b90505f6002555f6003555b5f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff141561017357505f5b6001546001828201116101885750505f61018e565b01600190035b5f555f6001556074025ff35b5f5ffd"))
	require.NoError(t, err)
	var withdrawalRequestCode hexutil.Bytes
	err = withdrawalRequestCode.UnmarshalText([]byte("0x3373fffffffffffffffffffffffffffffffffffffffe1460cb5760115f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff146101f457600182026001905f5b5f82111560685781019083028483029004916001019190604d565b909390049250505036603814608857366101f457346101f4575f5260205ff35b34106101f457600154600101600155600354806003026004013381556001015f35815560010160203590553360601b5f5260385f601437604c5fa0600101600355005b6003546002548082038060101160df575060105b5f5b8181146101835782810160030260040181604c02815460601b8152601401816001015481526020019060020154807fffffffffffffffffffffffffffffffff00000000000000000000000000000000168252906010019060401c908160381c81600701538160301c81600601538160281c81600501538160201c81600401538160181c81600301538160101c81600201538160081c81600101535360010160e1565b910180921461019557906002556101a0565b90505f6002555f6003555b5f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff14156101cd57505f5b6001546002828201116101e25750505f6101e8565b01600290035b5f555f600155604c025ff35b5f5ffd"))
	require.NoError(t, err)
	var chainConfig chain.Config
	err = copier.CopyWithOption(&chainConfig, chain.AllProtocolChanges, copier.Option{DeepCopy: true})
	require.NoError(t, err)
	genesis := &types.Genesis{
		Config:     &chainConfig,
		Coinbase:   coinbaseAddr,
		Difficulty: merge.ProofOfStakeDifficulty,
		GasLimit:   1_000_000_000,
		Alloc: types.GenesisAlloc{
			coinbaseAddr: {
				Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil), // 1_000 ETH
			},
			params.ConsolidationRequestAddress.Value(): {
				Code:    consolidationRequestCode, // can't be empty
				Storage: make(map[common.Hash]common.Hash),
				Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil),
				Nonce:   1,
			},
			params.WithdrawalRequestAddress.Value(): {
				Code:    withdrawalRequestCode, // can't be empty'
				Storage: make(map[common.Hash]common.Hash),
				Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil),
				Nonce:   1,
			},
		},
	}
	return genesis, coinbasePrivKey
}
