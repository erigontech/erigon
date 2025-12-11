// Copyright 2020 The go-ethereum Authors
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

package t8ntool

import (
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/db/kv"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus/ethash"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

type Prestate struct {
	Env stEnv              `json:"env"`
	Pre types.GenesisAlloc `json:"pre"`
}

type ommer struct {
	Delta   uint64         `json:"delta"`
	Address common.Address `json:"address"`
}

//go:generate gencodec -type stEnv -field-override stEnvMarshaling -out gen_stenv.go
type stEnv struct {
	Coinbase         common.Address                      `json:"currentCoinbase"   gencodec:"required"`
	Difficulty       *big.Int                            `json:"currentDifficulty"`
	Random           *big.Int                            `json:"currentRandom"`
	MixDigest        common.Hash                         `json:"mixHash,omitempty"`
	ParentDifficulty *big.Int                            `json:"parentDifficulty"`
	GasLimit         uint64                              `json:"currentGasLimit"   gencodec:"required"`
	Number           uint64                              `json:"currentNumber"     gencodec:"required"`
	Timestamp        uint64                              `json:"currentTimestamp"  gencodec:"required"`
	ParentTimestamp  uint64                              `json:"parentTimestamp,omitempty"`
	BlockHashes      map[math.HexOrDecimal64]common.Hash `json:"blockHashes,omitempty"`
	Ommers           []ommer                             `json:"ommers,omitempty"`
	BaseFee          *big.Int                            `json:"currentBaseFee,omitempty"`
	ParentUncleHash  common.Hash                         `json:"parentUncleHash"`
	UncleHash        common.Hash                         `json:"uncleHash,omitempty"`
	Withdrawals      []*types.Withdrawal                 `json:"withdrawals,omitempty"`
	WithdrawalsHash  *common.Hash                        `json:"withdrawalsRoot,omitempty"`
	RequestsHash     *common.Hash                        `json:"requestsHash,omitempty"`
}

type stEnvMarshaling struct {
	Coinbase         common.UnprefixedAddress
	Difficulty       *math.HexOrDecimal256
	Random           *math.HexOrDecimal256
	ParentDifficulty *math.HexOrDecimal256
	GasLimit         math.HexOrDecimal64
	Number           math.HexOrDecimal64
	Timestamp        math.HexOrDecimal64
	ParentTimestamp  math.HexOrDecimal64
	BaseFee          *math.HexOrDecimal256
}

func MakePreState(chainRules *chain.Rules, tx kv.TemporalRwTx, sd *dbstate.SharedDomains, accounts types.GenesisAlloc, blockNum, txNum uint64) (state.StateReader, state.StateWriter) {
	stateReader, stateWriter := rpchelper.NewLatestStateReader(tx), state.NewWriter(sd.AsPutDel(tx), nil, txNum)
	statedb := state.New(stateReader) //ibs
	for addr, a := range accounts {
		statedb.SetCode(addr, a.Code)
		statedb.SetNonce(addr, a.Nonce)
		var balance uint256.Int
		_ = balance.SetFromBig(a.Balance)
		statedb.SetBalance(addr, balance, tracing.BalanceIncreaseGenesisBalance)
		for k, v := range a.Storage {
			key := k
			val := uint256.NewInt(0).SetBytes(v.Bytes())
			statedb.SetState(addr, key, *val)
		}

		if len(a.Code) > 0 || len(a.Storage) > 0 {
			statedb.SetIncarnation(addr, state.FirstContractIncarnation)
		}
	}
	// Commit and re-open to start with a clean state.
	if err := statedb.FinalizeTx(chainRules, stateWriter); err != nil {
		panic(err)
	}
	if err := statedb.CommitBlock(chainRules, stateWriter); err != nil {
		panic(err)
	}
	return stateReader, stateWriter
}

// calcDifficulty is based on ethash.CalcDifficulty. This method is used in case
// the caller does not provide an explicit difficulty, but instead provides only
// parent timestamp + difficulty.
// Note: this method only works for ethash engine.
func calcDifficulty(config *chain.Config, number, currentTime, parentTime uint64,
	parentDifficulty *big.Int, parentUncleHash common.Hash) *big.Int {
	uncleHash := parentUncleHash
	if uncleHash == (common.Hash{}) {
		uncleHash = empty.UncleHash
	}
	parent := &types.Header{
		ParentHash: common.Hash{},
		UncleHash:  uncleHash,
		Difficulty: parentDifficulty,
		Number:     new(big.Int).SetUint64(number - 1),
		Time:       parentTime,
	}
	return ethash.CalcDifficulty(config, currentTime, parent.Time, parent.Difficulty, number-1, parent.UncleHash)
}
