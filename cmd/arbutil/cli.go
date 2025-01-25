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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/kv/temporal"
	"github.com/erigontech/erigon-lib/log/v3"
	state2 "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/holiman/uint256"
)

var CLI struct {
	GenerateStateFromAccountFile GenerateStateFromAccountFile `cmd help:"Generate state from account file (accounts.json)."`
}

type GenerateStateFromAccountFile struct {
	AccountsFile string `help:"Path to accounts.json file." type:"existingfile"`
	OutputDir    string `help:"Output directory for the generated state."`
}

type AccountFileEntry struct {
	Addr         common.Address         `json:"Addr"`
	Balance      json.Number            `json:"Balance"`
	Nonce        uint64                 `json:"Nonce"`
	ContractInfo *ContractInfoStateFile `json:"ContractInfo,omitempty"`
}

type ContractInfoStateFile struct {
	Code            []byte                      `json:"Code"`
	ContractStorage map[common.Hash]common.Hash `json:"ContractStorage,omitempty"`
}

func (g *GenerateStateFromAccountFile) Run(ctx *Context) error {
	dirs := datadir.New(g.OutputDir)
	// read accounts.json
	fileContent, err := os.ReadFile(g.AccountsFile)
	if err != nil {
		return err
	}
	stats := struct {
		Accounts     int
		Contracts    int
		StorageSlots int
	}{}

	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
	logger := log.Root()
	var statedb *state.IntraBlockState // reader behind this statedb is dead at the moment of return, tx is rolled back

	// some users creaing > 1Gb custome genesis by `erigon init`
	genesisTmpDB := mdbx.New(kv.TemporaryDB, logger).InMem(dirs.DataDir).MapSize(2 * datasize.GB).GrowthStep(1 * datasize.MB).MustOpen()
	defer genesisTmpDB.Close()

	agg, err := state2.NewAggregator2(context.Background(), dirs, config3.DefaultStepSize, genesisTmpDB, logger)
	if err != nil {
		return err
	}
	defer agg.Close()

	tdb, err := temporal.New(genesisTmpDB, agg)
	if err != nil {
		return err
	}
	defer tdb.Close()

	tx, err := tdb.BeginTemporalRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	sd, err := state2.NewSharedDomains(tx, logger)
	if err != nil {
		return err
	}
	defer sd.Close()
	// Scan the content line by line
	var account AccountFileEntry

	r, w := state.NewReaderV3(sd), state.NewWriterV4(sd)
	statedb = state.New(r)
	statedb.SetTrace(false)

	for i, line := range bytes.Split(fileContent, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		if err := json.Unmarshal(line, &account); err != nil {
			return err
		}
		//fmt.Println("Account", account.Addr, "Balance", account.Balance, "Nonce", account.Nonce)
		stats.Accounts++
		if account.ContractInfo != nil {
			stats.Contracts++
			if account.ContractInfo.ContractStorage != nil {
				stats.StorageSlots += len(account.ContractInfo.ContractStorage)
			}
		}
		if i%6000 == 0 {
			var m runtime.MemStats
			dbg.ReadMemStats(&m)
			fmt.Println("Processed accounts", i, "alloc", datasize.ByteSize(m.Alloc).HR(), "sys", datasize.ByteSize(m.Sys).HR())
		}

		balance, err := uint256.FromDecimal(account.Balance.String())
		if err != nil {
			panic(fmt.Errorf("failed to parse balance: %w", err))
		}

		statedb.AddBalance(account.Addr, balance, tracing.BalanceIncreaseGenesisBalance)
		statedb.SetNonce(account.Addr, account.Nonce)
		if account.ContractInfo != nil {
			statedb.SetCode(account.Addr, account.ContractInfo.Code)
			if account.ContractInfo.ContractStorage != nil {
				for key, value := range account.ContractInfo.ContractStorage {
					val := uint256.NewInt(0).SetBytes(value.Bytes())
					statedb.SetState(account.Addr, &key, *val)
				}
			}

		}

	}

	// for _, key := range keys {
	// 	addr := libcommon.BytesToAddress([]byte(key))
	// 	account := g.Alloc[addr]

	// 	balance, overflow := uint256.FromBig(account.Balance)
	// 	if overflow {
	// 		panic("overflow at genesis allocs")
	// 	}

	// 	for key, value := range account.Storage {
	// 		key := key
	// 		val := uint256.NewInt(0).SetBytes(value.Bytes())
	// 		statedb.SetState(addr, &key, *val)
	// 	}

	// 	if len(account.Code) > 0 || len(account.Storage) > 0 || len(account.Constructor) > 0 {
	// 		statedb.SetIncarnation(addr, 1)
	// 	}
	// }

	//r, w := state.NewDbStateReader(tx), state.NewDbStateWriter(tx, 0)

	if err = statedb.FinalizeTx(&chain.Rules{}, w); err != nil {
		return err
	}

	rh, err := sd.ComputeCommitment(context.Background(), true, 0, "genesis")
	if err != nil {
		return err
	}
	root := common.BytesToHash(rh)

	fmt.Println("Stats", "Accounts", stats.Accounts, "Contracts", stats.Contracts, "StorageSlots", stats.StorageSlots, "Root", root)
	return nil
}
