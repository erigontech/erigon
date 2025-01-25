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
	"encoding/json"
	"fmt"
	"os"
	"runtime"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
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
	//dir := datadir.New(g.OutputDir)
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
	// Scan the content line by line
	var account AccountFileEntry

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
	}
	fmt.Println("Stats", "Accounts", stats.Accounts, "Contracts", stats.Contracts, "StorageSlots", stats.StorageSlots)
	return nil
}
