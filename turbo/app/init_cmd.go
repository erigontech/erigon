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

package app

import (
	"bufio"
	"os"
	"unsafe"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/eth/tracers"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/turbo/debug"

	jsoniter "github.com/json-iterator/go"
)

var jsoniterAPI = jsoniter.ConfigCompatibleWithStandardLibrary

var initCommand = cli.Command{
	Action:    MigrateFlags(initGenesis),
	Name:      "init",
	Usage:     "Bootstrap and initialize a new genesis block",
	ArgsUsage: "<genesisPath>",
	Flags: []cli.Flag{
		&utils.DataDirFlag,
		&utils.ChainFlag,
	},
	//Category: "BLOCKCHAIN COMMANDS",
	Description: `
The init command initializes a new genesis block and definition for the network.
This is a destructive action and changes the network in which you will be
participating.

It expects the genesis file as argument.`,
}

// initGenesis will initialise the given JSON format genesis file and writes it as
// the zero'd block (i.e. genesis) or will fail hard if it can't succeed.
func initGenesis(cliCtx *cli.Context) error {

	var logger log.Logger
	var tracer *tracers.Tracer
	var err error
	if logger, tracer, _, _, err = debug.Setup(cliCtx, true /* rootLogger */); err != nil {
		return err
	}
	// Make sure we have a valid genesis JSON
	genesisPath := cliCtx.Args().First()
	if len(genesisPath) == 0 {
		utils.Fatalf("Must supply path to genesis JSON file")
	}

	file, err := os.Open(genesisPath)
	if err != nil {
		utils.Fatalf("Failed to read genesis file: %v", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	iter := jsoniter.Parse(jsoniterAPI, reader, 4096)
	defer jsoniterAPI.ReturnIterator(iter)

	genesis := new(types.Genesis)

	for field := iter.ReadObject(); field != ""; field = iter.ReadObject() {
		switch field {
		case "config":
			genesis.Config = new(chain.Config)
			iter.ReadVal(genesis.Config)
		case "nonce":
			genesis.Nonce = math.MustParseUint64(iter.ReadString())
		case "timestamp":
			genesis.Timestamp = math.MustParseUint64(iter.ReadString())
		case "extraData":
			genesis.ExtraData = common.FromHex(iter.ReadString())
		case "gasLimit":
			genesis.GasLimit = math.MustParseUint64(iter.ReadString())
		case "difficulty":
			genesis.Difficulty = math.MustParseBig256(iter.ReadString())
		case "mixhash":
			genesis.Mixhash = common.HexToHash(iter.ReadString())
		case "coinbase":
			genesis.Coinbase = common.HexToAddress(iter.ReadString())
		case "alloc":
			genesis.Alloc = make(types.GenesisAlloc)
			var storageKey string
			var storageValue []byte

			for addr := iter.ReadObject(); addr != ""; addr = iter.ReadObject() {
				address := common.HexToAddress(addr)
				account := types.GenesisAccount{}

				for accountField := iter.ReadObject(); accountField != ""; accountField = iter.ReadObject() {
					switch accountField {
					case "balance":
						account.Balance = math.MustParseBig256(iter.ReadString())
					case "nonce":
						account.Nonce = math.MustParseUint64(iter.ReadString())
					case "code":
						account.Code = common.FromHex(iter.ReadString())
					case "storage":
						account.Storage = make(map[common.Hash]common.Hash)
						for storageKey = iter.ReadObject(); storageKey != ""; storageKey = iter.ReadObject() {
							storageValue = iter.ReadStringAsSlice()
							// unsafe []byte to string to avoid extra memory allocation
							ss := unsafe.String((*byte)(unsafe.Pointer(&storageValue[0])), len(storageValue))
							account.Storage[common.HexToHash(storageKey)] = common.HexToHash(ss)
						}
					default:
						iter.Skip()
					}
				}
				genesis.Alloc[address] = account
			}
		default:
			iter.Skip()
		}
	}

	if iter.Error != nil {
		utils.Fatalf("invalid genesis file: %v", iter.Error)
	}

	// Open and initialise both full and light databases
	stack, err := MakeNodeWithDefaultConfig(cliCtx, logger)
	if err != nil {
		return err
	}
	defer stack.Close()

	chaindb, err := node.OpenDatabase(cliCtx.Context, stack.Config(), kv.ChainDB, "", false, logger)
	if err != nil {
		utils.Fatalf("Failed to open database: %v", err)
	}

	if tracer != nil {
		if tracer.Hooks != nil && tracer.Hooks.OnBlockchainInit != nil {
			tracer.Hooks.OnBlockchainInit(genesis.Config)
		}
	}
	_, hash, err := core.CommitGenesisBlock(chaindb, genesis, datadir.New(cliCtx.String(utils.DataDirFlag.Name)), logger)
	if err != nil {
		utils.Fatalf("Failed to write genesis block: %v", err)
	}
	chaindb.Close()
	logger.Info("Successfully wrote genesis state", "hash", hash.Hash())
	return nil
}
