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
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"os"
	"runtime/pprof"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/eth/tracers"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/turbo/debug"
)

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
	// CPU profiling
	cpuFile, err := os.Create("initgenesis_cpu.prof")
	if err != nil {
		return err
	}
	defer cpuFile.Close()

	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		return err
	}
	defer pprof.StopCPUProfile()

	var logger log.Logger
	var tracer *tracers.Tracer
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

	genesis := new(types.Genesis)

	// Check file size to determine if we should use streaming
	fileInfo, err := file.Stat()
	if err != nil {
		utils.Fatalf("Failed to stat genesis file: %v", err)
	}

	// Use streaming for files larger than 100MB
	if fileInfo.Size() > 100*1024*1024 {
		logger.Info("Using streaming JSON parser for large genesis file", "size", fileInfo.Size())
		if err := decodeGenesisStreaming(file, genesis); err != nil {
			utils.Fatalf("invalid genesis file: %v", err)
		}
	} else {
		if err := json.NewDecoder(file).Decode(genesis); err != nil {
			utils.Fatalf("invalid genesis file: %v", err)
		}
	}
	// TODO:DEBUG:record final allocation profile
	if allocFile, err := os.Create("initgenesis_alloc_final.prof"); err == nil {
		pprof.Lookup("allocs").WriteTo(allocFile, 0)
		allocFile.Close()
		logger.Info("Allocation profile saved", "stage", "final", "file", "initgenesis_alloc_final.prof")
	}
	//TODO: just test json decode to save time
	return nil

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

	if allocFile, err := os.Create("initgenesis_alloc_final.prof"); err == nil {
		pprof.Lookup("allocs").WriteTo(allocFile, 0)
		allocFile.Close()
		logger.Info("Allocation profile saved", "stage", "final", "file", "initgenesis_alloc_final.prof")
	}

	logger.Info("Successfully wrote genesis state", "hash", hash.Hash())
	return nil
}

// decodeGenesisStreaming decodes a large genesis file using streaming to reduce memory usage
func decodeGenesisStreaming(r io.Reader, genesis *types.Genesis) error {
	// Create a buffered reader for efficient reading
	bufReader := bufio.NewReaderSize(r, 1024*1024) // 1MB buffer

	// First, we need to parse the JSON structure to extract non-alloc fields
	// and locate the alloc section
	decoder := json.NewDecoder(bufReader)

	// Initialize genesis with empty alloc
	genesis.Alloc = make(types.GenesisAlloc)

	// Parse the root object
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("expected '{' at start of genesis file")
	}

	// Process each field in the genesis object
	for decoder.More() {
		// Read field name
		token, err := decoder.Token()
		if err != nil {
			return err
		}

		fieldName, ok := token.(string)
		if !ok {
			return fmt.Errorf("expected field name, got %T", token)
		}

		// Handle each field
		switch fieldName {
		case "config":
			var config chain.Config
			if err := decoder.Decode(&config); err != nil {
				return fmt.Errorf("failed to decode config: %w", err)
			}
			genesis.Config = &config

		case "nonce":
			var nonce string
			if err := decoder.Decode(&nonce); err != nil {
				return fmt.Errorf("failed to decode nonce: %w", err)
			}
			genesis.Nonce = parseUint64(nonce)

		case "timestamp":
			var timestamp uint64
			if err := decoder.Decode(&timestamp); err != nil {
				return fmt.Errorf("failed to decode timestamp: %w", err)
			}
			genesis.Timestamp = timestamp

		case "extraData":
			var extraData string
			if err := decoder.Decode(&extraData); err != nil {
				return fmt.Errorf("failed to decode extraData: %w", err)
			}
			genesis.ExtraData = common.FromHex(extraData)

		case "gasLimit":
			var gasLimit string
			if err := decoder.Decode(&gasLimit); err != nil {
				return fmt.Errorf("failed to decode gasLimit: %w", err)
			}
			genesis.GasLimit = parseUint64(gasLimit)

		case "difficulty":
			var difficulty string
			if err := decoder.Decode(&difficulty); err != nil {
				return fmt.Errorf("failed to decode difficulty: %w", err)
			}
			genesis.Difficulty = parseBigInt(difficulty)

		case "mixHash":
			var mixHash string
			if err := decoder.Decode(&mixHash); err != nil {
				return fmt.Errorf("failed to decode mixHash: %w", err)
			}
			genesis.Mixhash = common.HexToHash(mixHash)

		case "coinbase":
			var coinbase string
			if err := decoder.Decode(&coinbase); err != nil {
				return fmt.Errorf("failed to decode coinbase: %w", err)
			}
			genesis.Coinbase = common.HexToAddress(coinbase)

		case "parentHash":
			var parentHash string
			if err := decoder.Decode(&parentHash); err != nil {
				return fmt.Errorf("failed to decode parentHash: %w", err)
			}
			genesis.ParentHash = common.HexToHash(parentHash)

		case "alloc":
			// Parse the alloc section with streaming
			if err := parseAllocStreaming(decoder, genesis.Alloc); err != nil {
				return fmt.Errorf("failed to parse alloc: %w", err)
			}

		default:
			// Skip unknown fields
			var skip json.RawMessage
			if err := decoder.Decode(&skip); err != nil {
				return fmt.Errorf("failed to skip field %s: %w", fieldName, err)
			}
		}
	}

	// Expect closing brace
	token, err = decoder.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '}' {
		return fmt.Errorf("expected '}' at end of genesis file")
	}

	return nil
}

// parseAllocStreaming parses the alloc section of genesis file entry by entry
func parseAllocStreaming(decoder *json.Decoder, alloc types.GenesisAlloc) error {
	// Expect opening brace
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("expected '{' for alloc section, got %v", token)
	}

	// Process each account entry
	for decoder.More() {
		// Read the address (key)
		token, err := decoder.Token()
		if err != nil {
			return err
		}

		addrStr, ok := token.(string)
		if !ok {
			return fmt.Errorf("expected string address, got %T", token)
		}

		// Parse address
		var addr common.Address
		if has0xPrefix(addrStr) {
			addr = common.HexToAddress(addrStr)
		} else {
			addr = common.HexToAddress("0x" + addrStr)
		}

		// Decode the account
		var account types.GenesisAccount
		if err := decoder.Decode(&account); err != nil {
			return err
		}

		alloc[addr] = account

		// TODO:GC Force garbage collection periodically to keep memory usage low
		// if len(alloc)%10000 == 0 {
		// 	runtime.GC()
		// }
	}

	// Expect closing brace
	token, err = decoder.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '}' {
		return fmt.Errorf("expected '}' for alloc section, got %v", token)
	}

	return nil
}

func has0xPrefix(str string) bool {
	return len(str) >= 2 && str[0] == '0' && (str[1] == 'x' || str[1] == 'X')
}

// Helper functions for parsing hex values
func parseUint64(s string) uint64 {
	if s == "" || s == "0x0" {
		return 0
	}
	// Remove 0x prefix if present
	if has0xPrefix(s) {
		s = s[2:]
	}
	var val uint64
	fmt.Sscanf(s, "%x", &val)
	return val
}

func parseBigInt(s string) *big.Int {
	if s == "" || s == "0x0" {
		return big.NewInt(0)
	}
	// Remove 0x prefix if present
	if has0xPrefix(s) {
		s = s[2:]
	}
	val := new(big.Int)
	val.SetString(s, 16)
	return val
}
