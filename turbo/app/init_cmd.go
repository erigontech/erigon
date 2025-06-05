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
	"strconv"

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
		if err := decodeGenesisStreaming(file, genesis, logger); err != nil {
			utils.Fatalf("invalid genesis file: %v", err)
		}
	} else {
		if err := json.NewDecoder(file).Decode(genesis); err != nil {
			utils.Fatalf("invalid genesis file: %v", err)
		}
	}
	// TODO:DEBUG:record final allocation profile
	// if allocFile, err := os.Create("initgenesis_alloc_final.prof"); err == nil {
	// 	pprof.Lookup("allocs").WriteTo(allocFile, 0)
	// 	allocFile.Close()
	// 	logger.Info("Allocation profile saved", "stage", "final", "file", "initgenesis_alloc_final.prof")
	// }
	// //TODO: just test json decode to save time
	// return nil

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
func decodeGenesisStreaming(r io.Reader, genesis *types.Genesis, logger log.Logger) error {
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
			if err := parseAllocStreaming(decoder, genesis.Alloc, logger); err != nil {
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
func parseAllocStreaming(decoder *json.Decoder, alloc types.GenesisAlloc, logger log.Logger) error {
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
		addr := common.HexToAddress(addrStr)

		// Parse the account manually to avoid reflection
		account, err := parseGenesisAccountStreaming(decoder, logger)
		if err != nil {
			return err
		}

		alloc[addr] = account

		// TODO:Force garbage collection periodically to keep memory usage low during large genesis parsing
		if len(alloc)%1000000 == 0 {
			logger.Info("Processed accounts", "count", len(alloc))
			// Uncomment the line below if memory usage is still too high
			// runtime.GC()
		}
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
	if s == "" {
		return 0
	}

	// Use the same logic as math.ParseUint64
	if len(s) >= 2 && (s[:2] == "0x" || s[:2] == "0X") {
		v, err := strconv.ParseUint(s[2:], 16, 64)
		if err != nil {
			return 0
		}
		return v
	}

	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func parseBigInt(s string) *big.Int {
	if s == "" {
		return new(big.Int)
	}

	// Use the same logic as math.ParseBig256
	var bigint *big.Int
	var ok bool
	if len(s) >= 2 && (s[:2] == "0x" || s[:2] == "0X") {
		bigint, ok = new(big.Int).SetString(s[2:], 16)
	} else {
		bigint, ok = new(big.Int).SetString(s, 10)
	}
	if !ok {
		return big.NewInt(0)
	}
	return bigint
}

// parseGenesisAccountStreaming parses a GenesisAccount manually to avoid reflection
func parseGenesisAccountStreaming(decoder *json.Decoder, logger log.Logger) (types.GenesisAccount, error) {
	// Expect opening brace
	token, err := decoder.Token()
	if err != nil {
		logger.Error("Error reading opening brace for account", "error", err)
		return types.GenesisAccount{}, err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		logger.Error("Expected '{' for account", "got", token)
		return types.GenesisAccount{}, fmt.Errorf("expected '{' for account, got %v", token)
	}

	var account types.GenesisAccount

	// Process each field in the account object
	for decoder.More() {
		// Read field name
		token, err := decoder.Token()
		if err != nil {
			logger.Error("Error reading field name token", "error", err)
			return types.GenesisAccount{}, err
		}

		fieldName, ok := token.(string)
		if !ok {
			logger.Error("Expected field name", "got_type", fmt.Sprintf("%T", token), "value", token)
			return types.GenesisAccount{}, fmt.Errorf("expected field name, got %T", token)
		}

		// Handle each field
		switch fieldName {
		case "balance":
			var balance interface{}
			if err := decoder.Decode(&balance); err != nil {
				logger.Error("Failed to decode balance field", "error", err)
				return types.GenesisAccount{}, fmt.Errorf("failed to decode balance: %w", err)
			}
			// Handle both string and number formats
			switch v := balance.(type) {
			case string:
				account.Balance = parseBigInt(v)
			case float64:
				account.Balance = big.NewInt(int64(v))
			default:
				logger.Error("Unexpected balance type", "type", fmt.Sprintf("%T", v), "value", v)
				return types.GenesisAccount{}, fmt.Errorf("unexpected balance type: %T", v)
			}

		case "nonce":
			var nonce interface{}
			if err := decoder.Decode(&nonce); err != nil {
				logger.Error("Failed to decode nonce field", "error", err)
				return types.GenesisAccount{}, fmt.Errorf("failed to decode nonce: %w", err)
			}
			// Handle both string and number formats
			switch v := nonce.(type) {
			case string:
				account.Nonce = parseUint64(v)
			case float64:
				account.Nonce = uint64(v)
			default:
				account.Nonce = 0 // Default to 0 if not provided or invalid
			}

		case "code":
			var code string
			if err := decoder.Decode(&code); err != nil {
				logger.Error("Failed to decode code field", "error", err)
				return types.GenesisAccount{}, fmt.Errorf("failed to decode code: %w", err)
			}
			account.Code = common.FromHex(code)

		case "constructor":
			var constructor string
			if err := decoder.Decode(&constructor); err != nil {
				logger.Error("Failed to decode constructor field", "error", err)
				return types.GenesisAccount{}, fmt.Errorf("failed to decode constructor: %w", err)
			}
			account.Constructor = common.FromHex(constructor)

		case "storage":
			// Initialize storage if needed
			if account.Storage == nil {
				account.Storage = make(map[common.Hash]common.Hash)
			}

			// Directly decode to account.Storage to avoid intermediate allocations
			// common.Hash implements json.Unmarshaler, so JSON decoder can handle it directly
			if err := decoder.Decode(&account.Storage); err != nil {
				logger.Error("Failed to decode storage field", "error", err)
				return types.GenesisAccount{}, fmt.Errorf("failed to decode storage: %w", err)
			}

		default:
			// Skip unknown fields
			var skip json.RawMessage
			if err := decoder.Decode(&skip); err != nil {
				logger.Error("Failed to skip unknown field", "field", fieldName, "error", err)
				return types.GenesisAccount{}, fmt.Errorf("failed to skip field %s: %w", fieldName, err)
			}
		}
	}

	// Expect closing brace
	token, err = decoder.Token()
	if err != nil {
		logger.Error("Error reading closing brace for account", "error", err)
		return types.GenesisAccount{}, err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '}' {
		logger.Error("Expected '}' for account", "got", token)
		return types.GenesisAccount{}, fmt.Errorf("expected '}' for account, got %v", token)
	}

	return account, nil
}
