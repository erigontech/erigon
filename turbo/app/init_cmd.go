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
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/math"
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

// Generic function type for streaming parser handlers
type JSONEntryHandler func(key string, decoder *json.Decoder, logger log.Logger) error

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

	genesis := new(types.Genesis)

	// Check file size to determine if we should use streaming
	fileInfo, err := file.Stat()
	if err != nil {
		utils.Fatalf("Failed to stat genesis file: %v", err)
	}
	// Use streaming for files larger than 100MB
	if fileInfo.Size() > 100*1024*1024 {
		logger.Info("Using streaming JSON parser for large genesis file", "size", fileInfo.Size())
		if err := parseGenesisStreaming(file, genesis, logger); err != nil {
			utils.Fatalf("invalid genesis file: %v", err)
		}
	} else if err = json.NewDecoder(file).Decode(genesis); err != nil {
		utils.Fatalf("invalid genesis file: %v", err)
	}
	logger.Info("after parseGenesisStreaming,GC")
	runtime.GC()
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
	logger.Info("Successfully wrote genesis state", "hash", hash.Hash())
	return nil
}

// Helper function to decode token as string
func decodeStringToken(decoder *json.Decoder, fieldName string) (string, error) {
	token, err := decoder.Token()
	if err != nil {
		return "", fmt.Errorf("failed to decode %s: %w", fieldName, err)
	}
	str, ok := token.(string)
	if !ok {
		return "", fmt.Errorf("expected string for %s, got %T", fieldName, token)
	}
	return str, nil
}

// parseGenesisStreaming decodes a large genesis file using streaming to reduce memory usage
func parseGenesisStreaming(r io.Reader, genesis *types.Genesis, logger log.Logger) error {
	bufReader := bufio.NewReaderSize(r, 1024*1024) // 1MB buffer
	decoder := json.NewDecoder(bufReader)
	genesis.Alloc = make(types.GenesisAlloc)

	// Create field handler for genesis object
	genesisFieldHandler := func(fieldName string, decoder *json.Decoder, logger log.Logger) error {
		switch fieldName {
		case "config":
			var config chain.Config
			if err := decoder.Decode(&config); err != nil {
				return fmt.Errorf("failed to decode config: %w", err)
			}
			genesis.Config = &config

		case "nonce":
			nonce, err := decodeStringToken(decoder, "nonce")
			if err != nil {
				return err
			}
			genesis.Nonce = math.MustParseUint64(nonce)

		case "timestamp":
			token, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("failed to decode timestamp: %w", err)
			}
			timestamp, ok := token.(float64)
			if !ok {
				return fmt.Errorf("expected float64 for timestamp, got %T", token)
			}
			genesis.Timestamp = uint64(timestamp)

		case "extraData":
			extraData, err := decodeStringToken(decoder, "extraData")
			if err != nil {
				return err
			}
			genesis.ExtraData = common.FromHex(extraData)

		case "gasLimit":
			gasLimit, err := decodeStringToken(decoder, "gasLimit")
			if err != nil {
				return err
			}
			genesis.GasLimit = math.MustParseUint64(gasLimit)

		case "difficulty":
			difficulty, err := decodeStringToken(decoder, "difficulty")
			if err != nil {
				return err
			}
			genesis.Difficulty = math.MustParseBig256(difficulty)

		case "mixhash":
			mixHash, err := decodeStringToken(decoder, "mixhash")
			if err != nil {
				return err
			}
			genesis.Mixhash = common.HexToHash(mixHash)

		case "coinbase":
			coinbase, err := decodeStringToken(decoder, "coinbase")
			if err != nil {
				return err
			}
			genesis.Coinbase = common.HexToAddress(coinbase)

		case "parentHash":
			parentHash, err := decodeStringToken(decoder, "parentHash")
			if err != nil {
				return err
			}
			genesis.ParentHash = common.HexToHash(parentHash)

		case "alloc":
			if err := parseAllocStreaming(decoder, genesis.Alloc, logger); err != nil {
				return fmt.Errorf("failed to parse alloc: %w", err)
			}

		default:
			// Skip unknown fields
			logger.Debug("Skipping unknown genesis field", "field", fieldName)
			var skip json.RawMessage
			if err := decoder.Decode(&skip); err != nil {
				return fmt.Errorf("failed to skip field %s: %w", fieldName, err)
			}
		}
		return nil
	}

	return parseJSONObjectMapStreaming(decoder, genesisFieldHandler, logger, "genesis file")
}

// parseAllocStreaming parses the alloc section of genesis file entry by entry
func parseAllocStreaming(decoder *json.Decoder, alloc types.GenesisAlloc, logger log.Logger) error {
	allocEntryHandler := func(addrStr string, decoder *json.Decoder, logger log.Logger) error {
		addr := common.HexToAddress(addrStr)
		account, err := parseGenesisAccountStreaming(decoder, logger)
		if err != nil {
			logger.Error("Failed to parse genesis account", "address", addrStr, "error", err)
			return fmt.Errorf("failed to parse account for address %s: %w", addrStr, err)
		}
		alloc[addr] = account
		return nil
	}

	return parseJSONObjectMapStreaming(decoder, allocEntryHandler, logger, "alloc section")
}

// parseGenesisAccountStreaming parses a GenesisAccount streaming
func parseGenesisAccountStreaming(decoder *json.Decoder, logger log.Logger) (types.GenesisAccount, error) {
	var account types.GenesisAccount
	accountFieldHandler := func(fieldName string, decoder *json.Decoder, logger log.Logger) error {
		switch fieldName {
		case "balance":
			balance, err := decodeStringToken(decoder, "balance")
			if err != nil {
				logger.Error("Failed to decode balance field", "error", err)
				return err
			}
			account.Balance = math.MustParseBig256(balance)

		case "nonce":
			nonce, err := decodeStringToken(decoder, "nonce")
			if err != nil {
				logger.Error("Failed to decode nonce field", "error", err)
				return err
			}
			account.Nonce = math.MustParseUint64(nonce)

		case "code":
			code, err := decodeStringToken(decoder, "code")
			if err != nil {
				logger.Error("Failed to decode code field", "error", err)
				return err
			}
			account.Code = common.FromHex(code)

		case "constructor":
			constructor, err := decodeStringToken(decoder, "constructor")
			if err != nil {
				logger.Error("Failed to decode constructor field", "error", err)
				return err
			}
			account.Constructor = common.FromHex(constructor)

		case "storage":
			if account.Storage == nil {
				account.Storage = make(map[common.Hash]common.Hash)
			}
			if err := decoder.Decode(&account.Storage); err != nil {
				logger.Error("Failed to decode storage field", "error", err)
				return fmt.Errorf("failed to decode storage: %w", err)
			}

		default:
			// Skip unknown fields
			var skip json.RawMessage
			if err := decoder.Decode(&skip); err != nil {
				logger.Error("Failed to skip unknown field", "field", fieldName, "error", err)
				return fmt.Errorf("failed to skip field %s: %w", fieldName, err)
			}
		}
		return nil
	}

	err := parseJSONObjectMapStreaming(decoder, accountFieldHandler, logger, "account")
	return account, err
}

// parseJSONObjectMapStreaming parses a JSON object/map and calls the handler for each key-value pair
func parseJSONObjectMapStreaming(decoder *json.Decoder, handler JSONEntryHandler, logger log.Logger, contextType string) error {
	token, err := decoder.Token()
	if err != nil {
		logger.Error("Failed to read opening token", "context", contextType, "error", err)
		return fmt.Errorf("failed to read opening token for %s: %w", contextType, err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		logger.Error("Expected opening brace", "context", contextType, "got", token)
		return fmt.Errorf("expected '{' for %s, got %v", contextType, token)
	}

	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			logger.Error("Failed to read key token", "context", contextType, "error", err)
			return fmt.Errorf("failed to read key token for %s: %w", contextType, err)
		}
		key, ok := token.(string)
		if !ok {
			logger.Error("Expected string key", "context", contextType, "got", token)
			return fmt.Errorf("expected string key for %s, got %T", contextType, token)
		}
		if err = handler(key, decoder, logger); err != nil {
			return err
		}
	}

	token, err = decoder.Token()
	if err != nil {
		logger.Error("Failed to read closing token", "context", contextType, "error", err)
		return fmt.Errorf("failed to read closing token for %s: %w", contextType, err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '}' {
		logger.Error("Expected closing brace", "context", contextType, "got", token)
		return fmt.Errorf("expected '}' for %s, got %v", contextType, token)
	}

	return nil
}
