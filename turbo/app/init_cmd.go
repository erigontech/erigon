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

	"github.com/urfave/cli/v2"

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

const largeGenesisFileThreshold = 100 * 1024 * 1024 // 100MB

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
	if fileInfo.Size() > largeGenesisFileThreshold {
		logger.Info("Using streaming JSON parser for large genesis file", "size", fileInfo.Size())
		if err := parseGenesisStreaming(file, genesis, logger); err != nil {
			utils.Fatalf("invalid genesis file: %v", err)
		}
	} else if err = json.NewDecoder(file).Decode(genesis); err != nil {
		utils.Fatalf("invalid genesis file: %v", err)
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

// parseGenesisStreaming decodes a large genesis file using streaming to reduce memory usage.
func parseGenesisStreaming(r io.Reader, genesis *types.Genesis, logger log.Logger) error {
	decoder := json.NewDecoder(bufio.NewReaderSize(r, 1024*1024)) // 1MB buffer
	genesis.Alloc = make(types.GenesisAlloc)

	// Handlers map defines how to parse each field of the genesis JSON.
	handlers := map[string]func(*json.Decoder) error{
		"config":     func(d *json.Decoder) error { return d.Decode(&genesis.Config) },
		"nonce":      makeStringParser(func(s string) { genesis.Nonce = math.MustParseUint64(s) }),
		"timestamp":  makeStringParser(func(s string) { genesis.Timestamp = math.MustParseUint64(s) }),
		"extraData":  makeStringParser(func(s string) { genesis.ExtraData = common.FromHex(s) }),
		"gasLimit":   makeStringParser(func(s string) { genesis.GasLimit = math.MustParseUint64(s) }),
		"difficulty": makeStringParser(func(s string) { genesis.Difficulty = math.MustParseBig256(s) }),
		"mixhash":    makeStringParser(func(s string) { genesis.Mixhash = common.HexToHash(s) }),
		"coinbase":   makeStringParser(func(s string) { genesis.Coinbase = common.HexToAddress(s) }),
		"parentHash": makeStringParser(func(s string) { genesis.ParentHash = common.HexToHash(s) }),
		"alloc":      func(d *json.Decoder) error { return parseAllocStreaming(d, genesis.Alloc, logger) },
	}

	return parseObject(decoder, handlers, logger, "genesis file")
}

// parseAllocStreaming parses the alloc section of genesis file entry by entry.
func parseAllocStreaming(decoder *json.Decoder, alloc types.GenesisAlloc, logger log.Logger) error {
	handler := func(addrStr string, d *json.Decoder, l log.Logger) error {
		addr := common.HexToAddress(addrStr)
		account, err := parseGenesisAccountStreaming(d, l)
		if err != nil {
			return fmt.Errorf("failed to parse account for address %s: %w", addrStr, err)
		}
		alloc[addr] = account
		return nil
	}
	return streamObjectFields(decoder, handler, logger, "alloc section")
}

// parseGenesisAccountStreaming parses a GenesisAccount using a streaming decoder.
func parseGenesisAccountStreaming(decoder *json.Decoder, logger log.Logger) (types.GenesisAccount, error) {
	var account types.GenesisAccount
	handlers := map[string]func(*json.Decoder) error{
		"balance":     makeStringParser(func(s string) { account.Balance = math.MustParseBig256(s) }),
		"nonce":       makeStringParser(func(s string) { account.Nonce = math.MustParseUint64(s) }),
		"code":        makeStringParser(func(s string) { account.Code = common.FromHex(s) }),
		"constructor": makeStringParser(func(s string) { account.Constructor = common.FromHex(s) }),
		"storage": func(d *json.Decoder) error {
			account.Storage = make(map[common.Hash]common.Hash)
			return d.Decode(&account.Storage)
		},
	}
	err := parseObject(decoder, handlers, logger, "account")
	return account, err
}

// parseObject parses a JSON object using a map of handlers for known keys.
func parseObject(decoder *json.Decoder, handlers map[string]func(*json.Decoder) error, logger log.Logger, contextType string) error {
	handler := func(key string, d *json.Decoder, l log.Logger) error {
		if h, ok := handlers[key]; ok {
			return h(d)
		}
		// Skip unknown fields
		var skip json.RawMessage
		return d.Decode(&skip)
	}
	return streamObjectFields(decoder, handler, logger, contextType)
}

// streamObjectFields parses a JSON object, calling the handler for each key-value pair.
func streamObjectFields(decoder *json.Decoder, handler func(key string, d *json.Decoder, l log.Logger) error, logger log.Logger, contextType string) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("failed to read opening token for %s: %w", contextType, err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("expected '{' for %s, got %v", contextType, token)
	}

	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return fmt.Errorf("failed to read key token for %s: %w", contextType, err)
		}
		key, ok := token.(string)
		if !ok {
			return fmt.Errorf("expected string key for %s, got %T", contextType, token)
		}
		if err = handler(key, decoder, logger); err != nil {
			return fmt.Errorf("error processing key '%s' in %s: %w", key, contextType, err)
		}
	}

	token, err = decoder.Token()
	if err != nil {
		return fmt.Errorf("failed to read closing token for %s: %w", contextType, err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '}' {
		return fmt.Errorf("expected '}' for %s, got %v", contextType, token)
	}
	return nil
}

// makeStringParser is a helper factory that creates a JSON field parser for string-based values.
func makeStringParser(setter func(s string)) func(d *json.Decoder) error {
	return func(d *json.Decoder) error {
		token, err := d.Token()
		if err != nil {
			return err
		}
		s, ok := token.(string)
		if !ok {
			return fmt.Errorf("expected string, got %T", token)
		}
		setter(s)
		return nil
	}
}
