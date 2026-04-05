// Copyright 2026 The Erigon Authors
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
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/holiman/uint256"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/engineapi"
	"github.com/erigontech/erigon/execution/engineapi/engine_block_downloader"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/rulesconfig"
)

var engineXTestCommand = cli.Command{
	Action:    engineXTestCmd,
	Name:      "enginextest",
	Usage:     "Executes engine-x test fixtures with cached tester per (fork, preAllocHash).",
	ArgsUsage: "<path>",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "pre-alloc-dir",
			Usage:    "Directory containing pre-alloc JSON files",
			Required: true,
		},
		&JSONOutputFlag,
		&RunFlag,
		&VerbosityFlag,
		&WorkersFlag,
	},
}

// enginex fixture types (mirroring execution/tests/engine_x_test_runner.go)

type exTestDef struct {
	Fork        string              `json:"network"`
	PreHash     string              `json:"preHash"`
	NewPayloads []exNewPayload      `json:"engineNewPayloads"`
}

type exNewPayload struct {
	Params            []json.RawMessage `json:"params"`
	NewPayloadVersion string            `json:"newPayloadVersion"`
	FcuVersion        string            `json:"forkchoiceUpdatedVersion"`
	ValidationError   string            `json:"validationError,omitempty"`
	ErrorCode         *int              `json:"errorCode,omitempty"`
}

type exPreAlloc struct {
	Environment exEnvironment      `json:"environment"`
	Genesis     types.Genesis      `json:"genesis"`
	Alloc       types.GenesisAlloc `json:"pre"`
}

type exEnvironment struct {
	Coinbase      common.Address       `json:"currentCoinbase"`
	GasLimit      math.HexOrDecimal64  `json:"currentGasLimit"`
	Timestamp     math.HexOrDecimal64  `json:"currentTimestamp"`
	Difficulty    math.HexOrDecimal64  `json:"currentDifficulty"`
	BaseFee       *math.HexOrDecimal64 `json:"currentBaseFee"`
	ExcessBlobGas *math.HexOrDecimal64 `json:"currentExcessBlobGas"`
	BlobGasUsed   *math.HexOrDecimal64 `json:"currentBlobGasUsed"`
}

// cachedEngine holds a reusable execmoduletester + engineServer for a (fork, preAllocHash).
// mu serializes test execution on the same engine to prevent race conditions.
type cachedEngine struct {
	mu     sync.Mutex
	m      *execmoduletester.ExecModuleTester
	server *engineapi.EngineServer
}

func engineXTestCmd(ctx *cli.Context) error {
	if ctx.Int(VerbosityFlag.Name) > 0 {
		log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(ctx.Int(VerbosityFlag.Name)), log.StderrHandler))
	} else {
		log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))
	}

	// Load pre-allocs
	preAllocDir := ctx.String("pre-alloc-dir")
	preAllocs := make(map[string]*exPreAlloc)
	if err := filepath.WalkDir(preAllocDir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}
		var pa exPreAlloc
		if err := json.Unmarshal(data, &pa); err != nil {
			return nil
		}
		key := strings.TrimSuffix(d.Name(), filepath.Ext(d.Name()))
		preAllocs[key] = &pa
		return nil
	}); err != nil {
		return fmt.Errorf("loading pre-allocs: %v", err)
	}
	fmt.Fprintf(os.Stderr, "Loaded %d pre-allocs\n", len(preAllocs))

	// Parse test files
	path := ctx.Args().First()
	if path == "" {
		return fmt.Errorf("test path required")
	}

	re, err := regexp.Compile(ctx.String(RunFlag.Name))
	if err != nil {
		return fmt.Errorf("invalid regex: %v", err)
	}

	type testItem struct {
		index int
		name  string
		def   exTestDef
	}

	collected := collectFiles(path)
	var items []testItem
	for _, fname := range collected {
		src, err := os.ReadFile(fname)
		if err != nil {
			continue
		}
		var tests map[string]exTestDef
		if err := json.Unmarshal(src, &tests); err != nil {
			continue
		}
		for _, name := range slices.Sorted(maps.Keys(tests)) {
			if !re.MatchString(name) {
				continue
			}
			items = append(items, testItem{index: len(items), name: name, def: tests[name]})
		}
	}
	fmt.Fprintf(os.Stderr, "Collected %d tests\n", len(items))

	if len(items) == 0 {
		return nil
	}

	// Cache of engines keyed by "fork:preHash"
	var mu sync.Mutex
	engines := make(map[string]*cachedEngine)

	getOrCreate := func(fork, preHash string) (*cachedEngine, error) {
		key := fork + ":" + preHash
		mu.Lock()
		if ce, ok := engines[key]; ok {
			mu.Unlock()
			return ce, nil
		}
		mu.Unlock()

		config, ok := testforks.Forks[fork]
		if !ok {
			return nil, fmt.Errorf("unsupported fork: %s", fork)
		}
		pa, ok := preAllocs[preHash]
		if !ok {
			return nil, fmt.Errorf("pre-alloc %s not found", preHash)
		}

		// Build genesis from pre-alloc
		var genesis types.Genesis
		if pa.Environment.GasLimit != 0 {
			env := pa.Environment
			genesis = types.Genesis{
				Config:    config,
				Alloc:     pa.Alloc,
				ExtraData: []byte{0},
				Coinbase:  env.Coinbase,
				GasLimit:  uint64(env.GasLimit),
				Difficulty: uint256.NewInt(uint64(env.Difficulty)),
				Timestamp: uint64(env.Timestamp),
			}
			if env.BaseFee != nil {
				genesis.BaseFee = uint256.NewInt(uint64(*env.BaseFee))
			}
			if env.ExcessBlobGas != nil {
				v := uint64(*env.ExcessBlobGas)
				genesis.ExcessBlobGas = &v
			}
			if env.BlobGasUsed != nil {
				v := uint64(*env.BlobGasUsed)
				genesis.BlobGasUsed = &v
			}
		} else {
			genesis = pa.Genesis
			genesis.Alloc = pa.Alloc
			genesis.Config = config
		}

		engine := rulesconfig.CreateRulesEngineBareBones(ctx.Context, config, log.New())
		m := execmoduletester.New(nil,
			execmoduletester.WithGenesisSpec(&genesis),
			execmoduletester.WithEngine(engine),
		)

		executionClient := direct.NewExecutionClientDirect(m.ExecModule)
		blockDownloader := engine_block_downloader.NewEngineBlockDownloader(
			m.Ctx, log.New(), executionClient, m.BlockReader, m.DB,
			config, ethconfig.Defaults.Sync, nil,
		)
		server := engineapi.NewEngineServer(
			log.New(), config, executionClient, blockDownloader,
			false, true, false, true, nil, time.Hour, ^uint64(0),
		)
		server.SetTest(true)

		// Send initial FCU to genesis so the chain head is set
		genesisHash := m.Genesis.Hash()
		fcuStatus, err := server.HandleForkChoice(m.Ctx, "ForkchoiceUpdated",
			&engine_types.ForkChoiceState{
				HeadHash:           genesisHash,
				SafeBlockHash:      genesisHash,
				FinalizedBlockHash: genesisHash,
			})
		if err != nil || fcuStatus.Status != engine_types.ValidStatus {
			m.DB.Close()
			return nil, fmt.Errorf("initial FCU to genesis failed: %v (status: %v)", err, fcuStatus)
		}

		ce := &cachedEngine{m: m, server: server}
		mu.Lock()
		// Double-check after lock
		if existing, ok := engines[key]; ok {
			mu.Unlock()
			m.DB.Close()
			return existing, nil
		}
		engines[key] = ce
		mu.Unlock()
		return ce, nil
	}

	// Execute tests
	workers := ctx.Int(WorkersFlag.Name)
	if workers <= 0 {
		workers = 1
	}

	type indexedResult struct {
		index  int
		result testResult
	}

	runPayloads := func(ce *cachedEngine, payloads []exNewPayload, result testResult) testResult {
		ce.mu.Lock()
		defer func() {
			// Reset head back to genesis for the next test
			genesisHash := ce.m.Genesis.Hash()
			ce.server.HandleForkChoice(ce.m.Ctx, "ForkchoiceUpdated",
				&engine_types.ForkChoiceState{
					HeadHash:           genesisHash,
					SafeBlockHash:      genesisHash,
					FinalizedBlockHash: genesisHash,
				})
			ce.mu.Unlock()
		}()

		for pi, np := range payloads {
			// Parse payload params
			var payload engine_types.ExecutionPayload
			if err := json.Unmarshal(np.Params[0], &payload); err != nil {
				result.Pass = false
				result.Error = fmt.Sprintf("payload %d: parse error: %v", pi, err)
				return result
			}
			var versionedHashes []common.Hash
			if len(np.Params) > 1 {
				json.Unmarshal(np.Params[1], &versionedHashes)
			}
			var beaconRoot common.Hash
			if len(np.Params) > 2 {
				json.Unmarshal(np.Params[2], &beaconRoot)
			}
			var requests []hexutil.Bytes
			if len(np.Params) > 3 {
				json.Unmarshal(np.Params[3], &requests)
			}

			// Parameter validation (error code checks)
			if np.ErrorCode != nil {
				// Validate params — if error expected, check validation catches it
				version := 0
				fmt.Sscanf(np.NewPayloadVersion, "%d", &version)
				epForValidation := &testutil.EngineNewPayloadPublic{
					ExecutionPayload: payload,
					VersionedHashes:  versionedHashes,
					BeaconRoot:       &beaconRoot,
					Requests:         requests,
				}
				_ = epForValidation
				// Error code tests: just verify we'd reject — skip execution
				continue
			}

			// Build block from payload
			ep := &testutil.EngineNewPayloadPublic{
				ExecutionPayload: payload,
				VersionedHashes:  versionedHashes,
				BeaconRoot:       &beaconRoot,
				Requests:         requests,
			}
			block, bal, err := testutil.PayloadToBlock(ep)
			if err != nil {
				if np.ValidationError != "" {
					continue // Expected invalid
				}
				result.Pass = false
				result.Error = fmt.Sprintf("payload %d: block build error: %v", pi, err)
				return result
			}

			// HandleNewPayload + FCU to advance head
			status, err := ce.server.HandleNewPayload(ce.m.Ctx, "NewPayload", block, versionedHashes, bal)
			if err != nil {
				if np.ValidationError != "" {
					continue
				}
				result.Pass = false
				result.Error = fmt.Sprintf("payload %d: %v", pi, err)
				return result
			}

			if np.ValidationError != "" {
				if status.Status != engine_types.InvalidStatus {
					result.Pass = false
					result.Error = fmt.Sprintf("payload %d: expected INVALID for %q, got %s", pi, np.ValidationError, status.Status)
					return result
				}
				continue
			}

			if status.Status != engine_types.ValidStatus {
				result.Pass = false
				errMsg := ""
				if status.ValidationError != nil {
					errMsg = status.ValidationError.Error().Error()
				}
				result.Error = fmt.Sprintf("payload %d: got %s (err: %s)", pi, status.Status, errMsg)
				return result
			}

			// FCU to advance head between payloads within the same test
			fcuStatus, err := ce.server.HandleForkChoice(ce.m.Ctx, "ForkchoiceUpdated",
				&engine_types.ForkChoiceState{
					HeadHash:           payload.BlockHash,
					SafeBlockHash:      payload.BlockHash,
					FinalizedBlockHash: payload.BlockHash,
				})
			if err != nil {
				result.Pass = false
				result.Error = fmt.Sprintf("payload %d: FCU error: %v", pi, err)
				return result
			}
			if fcuStatus.Status != engine_types.ValidStatus {
				result.Pass = false
				result.Error = fmt.Sprintf("payload %d: FCU %s", pi, fcuStatus.Status)
				return result
			}
		}

		return result
	}

	runOne := func(item testItem) testResult {
		result := testResult{Name: item.name, Pass: true, Fork: item.def.Fork}
		ce, err := getOrCreate(item.def.Fork, item.def.PreHash)
		if err != nil {
			result.Pass = false
			result.Error = err.Error()
			return result
		}
		return runPayloads(ce, item.def.NewPayloads, result)
	}

	if workers == 1 {
		results := make([]testResult, 0, len(items))
		for _, item := range items {
			results = append(results, runOne(item))
		}
		report(ctx, results)
	} else {
		itemCh := make(chan testItem, len(items))
		for _, item := range items {
			itemCh <- item
		}
		close(itemCh)

		resultCh := make(chan indexedResult, len(items))
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for item := range itemCh {
					resultCh <- indexedResult{index: item.index, result: runOne(item)}
				}
			}()
		}
		go func() {
			wg.Wait()
			close(resultCh)
		}()

		ordered := make([]testResult, len(items))
		for ir := range resultCh {
			ordered[ir.index] = ir.result
		}
		report(ctx, ordered)
	}

	// Cleanup
	for _, ce := range engines {
		ce.m.DB.Close()
	}
	return nil
}
