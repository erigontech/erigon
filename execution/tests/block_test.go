// Copyright 2015 The go-ethereum Authors
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

package executiontests

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/execution/chain"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/types"
)

func TestLegacyBlockchain(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please") // after remove ChainReader from rules engine - this test can be changed to create less databases, then can enable on win. now timeout after 20min
	}

	bt := new(testMatcher)
	dir := filepath.Join(legacyDir, "BlockchainTests")

	// This directory contains no tests
	bt.skipLoad(`.*\.meta/.*`)

	bt.walk(t, dir, func(t *testing.T, name string, test *testutil.BlockTest) {
		// import pre accounts & construct test genesis block & state root
		if err := bt.checkFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
	})
	// There is also a LegacyTests folder, containing blockchain tests generated
	// prior to Istanbul. However, they are all derived from GeneralStateTests,
	// which run natively, so there's no reason to run them here.
}

func TestExecutionSpecBlockchain(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	bt := new(testMatcher)
	dir := filepath.Join(eestDir, "blockchain_tests")

	// Slow tests
	bt.slow(`^cancun/eip4844_blobs/test_invalid_negative_excess_blob_gas.json`)
	bt.slow(`^frontier/scenarios/test_scenarios.json`)
	bt.slow(`^osaka/eip7939_count_leading_zeros/test_clz_opcode_scenarios.json`)
	bt.slow(`^prague/eip7623_increase_calldata_cost/test_transaction_validity_type_1_type_2.json`)

	// Very slow tests
	bt.skipLoad(`^berlin/eip2930_access_list/test_tx_intrinsic_gas.json`)
	bt.skipLoad(`^cancun/eip4844_blobs/test_sufficient_balance_blob_tx`)
	bt.skipLoad(`^cancun/eip4844_blobs/test_valid_blob_tx_combinations.json`)
	bt.skipLoad(`^frontier/opcodes/test_stack_overflow.json`)
	bt.skipLoad(`^prague/eip2537_bls_12_381_precompiles/test_invalid.json`)
	bt.skipLoad(`^prague/eip2537_bls_12_381_precompiles/test_valid.json`)

	// Tested in the state test format by TestState
	bt.skipLoad(`^static/state_tests/`)

	bt.walk(t, dir, func(t *testing.T, name string, test *testutil.BlockTest) {
		// import pre accounts & construct test genesis block & state root
		if err := bt.checkFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
	})
}

// Only runs EEST tests for current devnet - can "skip" on off-seasons
func TestExecutionSpecBlockchainDevnet(t *testing.T) {
	t.Skip("Osaka is already covered by TestExecutionSpecBlockchain")

	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	defer log.Root().SetHandler(log.Root().GetHandler())
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	bt := new(testMatcher)
	dir := filepath.Join(eestDir, "blockchain_tests_devnet")

	bt.walk(t, dir, func(t *testing.T, name string, test *testutil.BlockTest) {
		// import pre accounts & construct test genesis block & state root
		if err := bt.checkFailure(t, test.Run(t)); err != nil {
			t.Error(err)
		}
	})
}

func TestBenchmarkEngineX(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()
	logger := testlog.Logger(t, log.LvlTrace)
	engineXDir := filepath.Join(eestDir, "benchmark", "blockchain_tests_engine_x")
	preAllocDir := filepath.Join(engineXDir, "pre_alloc")
	engineXRunner, err := ConstructEngineXTestRunner(t, logger, preAllocDir)
	require.NoError(t, err)
	testsDir := filepath.Join(engineXDir, "benchmark")
	tm := new(testMatcher)
	// if wanting to run only a particular test, use this, e.g.:
	//tm.whitelist(".*log\\.json")
	//tm.whitelist(".*codecopy\\.json")
	tm.walk(t, testsDir, func(t *testing.T, name string, test EngineXTestDefinition) {
		err := engineXRunner.Run(t.Context(), test)
		require.NoError(t, err)
	})
}

func ConstructEngineXTestRunner(t *testing.T, logger log.Logger, preAllocsDir string) (*EngineXTestRunner, error) {
	preAllocs := make(map[PreAllocHash]*PreAlloc)
	err := filepath.Walk(preAllocsDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var preAlloc PreAlloc
		err = json.Unmarshal(b, &preAlloc)
		if err != nil {
			return err
		}
		preAllocs[PreAllocHash(strings.TrimSuffix(info.Name(), filepath.Ext(info.Name())))] = &preAlloc
		return nil
	})
	if err != nil {
		return nil, err
	}
	runner := &EngineXTestRunner{
		t:         t,
		logger:    logger,
		preAllocs: preAllocs,
		testers:   make(map[Fork]map[PreAllocHash]EngineApiTester),
	}
	return runner, nil
}

type EngineXTestRunner struct {
	t         *testing.T
	logger    log.Logger
	preAllocs map[PreAllocHash]*PreAlloc
	mu        sync.Mutex
	testers   map[Fork]map[PreAllocHash]EngineApiTester
	wg        sync.WaitGroup
}

type PreAllocHash string

type PreAlloc struct {
	Genesis types.Genesis      `json:"genesis"`
	Alloc   types.GenesisAlloc `json:"pre"`
}

type Fork string

func (f Fork) String() string {
	return string(f)
}

func (extr *EngineXTestRunner) Run(ctx context.Context, test EngineXTestDefinition) error {
	tester, err := extr.getOrCreateTester(test.Fork, test.PreAllocHash)
	if err != nil {
		return err
	}
	for _, newPayload := range test.NewPayloads {
		err := extr.processNewPayload(ctx, tester, newPayload)
		if err != nil {
			return err
		}
	}
	return nil
}

func (extr *EngineXTestRunner) getOrCreateTester(fork Fork, preAllocHash PreAllocHash) (EngineApiTester, error) {
	extr.mu.Lock()
	defer extr.mu.Unlock()
	testersPerAlloc, ok := extr.testers[fork]
	if ok {
		tester, ok := testersPerAlloc[preAllocHash]
		if ok {
			return tester, nil
		}
	} else {
		testersPerAlloc = make(map[PreAllocHash]EngineApiTester)
		extr.testers[fork] = testersPerAlloc
	}
	// create an engine api tester for [fork, preAllocHash] tuple
	forkConfig, ok := testforks.Forks[fork.String()]
	if !ok {
		return EngineApiTester{}, testforks.UnsupportedForkError{Name: fork.String()}
	}
	alloc, ok := extr.preAllocs[preAllocHash]
	if !ok {
		return EngineApiTester{}, fmt.Errorf("pre_alloc %s not found", preAllocHash)
	}
	var forkConfigCopy chain.Config
	err := copier.Copy(&forkConfigCopy, forkConfig)
	if err != nil {
		return EngineApiTester{}, err
	}
	forkConfig = &forkConfigCopy
	genesis := alloc.Genesis
	genesis.Alloc = alloc.Alloc
	genesis.Config = forkConfig
	tester := InitialiseEngineApiTester(extr.t, EngineApiTesterInitArgs{
		Logger:        extr.logger,
		DataDir:       extr.t.TempDir(),
		Genesis:       &genesis,
		NoEmptyBlock1: true,
	})
	testersPerAlloc[preAllocHash] = tester
	return testersPerAlloc[preAllocHash], nil
}

func (extr *EngineXTestRunner) processNewPayload(ctx context.Context, tester EngineApiTester, payload EngineXTestNewPayload) error {
	var enginePayload enginetypes.ExecutionPayload
	var blobHashes []common.Hash
	var parentBeaconRoot common.Hash
	var executionRequests []hexutil.Bytes
	err := json.Unmarshal(payload.Params[0], &enginePayload)
	if err != nil {
		return err
	}
	if len(payload.Params) > 1 {
		err := json.Unmarshal(payload.Params[1], &blobHashes)
		if err != nil {
			return err
		}
	}
	if len(payload.Params) > 2 {
		err := json.Unmarshal(payload.Params[2], &parentBeaconRoot)
		if err != nil {
			return err
		}
	}
	if len(payload.Params) > 3 {
		err := json.Unmarshal(payload.Params[3], &executionRequests)
		if err != nil {
			return err
		}
	}
	payloadVersion, err := strconv.ParseUint(payload.NewPayloadVersion, 10, 64)
	if err != nil {
		return err
	}
	enginePayloadStatus, err := retryEngine(
		ctx,
		[]enginetypes.EngineStatus{enginetypes.SyncingStatus},
		nil,
		func() (*enginetypes.PayloadStatus, enginetypes.EngineStatus, error) {
			var r *enginetypes.PayloadStatus
			var err error
			switch payloadVersion {
			case 1:
				r, err = tester.EngineApiClient.NewPayloadV1(ctx, &enginePayload)
			case 2:
				r, err = tester.EngineApiClient.NewPayloadV2(ctx, &enginePayload)
			case 3:
				r, err = tester.EngineApiClient.NewPayloadV3(ctx, &enginePayload, blobHashes, &parentBeaconRoot)
			case 4:
				r, err = tester.EngineApiClient.NewPayloadV4(ctx, &enginePayload, blobHashes, &parentBeaconRoot, executionRequests)
			case 5:
				r, err = tester.EngineApiClient.NewPayloadV5(ctx, &enginePayload, blobHashes, &parentBeaconRoot, executionRequests)
			default:
				return nil, "", fmt.Errorf("unsupported new payload version %d", payloadVersion)
			}
			if err != nil {
				return nil, "", err
			}
			return r, r.Status, nil
		},
	)
	if err != nil {
		return err
	}
	if enginePayloadStatus.Status != enginetypes.ValidStatus {
		return fmt.Errorf("payload status is not valid: %s", enginePayloadStatus.Status)
	}
	return nil
}

type EngineXTestDefinition struct {
	Fork Fork `json:"network"`
	//LastBlockHash   hexutil.Bytes           `json:"lastblockhash"`
	//ConfigOverrides *chain.Config           `json:"config"`
	PreAllocHash PreAllocHash            `json:"prehash"`
	NewPayloads  []EngineXTestNewPayload `json:"engineNewPayloads"`
}

type EngineXTestNewPayload struct {
	Params            []json.RawMessage `json:"params"`
	NewPayloadVersion string            `json:"newPayloadVersion"`
	FcuVersion        string            `json:"fcuVersion"`
}
