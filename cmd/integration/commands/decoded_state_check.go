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

package commands

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common"
	ecrypto "github.com/erigontech/erigon/common/crypto"
	log "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/execution/decodedstate"
	"github.com/erigontech/erigon/node/debug"
	"github.com/erigontech/erigon/rpc"
)

var (
	decodedCheckRPCURL             string
	decodedCheckContracts          []string
	decodedCheckDiscoverCount      int
	decodedCheckScanContractsLimit int
	decodedCheckBalanceSamples     int
	decodedCheckAllowanceSamples   int
)

// ethCaller is the subset of *rpc.Client used by the verification helpers,
// extracted so the ERC20 probe can be exercised without a live node.
type ethCaller interface {
	CallContext(ctx context.Context, result any, method string, args ...any) error
}

type decodedCheckStatus int

const (
	decodedCheckSkipped decodedCheckStatus = iota
	decodedCheckPass
	decodedCheckFail
)

// errProbeReverted signals that an ERC20 probe eth_call reverted, i.e. the
// contract is not a verifiable ERC20 rather than a genuine decoded mismatch.
var errProbeReverted = errors.New("erc20 probe reverted")

func isRevertErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "execution reverted")
}

func ensureHexPrefix(s string) string {
	if strings.HasPrefix(s, "0x") {
		return s
	}
	return "0x" + s
}

func init() {
	withDataDir(decodedStateCheckCmd)
	withChain(decodedStateCheckCmd)
	decodedStateCheckCmd.Flags().StringVar(&decodedCheckRPCURL, "rpc.url", "http://127.0.0.1:8545", "JSON-RPC endpoint to compare decoded state against")
	decodedStateCheckCmd.Flags().StringSliceVar(&decodedCheckContracts, "contracts", nil, "Specific contract addresses to verify; omit to auto-discover ERC20-like candidates from DecodedLatest")
	decodedStateCheckCmd.Flags().IntVar(&decodedCheckDiscoverCount, "discover", 3, "How many contracts to auto-discover and verify when --contracts is empty")
	decodedStateCheckCmd.Flags().IntVar(&decodedCheckScanContractsLimit, "scan-contracts", 400, "How many distinct decoded contracts to scan during auto-discovery")
	decodedStateCheckCmd.Flags().IntVar(&decodedCheckBalanceSamples, "balance-samples", 5, "How many balanceOf samples to verify per contract")
	decodedStateCheckCmd.Flags().IntVar(&decodedCheckAllowanceSamples, "allowance-samples", 3, "How many allowance samples to verify per contract when allowance mappings exist")
	rootCmd.AddCommand(decodedStateCheckCmd)
}

var decodedStateCheckCmd = &cobra.Command{
	Use:   "check_decoded_state",
	Short: "Check decoded latest state against canonical RPC state on one fixed block",
	Long: `Verifies decoded latest data against canonical EVM state at DecodedMeta.latestBlock.

For each contract it anchors all checks to the same block:
  1. choose the decoded latest block from DecodedMeta
  2. compare decoded balance mappings against eth_call(balanceOf)
  3. compare the same entries against eth_getStorageAt(raw slot)
  4. compare simple mapping values against erigon_getMappingValue on the same fixed block
  5. if nested address=>address mappings exist, compare decoded allowance entries against eth_call(allowance) and raw storage

When --contracts is omitted, the command auto-discovers ERC20-like contracts directly from DecodedLatest.`,
	Example: `./build/bin/integration check_decoded_state --datadir=/path/to/datadir --chain=hoodi
./build/bin/integration check_decoded_state --datadir=/path/to/datadir --chain=hoodi --contracts=0x4d38Bd670764c49Cce1E59EeaEBD05974760aCbD
./build/bin/integration check_decoded_state --datadir=/path/to/datadir --chain=hoodi --discover=5 --rpc.url=http://127.0.0.1:8545`,
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		ctx := cmd.Context()

		dirs := datadir.New(datadirCli)
		chainDB, err := openDB(dbCfg(dbcfg.ChainDB, dirs.Chaindata), true, chain, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer chainDB.Close()

		client, err := rpc.DialContext(ctx, decodedCheckRPCURL, logger)
		if err != nil {
			logger.Error("Dial RPC", "error", err, "rpc", decodedCheckRPCURL)
			return
		}
		defer client.Close()

		latestBlock, latestTxNum, err := decodedProgress(ctx, chainDB)
		if err != nil {
			logger.Error("Read decoded progress", "error", err)
			return
		}
		if latestBlock == 0 {
			logger.Error("Decoded latestBlock is zero; nothing to verify")
			return
		}

		contracts, err := resolvedContracts(ctx, chainDB, client, latestBlock)
		if err != nil {
			logger.Error("Resolve contracts", "error", err)
			return
		}
		if len(contracts) == 0 {
			logger.Warn("No decoded contracts selected for verification")
			return
		}

		logger.Info("decoded-state consistency check start",
			"decodedLatestBlock", latestBlock,
			"decodedLatestTxNum", latestTxNum,
			"contracts", len(contracts),
			"rpc", decodedCheckRPCURL)

		failed := 0
		skipped := 0
		passed := 0
		for _, contract := range contracts {
			storage, err := loadDecodedStorage(ctx, chainDB, contract)
			if err != nil {
				failed++
				logger.Error("decoded-state check failed", "contract", contract, "error", err)
				continue
			}
			report, status, err := inspectDecodedContract(ctx, storage, client, latestBlock, contract)
			if err != nil {
				failed++
				logger.Error("decoded-state check failed", "contract", contract, "error", err)
				continue
			}
			switch status {
			case decodedCheckSkipped:
				skipped++
				logger.Info("decoded-state check skipped: contract is not a verifiable ERC20", "contract", contract)
			case decodedCheckPass:
				passed++
				logDecodedReport(logger, report)
			case decodedCheckFail:
				failed++
				logDecodedReport(logger, report)
			}
		}
		if failed > 0 {
			logger.Error("decoded-state consistency check finished with failures", "failedContracts", failed, "passedContracts", passed, "skippedContracts", skipped, "checkedContracts", len(contracts))
			return
		}
		logger.Info("decoded-state consistency check passed", "passedContracts", passed, "skippedContracts", skipped, "checkedContracts", len(contracts))
	},
}

type decodedContractReport struct {
	Contract           common.Address
	Block              uint64
	Name               string
	Symbol             string
	Decimals           uint64
	TotalSupply        string
	BalanceSlot        common.Hash
	BalanceMatches     int
	BalanceChecked     int
	AllowanceSlot      common.Hash
	AllowanceMatches   int
	AllowanceChecked   int
	BalanceResults     []decodedBalanceCheck
	AllowanceResults   []decodedAllowanceCheck
	Pass               bool
	AllowanceAvailable bool
}

type decodedBalanceCheck struct {
	Address    common.Address
	DecodedVal common.Hash
	RPCVal     common.Hash
	RawVal     common.Hash
	RPCDecoded common.Hash
	RawSlot    common.Hash
	OK         bool
}

type decodedAllowanceCheck struct {
	Owner      common.Address
	Spender    common.Address
	DecodedVal common.Hash
	RPCVal     common.Hash
	RawVal     common.Hash
	RawSlot    common.Hash
	OK         bool
}

type decodedBalanceCandidate struct {
	Slot    common.Hash
	Samples []decodedBalanceSample
	Matches int
}

type decodedAllowanceCandidate struct {
	Slot    common.Hash
	Samples []decodedAllowanceSample
	Matches int
}

type decodedBalanceSample struct {
	Address common.Address
	Value   common.Hash
}

type decodedAllowanceSample struct {
	Owner   common.Address
	Spender common.Address
	Value   common.Hash
}

func decodedProgress(ctx context.Context, db kv.TemporalRwDB) (latestBlock, latestTxNum uint64, err error) {
	err = db.View(ctx, func(tx kv.Tx) error {
		latestBlock, err = decodedstate.LatestBlockTx(tx)
		if err != nil {
			return err
		}
		latestTxNum, err = decodedstate.LatestTxNumTx(tx)
		return err
	})
	return latestBlock, latestTxNum, err
}

func resolvedContracts(ctx context.Context, db kv.TemporalRwDB, client *rpc.Client, decodedLatestBlock uint64) ([]common.Address, error) {
	if len(decodedCheckContracts) > 0 {
		out := make([]common.Address, 0, len(decodedCheckContracts))
		for _, raw := range decodedCheckContracts {
			out = append(out, common.HexToAddress(raw))
		}
		return out, nil
	}
	return discoverDecodedERC20Contracts(ctx, db, client, decodedLatestBlock, decodedCheckDiscoverCount, decodedCheckScanContractsLimit)
}

func discoverDecodedERC20Contracts(ctx context.Context, db kv.TemporalRwDB, client ethCaller, decodedLatestBlock uint64, want, scanLimit int) ([]common.Address, error) {
	if want <= 0 {
		return nil, nil
	}

	contracts, err := distinctDecodedContracts(ctx, db, scanLimit)
	if err != nil {
		return nil, err
	}

	out := make([]common.Address, 0, want)
	for _, contract := range contracts {
		ok, err := isDecodedERC20Like(ctx, db, client, decodedLatestBlock, contract)
		if err != nil {
			continue
		}
		if ok {
			out = append(out, contract)
			if len(out) == want {
				break
			}
		}
	}
	return out, nil
}

func distinctDecodedContracts(ctx context.Context, db kv.TemporalRwDB, limit int) ([]common.Address, error) {
	var out []common.Address
	err := db.View(ctx, func(tx kv.Tx) error {
		cursor, err := tx.Cursor(kv.DecodedLatest)
		if err != nil {
			return err
		}
		defer cursor.Close()

		var last common.Address
		hasLast := false
		for k, _, err := cursor.First(); k != nil; k, _, err = cursor.Next() {
			if err != nil {
				return err
			}
			if len(k) < 20 {
				continue
			}
			contract := common.BytesToAddress(k[:20])
			if hasLast && contract == last {
				continue
			}
			out = append(out, contract)
			last = contract
			hasLast = true
			if limit > 0 && len(out) >= limit {
				break
			}
		}
		return nil
	})
	return out, err
}

func isDecodedERC20Like(ctx context.Context, db kv.TemporalRwDB, client ethCaller, decodedLatestBlock uint64, contract common.Address) (bool, error) {
	storage, err := loadDecodedStorage(ctx, db, contract)
	if err != nil {
		return false, err
	}
	balanceCandidate, ok, err := bestBalanceCandidate(ctx, client, contract, decodedLatestBlock, storage)
	if err != nil || !ok {
		return false, err
	}
	return balanceCandidate.Matches >= min(3, len(balanceCandidate.Samples)), nil
}

func inspectDecodedContract(ctx context.Context, storage map[common.Hash][]decodedstate.DecodedEntry, client ethCaller, decodedLatestBlock uint64, contract common.Address) (*decodedContractReport, decodedCheckStatus, error) {
	if len(storage) == 0 {
		return nil, decodedCheckSkipped, fmt.Errorf("no decoded latest state for contract")
	}

	blockArg := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(decodedLatestBlock))
	name, _ := erc20String(ctx, client, contract, "06fdde03", blockArg)
	symbol, _ := erc20String(ctx, client, contract, "95d89b41", blockArg)
	decimals, _ := erc20Uint(ctx, client, contract, "313ce567", blockArg)
	totalSupply, _ := erc20Uint(ctx, client, contract, "18160ddd", blockArg)

	report := &decodedContractReport{
		Contract:    contract,
		Block:       decodedLatestBlock,
		Name:        name,
		Symbol:      symbol,
		BalanceSlot: common.Hash{},
	}
	if decimals != nil {
		report.Decimals = decimals.Uint64()
	}
	if totalSupply != nil {
		report.TotalSupply = totalSupply.String()
	}

	balanceCandidate, ok, err := bestBalanceCandidate(ctx, client, contract, decodedLatestBlock, storage)
	if err != nil {
		return nil, decodedCheckSkipped, err
	}
	if !ok {
		return nil, decodedCheckSkipped, nil
	}
	report.BalanceSlot = balanceCandidate.Slot
	report.BalanceMatches = balanceCandidate.Matches
	report.BalanceChecked = len(balanceCandidate.Samples)

	balanceChecks, err := verifyBalanceSamples(ctx, client, contract, blockArg, balanceCandidate, decodedCheckBalanceSamples)
	if errors.Is(err, errProbeReverted) {
		return nil, decodedCheckSkipped, nil
	}
	if err != nil {
		return nil, decodedCheckSkipped, err
	}
	report.BalanceResults = balanceChecks

	allowanceCandidate, ok, err := bestAllowanceCandidate(ctx, client, contract, decodedLatestBlock, storage)
	if err != nil {
		return nil, decodedCheckSkipped, err
	}
	if ok {
		report.AllowanceAvailable = true
		report.AllowanceSlot = allowanceCandidate.Slot
		report.AllowanceMatches = allowanceCandidate.Matches
		report.AllowanceChecked = len(allowanceCandidate.Samples)
		allowanceChecks, err := verifyAllowanceSamples(ctx, client, contract, blockArg, allowanceCandidate, decodedCheckAllowanceSamples)
		if errors.Is(err, errProbeReverted) {
			report.AllowanceAvailable = false
		} else if err != nil {
			return nil, decodedCheckSkipped, err
		} else {
			report.AllowanceResults = allowanceChecks
		}
	}

	report.Pass = allBalanceChecksPass(report.BalanceResults) && allAllowanceChecksPass(report.AllowanceResults)
	if report.Pass {
		return report, decodedCheckPass, nil
	}
	return report, decodedCheckFail, nil
}

func loadDecodedStorage(ctx context.Context, db kv.TemporalRwDB, contract common.Address) (map[common.Hash][]decodedstate.DecodedEntry, error) {
	var storage map[common.Hash][]decodedstate.DecodedEntry
	err := db.View(ctx, func(tx kv.Tx) error {
		var err error
		storage, err = decodedstate.QueryDecodedStorage(tx, contract)
		return err
	})
	return storage, err
}

func bestBalanceCandidate(ctx context.Context, client ethCaller, contract common.Address, blockNum uint64, storage map[common.Hash][]decodedstate.DecodedEntry) (decodedBalanceCandidate, bool, error) {
	blockArg := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNum))
	candidates := collectBalanceCandidates(storage, decodedCheckBalanceSamples+1)
	for i := range candidates {
		for _, sample := range candidates[i].Samples {
			bal, err := erc20BalanceOf(ctx, client, contract, sample.Address, blockArg)
			if isRevertErr(err) {
				continue
			}
			if err != nil {
				return decodedBalanceCandidate{}, false, err
			}
			if common.BigToHash(bal) == sample.Value {
				candidates[i].Matches++
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Matches != candidates[j].Matches {
			return candidates[i].Matches > candidates[j].Matches
		}
		return candidates[i].Slot.Big().Cmp(candidates[j].Slot.Big()) < 0
	})
	if len(candidates) == 0 || candidates[0].Matches == 0 {
		return decodedBalanceCandidate{}, false, nil
	}
	return candidates[0], true, nil
}

func bestAllowanceCandidate(ctx context.Context, client ethCaller, contract common.Address, blockNum uint64, storage map[common.Hash][]decodedstate.DecodedEntry) (decodedAllowanceCandidate, bool, error) {
	blockArg := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNum))
	candidates := collectAllowanceCandidates(storage, decodedCheckAllowanceSamples+1)
	for i := range candidates {
		for _, sample := range candidates[i].Samples {
			val, err := erc20Allowance(ctx, client, contract, sample.Owner, sample.Spender, blockArg)
			if isRevertErr(err) {
				continue
			}
			if err != nil {
				return decodedAllowanceCandidate{}, false, err
			}
			if common.BigToHash(val) == sample.Value {
				candidates[i].Matches++
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Matches != candidates[j].Matches {
			return candidates[i].Matches > candidates[j].Matches
		}
		return candidates[i].Slot.Big().Cmp(candidates[j].Slot.Big()) < 0
	})
	if len(candidates) == 0 || candidates[0].Matches == 0 {
		return decodedAllowanceCandidate{}, false, nil
	}
	return candidates[0], true, nil
}

func collectBalanceCandidates(storage map[common.Hash][]decodedstate.DecodedEntry, maxPerSlot int) []decodedBalanceCandidate {
	candidates := make([]decodedBalanceCandidate, 0, len(storage))
	for slot, entries := range storage {
		samples := make([]decodedBalanceSample, 0, maxPerSlot)
		for _, entry := range entries {
			if entry.EntryType != decodedstate.MappingEntry || len(entry.Keys) != 1 || !looksLikeAddressHash(entry.Keys[0]) {
				continue
			}
			samples = append(samples, decodedBalanceSample{
				Address: common.BytesToAddress(entry.Keys[0][12:]),
				Value:   entry.Value,
			})
			if len(samples) == maxPerSlot {
				break
			}
		}
		if len(samples) >= min(3, maxPerSlot) {
			candidates = append(candidates, decodedBalanceCandidate{Slot: slot, Samples: samples})
		}
	}
	return candidates
}

func collectAllowanceCandidates(storage map[common.Hash][]decodedstate.DecodedEntry, maxPerSlot int) []decodedAllowanceCandidate {
	candidates := make([]decodedAllowanceCandidate, 0, len(storage))
	for slot, entries := range storage {
		samples := make([]decodedAllowanceSample, 0, maxPerSlot)
		for _, entry := range entries {
			if entry.EntryType != decodedstate.NestedMappingEntry || len(entry.Keys) != 2 {
				continue
			}
			if !looksLikeAddressHash(entry.Keys[0]) || !looksLikeAddressHash(entry.Keys[1]) {
				continue
			}
			samples = append(samples, decodedAllowanceSample{
				Owner:   common.BytesToAddress(entry.Keys[0][12:]),
				Spender: common.BytesToAddress(entry.Keys[1][12:]),
				Value:   entry.Value,
			})
			if len(samples) == maxPerSlot {
				break
			}
		}
		if len(samples) >= min(2, maxPerSlot) {
			candidates = append(candidates, decodedAllowanceCandidate{Slot: slot, Samples: samples})
		}
	}
	return candidates
}

func verifyBalanceSamples(ctx context.Context, client ethCaller, contract common.Address, blockArg rpc.BlockNumberOrHash, candidate decodedBalanceCandidate, maxChecks int) ([]decodedBalanceCheck, error) {
	samples := candidate.Samples
	if len(samples) > maxChecks {
		samples = samples[:maxChecks]
	}
	results := make([]decodedBalanceCheck, 0, len(samples))
	for _, sample := range samples {
		callVal, err := erc20BalanceOf(ctx, client, contract, sample.Address, blockArg)
		if isRevertErr(err) {
			return nil, errProbeReverted
		}
		if err != nil {
			return nil, err
		}
		rawSlot := mappingSlotForAddress(sample.Address, candidate.Slot)
		rawVal, err := getStorageAt(ctx, client, contract, rawSlot, blockArg)
		if err != nil {
			return nil, err
		}
		var rpcDecoded common.Hash
		if err := client.CallContext(ctx, &rpcDecoded, "erigon_getMappingValue", contract, candidate.Slot, sample.Address.Hash(), blockArg); err != nil {
			return nil, err
		}
		callHash := common.BigToHash(callVal)
		results = append(results, decodedBalanceCheck{
			Address:    sample.Address,
			DecodedVal: sample.Value,
			RPCVal:     callHash,
			RawVal:     rawVal,
			RPCDecoded: rpcDecoded,
			RawSlot:    rawSlot,
			OK:         callHash == sample.Value && rawVal == sample.Value && rpcDecoded == sample.Value,
		})
	}
	return results, nil
}

func verifyAllowanceSamples(ctx context.Context, client ethCaller, contract common.Address, blockArg rpc.BlockNumberOrHash, candidate decodedAllowanceCandidate, maxChecks int) ([]decodedAllowanceCheck, error) {
	samples := candidate.Samples
	if len(samples) > maxChecks {
		samples = samples[:maxChecks]
	}
	results := make([]decodedAllowanceCheck, 0, len(samples))
	for _, sample := range samples {
		callVal, err := erc20Allowance(ctx, client, contract, sample.Owner, sample.Spender, blockArg)
		if isRevertErr(err) {
			return nil, errProbeReverted
		}
		if err != nil {
			return nil, err
		}
		rawSlot := nestedMappingSlotForAddresses(sample.Owner, sample.Spender, candidate.Slot)
		rawVal, err := getStorageAt(ctx, client, contract, rawSlot, blockArg)
		if err != nil {
			return nil, err
		}
		callHash := common.BigToHash(callVal)
		results = append(results, decodedAllowanceCheck{
			Owner:      sample.Owner,
			Spender:    sample.Spender,
			DecodedVal: sample.Value,
			RPCVal:     callHash,
			RawVal:     rawVal,
			RawSlot:    rawSlot,
			OK:         callHash == sample.Value && rawVal == sample.Value,
		})
	}
	return results, nil
}

func erc20BalanceOf(ctx context.Context, client ethCaller, token common.Address, owner common.Address, blockArg rpc.BlockNumberOrHash) (*big.Int, error) {
	data := "0x70a08231" + hex.EncodeToString(common.LeftPadBytes(owner[:], 32))
	return erc20Uint(ctx, client, token, data, blockArg)
}

func erc20Allowance(ctx context.Context, client ethCaller, token common.Address, owner, spender common.Address, blockArg rpc.BlockNumberOrHash) (*big.Int, error) {
	data := "0xdd62ed3e" +
		hex.EncodeToString(common.LeftPadBytes(owner[:], 32)) +
		hex.EncodeToString(common.LeftPadBytes(spender[:], 32))
	return erc20Uint(ctx, client, token, data, blockArg)
}

func erc20Uint(ctx context.Context, client ethCaller, token common.Address, data string, blockArg rpc.BlockNumberOrHash) (*big.Int, error) {
	var out hexString
	call := map[string]any{"to": token, "data": ensureHexPrefix(data)}
	if err := client.CallContext(ctx, &out, "eth_call", call, blockArg); err != nil {
		return nil, err
	}
	return decodeUint256(out)
}

func erc20String(ctx context.Context, client ethCaller, token common.Address, selector string, blockArg rpc.BlockNumberOrHash) (string, error) {
	var out hexString
	call := map[string]any{"to": token, "data": ensureHexPrefix(selector)}
	if err := client.CallContext(ctx, &out, "eth_call", call, blockArg); err != nil {
		return "", err
	}
	return decodeABIString(out)
}

func getStorageAt(ctx context.Context, client ethCaller, contract common.Address, slot common.Hash, blockArg rpc.BlockNumberOrHash) (common.Hash, error) {
	var out hexString
	if err := client.CallContext(ctx, &out, "eth_getStorageAt", contract, slot, blockArg); err != nil {
		return common.Hash{}, err
	}
	return common.HexToHash(string(out)), nil
}

func mappingSlotForAddress(addr common.Address, slot common.Hash) common.Hash {
	var buf [64]byte
	copy(buf[12:32], addr[:])
	copy(buf[32:], slot[:])
	return ecrypto.Keccak256Hash(buf[:])
}

func nestedMappingSlotForAddresses(owner, spender common.Address, slot common.Hash) common.Hash {
	inner := mappingSlotForAddress(owner, slot)
	var buf [64]byte
	copy(buf[12:32], spender[:])
	copy(buf[32:], inner[:])
	return ecrypto.Keccak256Hash(buf[:])
}

func looksLikeAddressHash(h common.Hash) bool {
	for _, b := range h[:12] {
		if b != 0 {
			return false
		}
	}
	return true
}

type hexString string

func decodeUint256(s hexString) (*big.Int, error) {
	raw := strings.TrimPrefix(string(s), "0x")
	if raw == "" {
		return nil, fmt.Errorf("empty uint256 result")
	}
	n, ok := new(big.Int).SetString(raw, 16)
	if !ok {
		return nil, fmt.Errorf("invalid uint256 %q", string(s))
	}
	return n, nil
}

func decodeABIString(s hexString) (string, error) {
	raw := strings.TrimPrefix(string(s), "0x")
	b, err := hex.DecodeString(raw)
	if err != nil {
		return "", err
	}
	if len(b) < 96 {
		return "", fmt.Errorf("abi string payload too short: %d", len(b))
	}
	length := new(big.Int).SetBytes(b[32:64]).Uint64()
	if 64+length > uint64(len(b)) {
		return "", fmt.Errorf("abi string length %d exceeds payload %d", length, len(b))
	}
	return string(b[64 : 64+length]), nil
}

func allBalanceChecksPass(checks []decodedBalanceCheck) bool {
	if len(checks) == 0 {
		return false
	}
	for _, check := range checks {
		if !check.OK {
			return false
		}
	}
	return true
}

func allAllowanceChecksPass(checks []decodedAllowanceCheck) bool {
	for _, check := range checks {
		if !check.OK {
			return false
		}
	}
	return true
}

func logDecodedReport(logger log.Logger, report *decodedContractReport) {
	logger.Info("decoded-state consistency report",
		"contract", report.Contract,
		"block", report.Block,
		"name", report.Name,
		"symbol", report.Symbol,
		"decimals", report.Decimals,
		"totalSupply", report.TotalSupply,
		"balanceSlot", report.BalanceSlot,
		"balanceMatches", fmt.Sprintf("%d/%d", report.BalanceMatches, report.BalanceChecked),
		"allowanceSlot", report.AllowanceSlot,
		"allowanceMatches", fmt.Sprintf("%d/%d", report.AllowanceMatches, report.AllowanceChecked),
		"pass", report.Pass)

	for _, check := range report.BalanceResults {
		logger.Info("decoded-state balance check",
			"contract", report.Contract,
			"block", report.Block,
			"address", check.Address,
			"decoded", check.DecodedVal,
			"ethCall", check.RPCVal,
			"raw", check.RawVal,
			"decodedRPC", check.RPCDecoded,
			"rawSlot", check.RawSlot,
			"ok", check.OK)
	}
	for _, check := range report.AllowanceResults {
		logger.Info("decoded-state allowance check",
			"contract", report.Contract,
			"block", report.Block,
			"owner", check.Owner,
			"spender", check.Spender,
			"decoded", check.DecodedVal,
			"ethCall", check.RPCVal,
			"raw", check.RawVal,
			"rawSlot", check.RawSlot,
			"ok", check.OK)
	}
}
