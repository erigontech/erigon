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

package jsonrpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/kv/dbutils"
	"github.com/erigontech/erigon/turbo/trie"

	"github.com/erigontech/erigon-lib/commitment"
	libstate "github.com/erigontech/erigon-lib/state"
	"github.com/holiman/uint256"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon-lib/kv/membatchwithdb"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/gointerfaces"
	txpool_proto "github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/types/accounts"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/eth/stagedsync"
	"github.com/erigontech/erigon/eth/tracers/logger"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rpc"
	ethapi2 "github.com/erigontech/erigon/turbo/adapter/ethapi"
	"github.com/erigontech/erigon/turbo/rpchelper"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/turbo/transactions"
)

var latestNumOrHash = rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)

// Call implements eth_call. Executes a new message call immediately without creating a transaction on the block chain.
func (api *APIImpl) Call(ctx context.Context, args ethapi2.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, overrides *ethapi2.StateOverrides) (hexutility.Bytes, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	engine := api.engine()

	if args.Gas == nil || uint64(*args.Gas) == 0 {
		args.Gas = (*hexutil.Uint64)(&api.GasCap)
	}

	blockNumber, hash, _, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters) // DoCall cannot be executed on non-canonical blocks
	if err != nil {
		return nil, err
	}
	block, err := api.blockWithSenders(ctx, tx, hash, blockNumber)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}

	stateReader, err := rpchelper.CreateStateReader(ctx, tx, api._blockReader, blockNrOrHash, 0, api.filters, api.stateCache, chainConfig.ChainName)
	if err != nil {
		return nil, err
	}
	header := block.HeaderNoCopy()
	result, err := transactions.DoCall(ctx, engine, args, tx, blockNrOrHash, header, overrides, api.GasCap, chainConfig, stateReader, api._blockReader, api.evmCallTimeout)
	if err != nil {
		return nil, err
	}

	if len(result.ReturnData) > api.ReturnDataLimit {
		return nil, fmt.Errorf("call returned result on length %d exceeding --rpc.returndata.limit %d", len(result.ReturnData), api.ReturnDataLimit)
	}

	// If the result contains a revert reason, try to unpack and return it.
	if len(result.Revert()) > 0 {
		return nil, ethapi2.NewRevertError(result)
	}

	return result.Return(), result.Err
}

// headerByNumberOrHash - intent to read recent headers only, tries from the lru cache before reading from the db
func headerByNumberOrHash(ctx context.Context, tx kv.Tx, blockNrOrHash rpc.BlockNumberOrHash, api *APIImpl) (*types.Header, error) {
	_, hash, _, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}
	block := api.tryBlockFromLru(hash)
	if block != nil {
		return block.Header(), nil
	}

	blockNum, _, _, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}
	header, err := api._blockReader.HeaderByNumber(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}
	// header can be nil
	return header, nil
}

// EstimateGas implements eth_estimateGas. Returns an estimate of how much gas is necessary to allow the transaction to complete. The transaction will not be added to the blockchain.
func (api *APIImpl) EstimateGas(ctx context.Context, argsOrNil *ethapi2.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash, overrides *ethapi2.StateOverrides) (hexutil.Uint64, error) {
	var args ethapi2.CallArgs
	// if we actually get CallArgs here, we use them
	if argsOrNil != nil {
		args = *argsOrNil
	}

	dbtx, err := api.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer dbtx.Rollback()

	// Binary search the gas requirement, as it may be higher than the amount used
	var (
		lo     = params.TxGas - 1
		hi     uint64
		gasCap uint64
	)
	// Use zero address if sender unspecified.
	if args.From == nil {
		args.From = new(libcommon.Address)
	}

	bNrOrHash := rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber)
	if blockNrOrHash != nil {
		bNrOrHash = *blockNrOrHash
	}

	// Determine the highest gas limit can be used during the estimation.
	if args.Gas != nil && uint64(*args.Gas) >= params.TxGas {
		hi = uint64(*args.Gas)
	} else {
		// Retrieve the block to act as the gas ceiling
		h, err := headerByNumberOrHash(ctx, dbtx, bNrOrHash, api)
		if err != nil {
			return 0, err
		}
		if h == nil {
			// if a block number was supplied and there is no header return 0
			if blockNrOrHash != nil {
				return 0, nil
			}

			// block number not supplied, so we haven't found a pending block, read the latest block instead
			h, err = headerByNumberOrHash(ctx, dbtx, latestNumOrHash, api)
			if err != nil {
				return 0, err
			}
			if h == nil {
				return 0, nil
			}
		}
		hi = h.GasLimit
	}

	var feeCap *big.Int
	if args.GasPrice != nil && (args.MaxFeePerGas != nil || args.MaxPriorityFeePerGas != nil) {
		return 0, errors.New("both gasPrice and (maxFeePerGas or maxPriorityFeePerGas) specified")
	} else if args.GasPrice != nil {
		feeCap = args.GasPrice.ToInt()
	} else if args.MaxFeePerGas != nil {
		feeCap = args.MaxFeePerGas.ToInt()
	} else {
		feeCap = libcommon.Big0
	}
	// Recap the highest gas limit with account's available balance.
	if feeCap.Sign() != 0 {
		cacheView, err := api.stateCache.View(ctx, dbtx)
		if err != nil {
			return 0, err
		}
		stateReader := rpchelper.CreateLatestCachedStateReader(cacheView, dbtx)
		state := state.New(stateReader)
		if state == nil {
			return 0, errors.New("can't get the current state")
		}

		balance := state.GetBalance(*args.From) // from can't be nil
		available := balance.ToBig()
		if args.Value != nil {
			if args.Value.ToInt().Cmp(available) >= 0 {
				return 0, errors.New("insufficient funds for transfer")
			}
			available.Sub(available, args.Value.ToInt())
		}
		allowance := new(big.Int).Div(available, feeCap)

		// If the allowance is larger than maximum uint64, skip checking
		if allowance.IsUint64() && hi > allowance.Uint64() {
			transfer := args.Value
			if transfer == nil {
				transfer = new(hexutil.Big)
			}
			log.Warn("Gas estimation capped by limited funds", "original", hi, "balance", balance,
				"sent", transfer.ToInt(), "maxFeePerGas", feeCap, "fundable", allowance)
			hi = allowance.Uint64()
		}
	}

	// Recap the highest gas allowance with specified gascap.
	if hi > api.GasCap {
		log.Warn("Caller gas above allowance, capping", "requested", hi, "cap", api.GasCap)
		hi = api.GasCap
	}
	gasCap = hi

	chainConfig, err := api.chainConfig(ctx, dbtx)
	if err != nil {
		return 0, err
	}
	engine := api.engine()

	latestCanBlockNumber, latestCanHash, isLatest, err := rpchelper.GetCanonicalBlockNumber(ctx, latestNumOrHash, dbtx, api._blockReader, api.filters) // DoCall cannot be executed on non-canonical blocks
	if err != nil {
		return 0, err
	}

	// try and get the block from the lru cache first then try DB before failing
	block := api.tryBlockFromLru(latestCanHash)
	if block == nil {
		block, err = api.blockWithSenders(ctx, dbtx, latestCanHash, latestCanBlockNumber)
		if err != nil {
			return 0, err
		}
	}
	if block == nil {
		return 0, errors.New("could not find latest block in cache or db")
	}

	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, api._blockReader))
	stateReader, err := rpchelper.CreateStateReaderFromBlockNumber(ctx, dbtx, txNumsReader, latestCanBlockNumber, isLatest, 0, api.stateCache, chainConfig.ChainName)
	if err != nil {
		return 0, err
	}
	header := block.HeaderNoCopy()

	caller, err := transactions.NewReusableCaller(engine, stateReader, overrides, header, args, api.GasCap, latestNumOrHash, dbtx, api._blockReader, chainConfig, api.evmCallTimeout)
	if err != nil {
		return 0, err
	}

	// Create a helper to check if a gas allowance results in an executable transaction
	executable := func(gas uint64) (bool, *evmtypes.ExecutionResult, error) {
		result, err := caller.DoCallWithNewGas(ctx, gas)
		if err != nil {
			if errors.Is(err, core.ErrIntrinsicGas) {
				// Special case, raise gas limit
				return true, nil, nil
			}

			// Bail out
			return true, nil, err
		}
		return result.Failed(), result, nil
	}

	// Execute the binary search and hone in on an executable gas limit
	for lo+1 < hi {
		mid := (hi + lo) / 2
		failed, _, err := executable(mid)
		// If the error is not nil(consensus error), it means the provided message
		// call or transaction will never be accepted no matter how much gas it is
		// assigened. Return the error directly, don't struggle any more.
		if err != nil {
			return 0, err
		}
		if failed {
			lo = mid
		} else {
			hi = mid
		}
	}

	// Reject the transaction as invalid if it still fails at the highest allowance
	if hi == gasCap {
		failed, result, err := executable(hi)
		if err != nil {
			return 0, err
		}
		if failed {
			if result != nil && !errors.Is(result.Err, vm.ErrOutOfGas) {
				if len(result.Revert()) > 0 {
					return 0, ethapi2.NewRevertError(result)
				}
				return 0, result.Err
			}
			// Otherwise, the specified gas cap is too low
			return 0, fmt.Errorf("gas required exceeds allowance (%d)", gasCap)
		}
	}
	return hexutil.Uint64(hi), nil
}

// maxGetProofRewindBlockCount limits the number of blocks into the past that
// GetProof will allow computing proofs.  Because we must rewind the hash state
// and re-compute the state trie, the further back in time the request, the more
// computationally intensive the operation becomes.  The staged sync code
// assumes that if more than 100_000 blocks are skipped, that the entire trie
// should be re-computed. Re-computing the entire trie will currently take ~15
// minutes on mainnet.  The current limit has been chosen arbitrarily as
// 'useful' without likely being overly computationally intense.

// GetProof is partially implemented; no Storage proofs, and proofs must be for
// blocks within maxGetProofRewindBlockCount blocks of the head.
func (api *APIImpl) GetProof(ctx context.Context, address libcommon.Address, storageKeys []libcommon.Hash, blockNrOrHash rpc.BlockNumberOrHash) (*accounts.AccProofResult, error) {
	return nil, errors.New("not supported by Erigon3")
	/*
		tx, err := api.db.BeginRo(ctx)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()

		blockNr, _, _, err := rpchelper.GetBlockNumber(blockNrOrHash, tx, api.filters)
		if err != nil {
			return nil, err
		}

		header, err := api._blockReader.HeaderByNumber(ctx, tx, blockNr)
		if err != nil {
			return nil, err
		}

		latestBlock, err := rpchelper.GetLatestBlockNumber(tx)
		if err != nil {
			return nil, err
		}

		if latestBlock < blockNr {
			// shouldn't happen, but check anyway
			return nil, fmt.Errorf("block number is in the future latest=%d requested=%d", latestBlock, blockNr)
		}

		rl := trie.NewRetainList(0)
		var loader *trie.FlatDBTrieLoader
		if blockNr < latestBlock {
			if latestBlock-blockNr > uint64(api.MaxGetProofRewindBlockCount) {
				return nil, fmt.Errorf("requested block is too old, block must be within %d blocks of the head block number (currently %d)", uint64(api.MaxGetProofRewindBlockCount), latestBlock)
			}
			batch := membatchwithdb.NewMemoryBatch(tx, api.dirs.Tmp, api.logger)
			defer batch.Rollback()

			unwindState := &stagedsync.UnwindState{UnwindPoint: blockNr}
			stageState := &stagedsync.StageState{BlockNumber: latestBlock}

			hashStageCfg := stagedsync.StageHashStateCfg(nil, api.dirs, api.historyV3(batch))
			if err := stagedsync.UnwindHashStateStage(unwindState, stageState, batch, hashStageCfg, ctx, api.logger); err != nil {
				return nil, err
			}

			interHashStageCfg := stagedsync.StageTrieCfg(nil, false, false, false, api.dirs.Tmp, api._blockReader, nil, api.historyV3(batch), api._agg)
			loader, err = stagedsync.UnwindIntermediateHashesForTrieLoader("eth_getProof", rl, unwindState, stageState, batch, interHashStageCfg, nil, nil, ctx.Done(), api.logger)
			if err != nil {
				return nil, err
			}
			tx = batch
		} else {
			loader = trie.NewFlatDBTrieLoader("eth_getProof", rl, nil, nil, false)
		}

		reader, err := rpchelper.CreateStateReader(ctx, tx, blockNrOrHash, 0, api.filters, api.stateCache, "")
		if err != nil {
			return nil, err
		}
		a, err := reader.ReadAccountData(address)
		if err != nil {
			return nil, err
		}
		if a == nil {
			a = &accounts.Account{}
		}
		pr, err := trie.NewProofRetainer(address, a, storageKeys, rl)
		if err != nil {
			return nil, err
		}

		loader.SetProofRetainer(pr)
		root, err := loader.CalcTrieRoot(tx, nil)
		if err != nil {
			return nil, err
		}

		if root != header.Root {
			return nil, fmt.Errorf("mismatch in expected state root computed %v vs %v indicates bug in proof implementation", root, header.Root)
		}
		return pr.ProofResult()
	*/
}

func (api *APIImpl) GetWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (hexutility.Bytes, error) {
	return api.getWitness(ctx, api.db, blockNrOrHash, 0, true, api.MaxGetProofRewindBlockCount, api.logger)
}

func (api *APIImpl) GetTxWitness(ctx context.Context, blockNr rpc.BlockNumberOrHash, txIndex hexutil.Uint) (hexutility.Bytes, error) {
	return api.getWitness(ctx, api.db, blockNr, txIndex, false, api.MaxGetProofRewindBlockCount, api.logger)
}

func verifyExecResult(execResult *core.EphemeralExecResult, block *types.Block) error {
	actualTxRoot := execResult.TxRoot.Bytes()
	expectedTxRoot := block.TxHash().Bytes()
	if !bytes.Equal(actualTxRoot, expectedTxRoot) {
		return fmt.Errorf("mismatch in block TxRoot actual(%x) != expected(%x)", actualTxRoot, expectedTxRoot)
	}

	actualGasUsed := uint64(execResult.GasUsed)
	expectedGasUsed := block.GasUsed()
	if actualGasUsed != expectedGasUsed {
		return fmt.Errorf("mismatch in block gas used actual(%x) != expected(%x)", actualGasUsed, expectedGasUsed)
	}

	actualReceiptsHash := execResult.ReceiptRoot.Bytes()
	expectedReceiptsHash := block.ReceiptHash().Bytes()
	if !bytes.Equal(actualReceiptsHash, expectedReceiptsHash) {
		return fmt.Errorf("mismatch in receipts hash actual(%x) != expected(%x)", actualReceiptsHash, expectedReceiptsHash)
	}

	// check the state root
	resultingStateRoot := execResult.StateRoot.Bytes()
	expectedBlockStateRoot := block.Root().Bytes()
	if !bytes.Equal(resultingStateRoot, expectedBlockStateRoot) {
		return fmt.Errorf("resulting state root after execution doesn't match state root in block actual(%x)!=expected(%x)", resultingStateRoot, expectedBlockStateRoot)
	}
	return nil
}

func (api *BaseAPI) getWitness(ctx context.Context, db kv.RoDB, blockNrOrHash rpc.BlockNumberOrHash, txIndex hexutil.Uint, fullBlock bool, maxGetProofRewindBlockCount int, logger log.Logger) (hexutility.Bytes, error) {
	roTx, err := db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer roTx.Rollback()

	blockNr, hash, _, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, roTx, api._blockReader, api.filters) // DoCall cannot be executed on non-canonical blocks
	if err != nil {
		return nil, err
	}

	// Witness for genesis block is empty
	if blockNr == 0 {
		w := trie.NewWitness(make([]trie.WitnessOperator, 0))

		var buf bytes.Buffer
		_, err = w.WriteInto(&buf)
		if err != nil {
			return nil, err
		}

		return buf.Bytes(), nil
	}

	block, err := api.blockWithSenders(ctx, roTx, hash, blockNr)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}

	if !fullBlock && int(txIndex) >= len(block.Transactions()) {
		return nil, fmt.Errorf("transaction index out of bounds")
	}

	latestBlock, err := rpchelper.GetLatestBlockNumber(roTx)
	if err != nil {
		return nil, err
	}

	if latestBlock < blockNr {
		// shouldn't happen, but check anyway
		return nil, fmt.Errorf("block number is in the future latest=%d requested=%d", latestBlock, blockNr)
	}

	// Compute the witness if it's for a tx or it's not present in db
	prevHeader, err := api._blockReader.HeaderByNumber(ctx, roTx, blockNr-1)
	if err != nil {
		return nil, err
	}

	regenerateHash := false
	if latestBlock-blockNr > uint64(maxGetProofRewindBlockCount) {
		regenerateHash = true
	}

	engine, ok := api.engine().(consensus.Engine)
	if !ok {
		return nil, fmt.Errorf("engine is not consensus.Engine")
	}

	roTx2, err := db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	txBatch2 := membatchwithdb.NewMemoryBatch(roTx2, "", logger)
	defer txBatch2.Rollback()

	// Prepare witness config
	chainConfig, err := api.chainConfig(ctx, roTx2)
	if err != nil {
		return nil, fmt.Errorf("error loading chain config: %v", err)
	}

	// Unwind to blockNr
	cfg := stagedsync.StageWitnessCfg(true, 0, chainConfig, engine, api._blockReader, api.dirs)
	err = stagedsync.RewindStagesForWitness(txBatch2, blockNr, latestBlock, &cfg, regenerateHash, ctx, logger)
	if err != nil {
		return nil, err
	}

	store, err := stagedsync.PrepareForWitness(txBatch2, block, prevHeader.Root, &cfg, ctx, logger)
	if err != nil {
		return nil, err
	}

	domains, err := libstate.NewSharedDomains(txBatch2, log.New())
	if err != nil {
		return nil, err
	}
	sdCtx := libstate.NewSharedDomainsCommitmentContext(domains, commitment.ModeUpdate, commitment.VariantHexPatriciaTrie)
	patricieTrie := sdCtx.Trie()
	hph, ok := patricieTrie.(*commitment.HexPatriciaHashed)
	if !ok {
		return nil, errors.New("casting to HexPatriciaTrieHashed failed")
	}

	// execute block #blockNr ephemerally. This will use TrieStateWriter to record touches of accounts and storage keys.
	_, err = core.ExecuteBlockEphemerally(chainConfig, &vm.Config{}, store.GetHashFn, engine, block, store.Tds, store.TrieStateWriter, store.ChainReader, nil, logger)
	if err != nil {
		return nil, err
	}

	// gather touched keys from ephemeral block execution
	touchedPlainKeys, touchedHashedKeys := store.Tds.GetTouchedPlainKeys()
	codeReads := store.Tds.BuildCodeTouches()

	// define these keys as "updates", but we are not really updating anything, we just want to load them into the grid,
	// so this is just to satisfy the current hex patricia trie api.
	updates := commitment.NewUpdates(commitment.ModeDirect, sdCtx.TempDir(), hph.HashAndNibblizeKey)
	for _, key := range touchedPlainKeys {
		updates.TouchPlainKey(key, nil, updates.TouchAccount)
	}

	hph.SetTrace(false) // disable tracing to avoid mixing with trace from witness computation
	// generate the block witness, this works by loading the merkle paths to the touched keys (they are loaded from the state at block #blockNr-1)
	witnessTrie, witnessRootHash, err := hph.GenerateWitness(ctx, updates, codeReads, prevHeader.Root[:], "computeWitness")
	if err != nil {
		return nil, err
	}

	//
	if !bytes.Equal(witnessRootHash, prevHeader.Root[:]) {
		return nil, fmt.Errorf("witness root hash mismatch actual(%x)!=expected(%x)", witnessRootHash, prevHeader.Root[:])
	}

	// retain list is need for the serialization of the trie.Trie into a witness
	retainListBuilder := trie.NewRetainListBuilder()
	for _, key := range touchedHashedKeys {
		if len(key) == 32 {
			retainListBuilder.AddTouch(key)
		} else {
			addr, _, hash := dbutils.ParseCompositeStorageKey(key)
			storageTouch := dbutils.GenerateCompositeTrieKey(addr, hash)
			retainListBuilder.AddStorageTouch(storageTouch)
		}
	}

	for _, codeWithHash := range codeReads {
		retainListBuilder.ReadCode(codeWithHash.CodeHash, codeWithHash.Code)
	}

	retainList := retainListBuilder.Build(false)

	// serialize witness trie
	witness, err := witnessTrie.ExtractWitness(true, retainList)
	if err != nil {
		return nil, err
	}

	var witnessBuffer bytes.Buffer
	_, err = witness.WriteInto(&witnessBuffer)
	if err != nil {
		return nil, err
	}

	// this is a verification step: we execute block #blockNr statelessly using the witness, and we expect to get the same state root as in the header
	// otherwise something went wrong
	store.Tds.SetTrie(witnessTrie)
	newStateRoot, err := stagedsync.ExecuteBlockStatelessly(block, prevHeader, store.ChainReader, store.Tds, &cfg, &witnessBuffer, store.GetHashFn, logger)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(newStateRoot.Bytes(), block.Root().Bytes()) {
		fmt.Printf("state root mismatch after stateless execution actual(%x) != expected(%x)\n", newStateRoot.Bytes(), block.Root().Bytes())
	}
	witnessBufBytes := witnessBuffer.Bytes()
	witnessBufBytesCopy := make([]byte, len(witnessBufBytes))
	copy(witnessBufBytesCopy, witnessBufBytes)
	return witnessBufBytesCopy, nil
}

func (api *APIImpl) tryBlockFromLru(hash libcommon.Hash) *types.Block {
	var block *types.Block
	if api.blocksLRU != nil {
		if it, ok := api.blocksLRU.Get(hash); ok && it != nil {
			block = it
		}
	}
	return block
}

// accessListResult returns an optional accesslist
// Its the result of the `eth_createAccessList` RPC call.
// It contains an error if the transaction itself failed.
type accessListResult struct {
	Accesslist *types.AccessList `json:"accessList"`
	Error      string            `json:"error,omitempty"`
	GasUsed    hexutil.Uint64    `json:"gasUsed"`
}

// CreateAccessList implements eth_createAccessList. It creates an access list for the given transaction.
// If the accesslist creation fails an error is returned.
// If the transaction itself fails, an vmErr is returned.
func (api *APIImpl) CreateAccessList(ctx context.Context, args ethapi2.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash, optimizeGas *bool) (*accessListResult, error) {
	bNrOrHash := rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber)
	if blockNrOrHash != nil {
		bNrOrHash = *blockNrOrHash
	}

	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	engine := api.engine()

	blockNumber, hash, latest, err := rpchelper.GetCanonicalBlockNumber(ctx, bNrOrHash, tx, api._blockReader, api.filters) // DoCall cannot be executed on non-canonical blocks
	if err != nil {
		return nil, err
	}
	block, err := api.blockWithSenders(ctx, tx, hash, blockNumber)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	var stateReader state.StateReader
	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, api._blockReader))

	if latest {
		cacheView, err := api.stateCache.View(ctx, tx)
		if err != nil {
			return nil, err
		}
		stateReader = rpchelper.CreateLatestCachedStateReader(cacheView, tx)
	} else {
		stateReader, err = rpchelper.CreateHistoryStateReader(tx, txNumsReader, blockNumber+1, 0, chainConfig.ChainName)
		if err != nil {
			return nil, err
		}
	}

	header := block.Header()
	// If the gas amount is not set, extract this as it will depend on access
	// lists and we'll need to reestimate every time
	nogas := args.Gas == nil

	var to libcommon.Address
	if args.To != nil {
		to = *args.To
	} else {
		// Require nonce to calculate address of created contract
		if args.Nonce == nil {
			var nonce uint64
			reply, err := api.txPool.Nonce(ctx, &txpool_proto.NonceRequest{
				Address: gointerfaces.ConvertAddressToH160(*args.From),
			}, &grpc.EmptyCallOption{})
			if err != nil {
				return nil, err
			}
			if reply.Found {
				nonce = reply.Nonce + 1
			} else {
				a, err := stateReader.ReadAccountData(*args.From)
				if err != nil {
					return nil, err
				}
				nonce = a.Nonce + 1
			}

			args.Nonce = (*hexutil.Uint64)(&nonce)
		}
		to = crypto.CreateAddress(*args.From, uint64(*args.Nonce))
	}

	if args.From == nil {
		args.From = &libcommon.Address{}
	}

	// Retrieve the precompiles since they don't need to be added to the access list
	precompiles := vm.ActivePrecompiles(chainConfig.Rules(blockNumber, header.Time))
	excl := make(map[libcommon.Address]struct{})
	for _, pc := range precompiles {
		excl[pc] = struct{}{}
	}

	// Create an initial tracer
	prevTracer := logger.NewAccessListTracer(nil, excl, nil)
	if args.AccessList != nil {
		prevTracer = logger.NewAccessListTracer(*args.AccessList, excl, nil)
	}
	for {
		state := state.New(stateReader)
		// Retrieve the current access list to expand
		accessList := prevTracer.AccessList()
		log.Trace("Creating access list", "input", accessList)

		// If no gas amount was specified, each unique access list needs it's own
		// gas calculation. This is quite expensive, but we need to be accurate
		// and it's convered by the sender only anyway.
		if nogas {
			args.Gas = nil
		}
		// Set the accesslist to the last al
		args.AccessList = &accessList

		var msg types.Message

		var baseFee *uint256.Int = nil
		// check if EIP-1559
		if header.BaseFee != nil {
			baseFee, _ = uint256.FromBig(header.BaseFee)
		}

		msg, err = args.ToMessage(api.GasCap, baseFee)
		if err != nil {
			return nil, err
		}

		// Apply the transaction with the access list tracer
		tracer := logger.NewAccessListTracer(accessList, excl, state)
		config := vm.Config{Tracer: tracer, Debug: true, NoBaseFee: true}
		blockCtx := transactions.NewEVMBlockContext(engine, header, bNrOrHash.RequireCanonical, tx, api._blockReader, chainConfig)
		txCtx := core.NewEVMTxContext(msg)

		evm := vm.NewEVM(blockCtx, txCtx, state, chainConfig, config)
		gp := new(core.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())
		res, err := core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)
		if err != nil {
			return nil, err
		}
		if tracer.Equal(prevTracer) {
			var errString string
			if res.Err != nil {
				errString = res.Err.Error()
			}
			accessList := &accessListResult{Accesslist: &accessList, Error: errString, GasUsed: hexutil.Uint64(res.UsedGas)}
			if optimizeGas == nil || *optimizeGas { // optimize gas unless explicitly told not to
				optimizeWarmAddrInAccessList(accessList, *args.From)
				optimizeWarmAddrInAccessList(accessList, to)
				optimizeWarmAddrInAccessList(accessList, header.Coinbase)
				for addr := range tracer.CreatedContracts() {
					if !tracer.UsedBeforeCreation(addr) {
						optimizeWarmAddrInAccessList(accessList, addr)
					}
				}
			}
			return accessList, nil
		}
		prevTracer = tracer
	}
}

// some addresses (like sender, recipient, block producer, and created contracts)
// are considered warm already, so we can save by adding these to the access list
// only if we are adding a lot of their respective storage slots as well
func optimizeWarmAddrInAccessList(accessList *accessListResult, addr libcommon.Address) {
	indexToRemove := -1

	for i := 0; i < len(*accessList.Accesslist); i++ {
		entry := (*accessList.Accesslist)[i]
		if entry.Address != addr {
			continue
		}

		// https://eips.ethereum.org/EIPS/eip-2930#charging-less-for-accesses-in-the-access-list
		accessListSavingPerSlot := params.ColdSloadCostEIP2929 - params.WarmStorageReadCostEIP2929 - params.TxAccessListStorageKeyGas

		numSlots := uint64(len(entry.StorageKeys))
		if numSlots*accessListSavingPerSlot <= params.TxAccessListAddressGas {
			indexToRemove = i
		}
	}

	if indexToRemove >= 0 {
		*accessList.Accesslist = removeIndex(*accessList.Accesslist, indexToRemove)
	}
}

func removeIndex(s types.AccessList, index int) types.AccessList {
	return append(s[:index], s[index+1:]...)
}
