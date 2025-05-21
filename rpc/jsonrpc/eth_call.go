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
	"unsafe"

	"github.com/holiman/uint256"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon-lib/chain/params"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/gointerfaces"
	txpool_proto "github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/dbutils"
	"github.com/erigontech/erigon-lib/kv/membatchwithdb"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	libstate "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/trie"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon-lib/types/accounts"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/stagedsync"
	"github.com/erigontech/erigon/eth/tracers/logger"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/rpc"
	ethapi2 "github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/turbo/transactions"
)

var latestNumOrHash = rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)

// Call implements eth_call. Executes a new message call immediately without creating a transaction on the block chain.
func (api *APIImpl) Call(ctx context.Context, args ethapi2.CallArgs, blockNrOrHash rpc.BlockNumberOrHash, overrides *ethapi2.StateOverrides) (hexutil.Bytes, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
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

	header, _, err := headerByNumberOrHash(ctx, tx, blockNrOrHash, api)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, nil
	}

	stateReader, err := rpchelper.CreateStateReader(ctx, tx, api._blockReader, blockNrOrHash, 0, api.filters, api.stateCache, chainConfig.ChainName)
	if err != nil {
		return nil, err
	}
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
func headerByNumberOrHash(ctx context.Context, tx kv.Tx, blockNrOrHash rpc.BlockNumberOrHash, api *APIImpl) (*types.Header, bool, error) {
	_, hash, isLatest, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, false, err
	}
	block := api.tryBlockFromLru(hash)
	if block != nil {
		return block.Header(), false, nil
	}

	blockNum, _, _, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, false, err
	}
	header, err := api._blockReader.HeaderByNumber(ctx, tx, blockNum)
	if err != nil {
		return nil, false, err
	}
	// header can be nil
	return header, isLatest, nil
}

// EstimateGas implements eth_estimateGas. Returns an estimate of how much gas is necessary to allow the transaction to complete. The transaction will not be added to the blockchain.
func (api *APIImpl) EstimateGas(ctx context.Context, argsOrNil *ethapi2.CallArgs, blockNrOrHash *rpc.BlockNumberOrHash, overrides *ethapi2.StateOverrides) (hexutil.Uint64, error) {
	var args ethapi2.CallArgs
	// if we actually get CallArgs here, we use them
	if argsOrNil != nil {
		args = *argsOrNil
	}

	dbtx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return 0, err
	}
	defer dbtx.Rollback()

	// Use latest block by default
	if blockNrOrHash == nil {
		blockNrOrHash = &latestNumOrHash
	}

	chainConfig, err := api.chainConfig(ctx, dbtx)
	if err != nil {
		return 0, err
	}
	engine := api.engine()

	header, isLatest, err := headerByNumberOrHash(ctx, dbtx, *blockNrOrHash, api)
	if err != nil {
		return 0, err
	}

	// try to check if it is a pending block
	if header == nil {
		b := api.filters.LastPendingBlock()
		blockNum, _, _, err := rpchelper.GetBlockNumber(ctx, *blockNrOrHash, dbtx, api._blockReader, api.filters)
		if err != nil {
			return 0, err
		}
		if b != nil && blockNum == b.NumberU64() {
			header = b.HeaderNoCopy()
		}
	}

	if header == nil {
		return 0, errors.New(fmt.Sprintf("could not find the header %s in cache or db", blockNrOrHash.String()))
	}

	blockNum := *(header.Number)

	stateReader, err := rpchelper.CreateStateReaderFromBlockNumber(ctx, dbtx, api._txNumReader, blockNum.Uint64(), isLatest, 0, api.stateCache, chainConfig.ChainName)
	if err != nil {
		return 0, err
	}

	// Binary search the gas requirement, as it may be higher than the amount used
	var (
		lo uint64
		hi uint64
	)
	// Use zero address if sender unspecified.
	if args.From == nil {
		args.From = new(common.Address)
	}

	// Determine the highest gas limit can be used during the estimation.
	if args.Gas != nil && uint64(*args.Gas) >= params.TxGas {
		hi = uint64(*args.Gas)
	} else {
		// Retrieve the block to act as the gas ceiling
		hi = header.GasLimit
	}
	// Recap the highest gas allowance with specified gascap.
	if hi > api.GasCap {
		log.Warn("Caller gas above allowance, capping", "requested", hi, "cap", api.GasCap)
		hi = api.GasCap
	}

	var feeCap *big.Int
	if args.GasPrice != nil && (args.MaxFeePerGas != nil || args.MaxPriorityFeePerGas != nil) {
		return 0, errors.New("both gasPrice and (maxFeePerGas or maxPriorityFeePerGas) specified")
	} else if args.GasPrice != nil {
		feeCap = args.GasPrice.ToInt()
	} else if args.MaxFeePerGas != nil {
		feeCap = args.MaxFeePerGas.ToInt()
	} else {
		feeCap = common.Big0
	}
	// Recap the highest gas limit with account's available balance.
	if feeCap.Sign() != 0 {
		state := state.New(stateReader)
		if state == nil {
			return 0, errors.New("can't get the current state")
		}

		balance, err := state.GetBalance(*args.From) // from can't be nil
		if err != nil {
			return 0, err
		}
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

	caller, err := transactions.NewReusableCaller(engine, stateReader, overrides, header, args, api.GasCap, *blockNrOrHash, dbtx, api._blockReader, chainConfig, api.evmCallTimeout)
	if err != nil {
		return 0, err
	}

	// First try with highest gas possible
	result, err := caller.DoCallWithNewGas(ctx, hi, engine, overrides)
	if err != nil || result == nil {
		return 0, err
	}
	if result.Failed() {
		if !errors.Is(result.Err, vm.ErrOutOfGas) {
			if len(result.Revert()) > 0 {
				return 0, ethapi2.NewRevertError(result)
			}
			return 0, result.Err
		}
		// Otherwise, the specified gas cap is too low
		return 0, fmt.Errorf("gas required exceeds allowance (%d)", hi)
	}
	// Assuming a contract can freely run all the instructions, we have
	// the true amount of gas it wants to consume to execute fully.
	// We want to ensure that the gas used doesn't fall below this
	trueGas := result.GasUsed // Must not fall below this
	lo = max(trueGas+result.EvmRefund-1, params.TxGas-1)

	i := 0
	// Execute the binary search and hone in on an executable gas limit
	for lo+1 < hi {
		mid := (hi + lo) / 2
		if mid < trueGas {
			lo = mid
			continue
		}
		result, err := caller.DoCallWithNewGas(ctx, mid, engine, overrides)
		// If the error is not nil(consensus error), it means the provided message
		// call or transaction will never be accepted no matter how much gas it is
		// assigened. Return the error directly, don't struggle any more.
		if err != nil && !errors.Is(err, core.ErrIntrinsicGas) {
			return 0, err
		}
		if errors.Is(err, core.ErrIntrinsicGas) || result.Failed() || result.GasUsed < trueGas {
			lo = mid
		} else {
			hi = mid
		}
		i++
	}
	return hexutil.Uint64(hi), nil
}

// GetProof implements eth_getProof partially; Proofs are available only with the `latest` block tag.
func (api *APIImpl) GetProof(ctx context.Context, address common.Address, storageKeys []hexutil.Bytes, blockNrOrHash rpc.BlockNumberOrHash) (*accounts.AccProofResult, error) {
	roTx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer roTx.Rollback()

	requestedBlockNr, _, _, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, roTx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	} else if requestedBlockNr == 0 {
		return nil, errors.New("block not found")
	}

	storageKeysConverted := make([]common.Hash, len(storageKeys))
	for i, s := range storageKeys {
		storageKeysConverted[i].SetBytes(s)
	}
	return api.getProof(ctx, roTx, address, storageKeysConverted, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(requestedBlockNr)), api.db, api.logger)
}

func (api *APIImpl) getProof(ctx context.Context, roTx kv.Tx, address common.Address, storageKeys []common.Hash, blockNrOrHash rpc.BlockNumberOrHash, db kv.RoDB, logger log.Logger) (*accounts.AccProofResult, error) {
	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	// get the root hash from header to validate proofs along the way
	header, err := api._blockReader.HeaderByNumber(ctx, roTx, blockNrOrHash.BlockNumber.Uint64())
	if err != nil {
		return nil, err
	}

	domains, err := libstate.NewSharedDomains(tx, log.New())
	if err != nil {
		return nil, err
	}
	sdCtx := domains.GetCommitmentContext()

	latestBlock, err := rpchelper.GetLatestBlockNumber(roTx)
	if err != nil {
		return nil, err
	}
	if latestBlock < blockNrOrHash.BlockNumber.Uint64() {
		return nil, fmt.Errorf("block number is in the future latest=%d requested=%d", latestBlock, blockNrOrHash.BlockNumber.Uint64())
	}
	if blockNrOrHash.BlockNumber.Uint64() < latestBlock {
		// Get first txnum of blockNumber+1 to ensure that correct state root will be restored as of blockNumber has been executed
		lastTxnInBlock, err := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, api._blockReader)).Min(tx, blockNrOrHash.BlockNumber.Uint64()+1)
		if err != nil {
			return nil, err
		}
		commitmentStartingTxNum := tx.HistoryStartFrom(kv.CommitmentDomain)
		if lastTxnInBlock < commitmentStartingTxNum {
			return nil, state.PrunedError
		}

		sdCtx.SetLimitReadAsOfTxNum(lastTxnInBlock, false)
		//domains.SetTrace(true)
		if err := domains.SeekCommitment(context.Background(), roTx); err != nil {
			return nil, err
		}
		domains.SetTrace(false)
	}

	// touch account
	sdCtx.TouchKey(kv.AccountsDomain, string(address.Bytes()), nil)

	// generate the trie for proofs, this works by loading the merkle paths to the touched keys
	proofTrie, _, err := sdCtx.Witness(ctx, nil, header.Root[:], "eth_getProof")
	if err != nil {
		return nil, err
	}

	// set initial response fields
	proof := &accounts.AccProofResult{
		Address:      address,
		Balance:      new(hexutil.Big),
		Nonce:        hexutil.Uint64(0),
		CodeHash:     common.Hash{},
		StorageHash:  common.Hash{},
		StorageProof: make([]accounts.StorProofResult, len(storageKeys)),
	}

	// get account proof
	accountProof, err := proofTrie.Prove(crypto.Keccak256(address.Bytes()), 0, false)
	if err != nil {
		return nil, err
	}
	proof.AccountProof = *(*[]hexutil.Bytes)(unsafe.Pointer(&accountProof))

	// get account data from the trie
	acc, _ := proofTrie.GetAccount(crypto.Keccak256(address.Bytes()))
	if acc == nil {
		for i, k := range storageKeys {
			proof.StorageProof[i] = accounts.StorProofResult{
				Key:   uint256.NewInt(0).SetBytes(k[:]).Hex(),
				Value: new(hexutil.Big),
				Proof: nil,
			}
		}
		return proof, nil
	}

	proof.Balance = (*hexutil.Big)(acc.Balance.ToBig())
	proof.Nonce = hexutil.Uint64(acc.Nonce)
	proof.CodeHash = acc.CodeHash
	proof.StorageHash = acc.Root

	// if storage is not empty touch keys and build trie
	if proof.StorageHash.Cmp(common.BytesToHash(empty.RootHash.Bytes())) != 0 && len(storageKeys) != 0 {
		// touch storage keys
		for _, storageKey := range storageKeys {
			sdCtx.TouchKey(kv.StorageDomain, string(common.FromHex(address.Hex()[2:]+storageKey.String()[2:])), nil)
		}

		// generate the trie for proofs, this works by loading the merkle paths to the touched keys
		proofTrie, _, err = sdCtx.Witness(ctx, nil, header.Root[:], "eth_getProof")
		if err != nil {
			return nil, err
		}
	}

	reader, err := rpchelper.CreateStateReader(ctx, tx, api._blockReader, blockNrOrHash, 0, api.filters, api.stateCache, "")
	if err != nil {
		return nil, err
	}

	// get storage key proofs
	for i, keyHash := range storageKeys {
		proof.StorageProof[i].Key = uint256.NewInt(0).SetBytes(keyHash[:]).Hex()

		// if we have simple non contract account just set values directly without requesting any key proof
		if proof.StorageHash.Cmp(common.BytesToHash(empty.RootHash.Bytes())) == 0 {
			proof.StorageProof[i].Proof = nil
			proof.StorageProof[i].Value = new(hexutil.Big)
			continue
		}

		// prepare key path (keccak(address) | keccak(key))
		var fullKey []byte
		fullKey = append(fullKey, crypto.Keccak256(address.Bytes())...)
		fullKey = append(fullKey, crypto.Keccak256(keyHash.Bytes())...)

		// get proof for the given key
		storageProof, err := proofTrie.Prove(fullKey, len(proof.AccountProof), true)
		if err != nil {
			return nil, errors.New("cannot verify store proof")
		}

		res, err := reader.ReadAccountStorage(address, keyHash)
		if err != nil {
			res = []byte{}
			logger.Warn(fmt.Sprintf("couldn't read account storage for the address %s\n", address.String()))
		}
		n := new(big.Int)
		n.SetBytes(res)
		proof.StorageProof[i].Value = (*hexutil.Big)(n)

		// 0x80 represents RLP encoding of an empty proof slice
		proof.StorageProof[i].Proof = []hexutil.Bytes{[]byte{0x80}}
		if len(storageProof) != 0 {
			proof.StorageProof[i].Proof = *(*[]hexutil.Bytes)(unsafe.Pointer(&storageProof))
		}
	}

	// Verify proofs before returning result to the user
	err = trie.VerifyAccountProof(header.Root, proof)
	if err != nil {
		return nil, fmt.Errorf("internal error: failed to verify account proof for generated proof : %w", err)
	}

	// verify storage proofs
	for _, storageProof := range proof.StorageProof {
		err = trie.VerifyStorageProof(proof.StorageHash, storageProof)
		if err != nil {
			return nil, fmt.Errorf("internal error: failed to verify storage proof for key=%x , proof=%+v : %w", storageProof.Key, proof, err)
		}
	}

	return proof, nil
}

func (api *APIImpl) GetWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (hexutil.Bytes, error) {
	return api.getWitness(ctx, api.db, blockNrOrHash, 0, true, api.MaxGetProofRewindBlockCount, api.logger)
}

func (api *APIImpl) GetTxWitness(ctx context.Context, blockNr rpc.BlockNumberOrHash, txIndex hexutil.Uint) (hexutil.Bytes, error) {
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

func (api *BaseAPI) getWitness(ctx context.Context, db kv.RoDB, blockNrOrHash rpc.BlockNumberOrHash, txIndex hexutil.Uint, fullBlock bool, maxGetProofRewindBlockCount int, logger log.Logger) (hexutil.Bytes, error) {
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
		return nil, fmt.Errorf("transaction index out of bounds: %d", txIndex)
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
		return nil, errors.New("engine is not consensus.Engine")
	}

	roTx2, err := db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer roTx2.Rollback()
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
	sdCtx := domains.GetCommitmentContext()

	// execute block #blockNr ephemerally. This will use TrieStateWriter to record touches of accounts and storage keys.
	_, err = core.ExecuteBlockEphemerally(chainConfig, &vm.Config{}, store.GetHashFn, engine, block, store.Tds, store.TrieStateWriter, store.ChainReader, nil, logger)
	if err != nil {
		return nil, err
	}

	// gather touched keys from ephemeral block execution
	touchedPlainKeys, touchedHashedKeys := store.Tds.GetTouchedPlainKeys()
	codeReads := store.Tds.BuildCodeTouches()

	// marking keys we want to get witness for
	for _, key := range touchedPlainKeys {
		sdCtx.TouchKey(kv.AccountsDomain, string(key), nil)
	}

	// generate the block witness, this works by loading the merkle paths to the touched keys (they are loaded from the state at block #blockNr-1)
	witnessTrie, witnessRootHash, err := sdCtx.Witness(ctx, codeReads, prevHeader.Root[:], "computeWitness")
	if err != nil {
		return nil, err
	}

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
	witnessBufBytesCopy := common.CopyBytes(witnessBufBytes)
	return witnessBufBytesCopy, nil
}

func (api *APIImpl) tryBlockFromLru(hash common.Hash) *types.Block {
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

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	engine := api.engine()

	header, latest, err := headerByNumberOrHash(ctx, tx, *blockNrOrHash, api)
	if err != nil {
		return nil, err
	}
	var stateReader state.StateReader

	blockNumber := header.Number.Uint64()

	if latest {
		cacheView, err := api.stateCache.View(ctx, tx)
		if err != nil {
			return nil, err
		}
		stateReader = rpchelper.CreateLatestCachedStateReader(cacheView, tx)
	} else {
		stateReader, err = rpchelper.CreateHistoryStateReader(tx, api._txNumReader, blockNumber+1, 0, chainConfig.ChainName)
		if err != nil {
			return nil, err
		}
	}

	// If the gas amount is not set, extract this as it will depend on access
	// lists and we'll need to reestimate every time
	nogas := args.Gas == nil

	var to common.Address
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
		args.From = &common.Address{}
	}

	// Retrieve the precompiles since they don't need to be added to the access list
	precompiles := vm.ActivePrecompiles(chainConfig.Rules(blockNumber, header.Time))
	excl := make(map[common.Address]struct{})
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

		var baseFee *uint256.Int = nil
		// check if EIP-1559
		if header.BaseFee != nil {
			baseFee, _ = uint256.FromBig(header.BaseFee)
		}

		msg, err := args.ToMessage(api.GasCap, baseFee)
		if err != nil {
			return nil, err
		}

		// Apply the transaction with the access list tracer
		tracer := logger.NewAccessListTracer(accessList, excl, state)
		config := vm.Config{Tracer: tracer.Hooks(), NoBaseFee: true}
		blockCtx := transactions.NewEVMBlockContext(engine, header, bNrOrHash.RequireCanonical, tx, api._blockReader, chainConfig)
		txCtx := core.NewEVMTxContext(msg)

		evm := vm.NewEVM(blockCtx, txCtx, state, chainConfig, config)
		gp := new(core.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())
		res, err := core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */, engine)
		if err != nil {
			return nil, err
		}
		if tracer.Equal(prevTracer) {
			var errString string
			if res.Err != nil {
				errString = res.Err.Error()
			}
			accessList := &accessListResult{Accesslist: &accessList, Error: errString, GasUsed: hexutil.Uint64(res.GasUsed)}
			if optimizeGas != nil && *optimizeGas {
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
func optimizeWarmAddrInAccessList(accessList *accessListResult, addr common.Address) {
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
