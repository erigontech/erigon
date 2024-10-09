package jsonrpc

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/erigon/rpc"
	db2 "github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

const (
	//used if address not specified
	defaultSenderAddress = "0x1111111111111111111111111111111111111111"

	defaultV = "0x1c"
	defaultR = "0xa54492cfacf71aef702421b7fbc70636537a7b2fbe5718c5ed970a001bb7756b"
	defaultS = "0x2e9fb27acc75955b898f0b12ec52aa34bf08f01db654374484b80bf12f0d841e"
)

type zkevmRPCTransaction struct {
	Gas      hexutil.Uint64   `json:"gas"`
	GasPrice *hexutil.Big     `json:"gasPrice,omitempty"`
	Input    hexutility.Bytes `json:"input"`
	Nonce    hexutil.Uint64   `json:"nonce"`
	To       *common.Address  `json:"to"`
	From     *common.Address  `json:"from"`
	Value    *hexutil.Big     `json:"value"`
	Data     hexutility.Bytes `json:"data"`
}

// Tx return types.Transaction from rpcTransaction
func (tx *zkevmRPCTransaction) Tx(sr state.StateReader) (types.Transaction, error) {
	if tx == nil {
		return nil, nil
	}

	sender := common.HexToAddress(defaultSenderAddress)
	if tx.From != nil {
		sender = *tx.From
	}
	nonce := uint64(0)

	ad, err := sr.ReadAccountData(sender)
	if err != nil {
		return nil, err
	}
	if ad != nil {
		nonce = ad.Nonce
	}

	if tx.Value == nil {
		// set this to something non nil
		tx.Value = &hexutil.Big{}
	}

	gasPrice := uint256.NewInt(1)
	if tx.GasPrice != nil {
		gasPrice = uint256.MustFromBig(tx.GasPrice.ToInt())
	}

	var data []byte
	if tx.Data != nil {
		data = tx.Data
	} else if tx.Input != nil {
		data = tx.Input
	} else if tx.To == nil {
		return nil, fmt.Errorf("contract creation without data provided")
	}

	var legacy *types.LegacyTx
	if tx.To == nil {
		legacy = types.NewContractCreation(
			nonce,
			uint256.MustFromBig(tx.Value.ToInt()),
			uint64(tx.Gas),
			gasPrice,
			data,
		)
	} else {
		legacy = types.NewTransaction(
			nonce,
			*tx.To,
			uint256.MustFromBig(tx.Value.ToInt()),
			uint64(tx.Gas),
			gasPrice,
			data,
		)
	}

	legacy.SetSender(sender)

	legacy.V = *uint256.MustFromHex(defaultV)
	legacy.R = *uint256.MustFromHex(defaultR)
	legacy.S = *uint256.MustFromHex(defaultS)

	return legacy, nil
}

// EstimateGas implements eth_estimateGas. Returns an estimate of how much gas is necessary to allow the transaction to complete. The transaction will not be added to the blockchain.
func (zkapi *ZkEvmAPIImpl) EstimateCounters(ctx context.Context, rpcTx *zkevmRPCTransaction) (json.RawMessage, error) {
	api := zkapi.ethApi

	dbtx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer dbtx.Rollback()

	chainConfig, err := api.chainConfig(ctx, dbtx)
	if err != nil {
		return nil, err
	}
	engine := api.engine()

	latestCanBlockNumber, latestCanHash, isLatest, err := rpchelper.GetCanonicalBlockNumber(latestNumOrHash, dbtx, api.filters) // DoCall cannot be executed on non-canonical blocks
	if err != nil {
		return nil, err
	}

	// try and get the block from the lru cache first then try DB before failing
	block := api.tryBlockFromLru(latestCanHash)
	if block == nil {
		block, err = api.blockWithSenders(ctx, dbtx, latestCanHash, latestCanBlockNumber)
		if err != nil {
			return nil, err
		}
	}
	if block == nil {
		return nil, fmt.Errorf("could not find latest block in cache or db")
	}

	stateReader, err := rpchelper.CreateStateReaderFromBlockNumber(ctx, dbtx, latestCanBlockNumber, isLatest, 0, api.stateCache, api.historyV3(dbtx), chainConfig.ChainName)
	if err != nil {
		return nil, err
	}
	header := block.HeaderNoCopy()

	ibs := state.New(stateReader)

	blockCtx := core.NewEVMBlockContext(header, core.GetHashFn(header, nil), engine, nil)

	rules := chainConfig.Rules(block.NumberU64(), header.Time)

	signer := types.MakeSigner(chainConfig, header.Number.Uint64(), 0)

	tx, err := rpcTx.Tx(stateReader)
	if err != nil {
		return nil, err
	}

	msg, err := tx.AsMessage(*signer, header.BaseFee, rules)
	if err != nil {
		return nil, err
	}

	// we don't care about the nonce value for this check on counters
	msg.SetCheckNonce(false)

	txCtx := core.NewEVMTxContext(msg)

	eriDb := db2.NewRoEriDb(dbtx)
	smt := smt.NewRoSMT(eriDb)
	hermezDb := hermez_db.NewHermezDbReader(dbtx)

	forkId, err := hermezDb.GetForkIdByBlockNum(block.NumberU64())
	if err != nil {
		return nil, err
	}

	smtDepth := smt.GetDepth()

	txCounters := vm.NewTransactionCounter(tx, int(smtDepth), uint16(forkId), zkapi.config.Zk.VirtualCountersSmtReduction, false)
	batchCounters := vm.NewBatchCounterCollector(int(smtDepth), uint16(forkId), zkapi.config.Zk.VirtualCountersSmtReduction, false, nil)

	_, err = batchCounters.AddNewTransactionCounters(txCounters)
	if err != nil {
		return nil, err
	}

	zkConfig := vm.ZkConfig{Config: vm.Config{NoBaseFee: true}, CounterCollector: txCounters.ExecutionCounters()}
	evm := vm.NewZkEVM(blockCtx, txCtx, ibs, chainConfig, zkConfig)

	gp := new(core.GasPool).AddGas(msg.Gas())

	ibs.Init(tx.Hash(), header.Hash(), 0)

	execResult, oocError := core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)

	l1InfoIndex, err := hermezDb.GetBlockL1InfoTreeIndex(block.NumberU64())
	if err != nil {
		return nil, err
	}
	collected, err := batchCounters.CombineCollectors(l1InfoIndex != 0)
	if err != nil {
		return nil, err
	}

	res, err := populateCounters(&collected, execResult, tx.GetGas(), oocError)
	if err != nil {
		return nil, err
	}

	return res, nil
}

type countersResponse struct {
	CountersUsed   combinecCounters `json:"countersUsed"`
	CoutnersLimits combinecCounters `json:"countersLimits"`
	RevertInfo     revertInfo       `json:"revertInfo"`
	OocError       string           `json:"oocError"`
}

type combinecCounters struct {
	Gas              uint64 `json:"gas"`
	KeccakHashes     int    `json:"keccakHashes"`
	Poseidonhashes   int    `json:"poseidonhashes"`
	PoseidonPaddings int    `json:"poseidonPaddings"`
	MemAligns        int    `json:"memAligns"`
	Arithmetics      int    `json:"arithmetics"`
	Binaries         int    `json:"binaries"`
	Steps            int    `json:"steps"`
	SHA256hashes     int    `json:"SHA256hashes"`
}

type revertInfo struct {
	Message string `json:"message"`
	Data    []byte `json:"data"`
}

func populateCounters(collected *vm.Counters, execResult *core.ExecutionResult, gasLimit uint64, oocError error) (json.RawMessage, error) {
	var revInfo revertInfo
	var usedGas uint64
	if execResult != nil {
		usedGas = execResult.UsedGas
		var errText string
		if execResult.Err != nil {
			errText = execResult.Err.Error()
		}
		revInfo = revertInfo{
			Message: errText,
			Data:    execResult.ReturnData,
		}
	}

	var oocErrorText string
	if oocError != nil {
		oocErrorText = oocError.Error()
	}

	res := countersResponse{
		CountersUsed: combinecCounters{
			Gas:              usedGas,
			KeccakHashes:     collected.GetKeccakHashes().Used(),
			Poseidonhashes:   collected.GetPoseidonHashes().Used(),
			PoseidonPaddings: collected.GetPoseidonPaddings().Used(),
			MemAligns:        collected.GetMemAligns().Used(),
			Arithmetics:      collected.GetArithmetics().Used(),
			Binaries:         collected.GetBinaries().Used(),
			Steps:            collected.GetSteps().Used(),
			SHA256hashes:     collected.GetSHA256Hashes().Used(),
		},
		CoutnersLimits: combinecCounters{
			Gas:              gasLimit,
			KeccakHashes:     collected.GetKeccakHashes().Limit(),
			Poseidonhashes:   collected.GetPoseidonHashes().Limit(),
			PoseidonPaddings: collected.GetPoseidonPaddings().Limit(),
			MemAligns:        collected.GetMemAligns().Limit(),
			Arithmetics:      collected.GetArithmetics().Limit(),
			Binaries:         collected.GetBinaries().Limit(),
			Steps:            collected.GetSteps().Limit(),
			SHA256hashes:     collected.GetSHA256Hashes().Limit(),
		},
		RevertInfo: revInfo,
		OocError:   oocErrorText,
	}

	resJson, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	return resJson, nil
}

func getSmtDepth(hermezDb *hermez_db.HermezDbReader, blockNum uint64, config *tracers.TraceConfig_ZkEvm) (int, error) {
	var smtDepth int
	if config != nil && config.SmtDepth != nil {
		smtDepth = *config.SmtDepth
	} else {
		depthBlockNum, smtDepth, err := hermezDb.GetClosestSmtDepth(blockNum)
		if err != nil {
			return 0, err
		}

		if depthBlockNum < blockNum {
			smtDepth += smtDepth / 10
		}

		if smtDepth == 0 || smtDepth > 256 {
			smtDepth = 256
		}
	}

	return smtDepth, nil
}

// implements zkevm_getBatchCountersByNumber - returns the batch counters for a given batch number
func (api *ZkEvmAPIImpl) GetBatchCountersByNumber(ctx context.Context, batchNumRpc rpc.BlockNumber) (res json.RawMessage, err error) {
	var (
		dbtx              kv.Tx
		chainConfig       *chain.Config
		batchBlockNumbers []uint64
		latestbatch       bool
		smtDepth          int
		batchNum,
		forkId,
		earliestBlockNum,
		latestBlockNum uint64
	)

	// setup env up until the batch
	if dbtx, err = api.db.BeginRo(ctx); err != nil {
		return nil, err
	}
	defer dbtx.Rollback()

	// get batch number from rpc
	if batchNum, _, err = rpchelper.GetBatchNumber(batchNumRpc, dbtx, api.ethApi.filters); err != nil {
		return nil, err
	}

	roHermezDb := hermez_db.NewHermezDbReader(dbtx)
	if forkId, err = roHermezDb.GetForkId(batchNum); err != nil {
		return nil, err
	}

	// get the block range to execute
	if batchBlockNumbers, err = roHermezDb.GetL2BlockNosByBatch(batchNum); err != nil {
		return nil, err
	}

	// get the earliest and latest block number
	for _, blockNum := range batchBlockNumbers {
		if earliestBlockNum == 0 || blockNum < earliestBlockNum {
			earliestBlockNum = blockNum
		}
		if blockNum > latestBlockNum {
			latestBlockNum = blockNum
		}
	}

	// if we've pruned this history away for this block then just return early
	// to save any red herring errors
	if err = api.ethApi.BaseAPI.checkPruneHistory(dbtx, latestBlockNum); err != nil {
		return nil, err
	}

	if chainConfig, err = api.ethApi.chainConfig(ctx, dbtx); err != nil {
		return nil, err
	}
	engine := api.ethApi.engine()

	// setup counters
	if smtDepth, err = getSmtDepth(roHermezDb, earliestBlockNum, nil); err != nil {
		return nil, err
	}

	batchCounters := vm.NewBatchCounterCollector(smtDepth, uint16(forkId), api.config.Zk.VirtualCountersSmtReduction, false, nil)

	var (
		block                                   *types.Block
		stateReader                             state.StateReader
		collected                               vm.Counters
		receipts                                types.Receipts
		blockGasUsed, totalGasUsed, l1InfoIndex uint64
	)

	for i, blockNum := range batchBlockNumbers {
		if l1InfoIndex, err = roHermezDb.GetBlockL1InfoTreeIndex(block.NumberU64()); err != nil {
			return nil, err
		}
		if _, err := batchCounters.StartNewBlock(l1InfoIndex != 0); err != nil {
			return nil, err
		}

		//get block with senders
		if block, err = api.ethApi.blockByNumberWithSenders(ctx, dbtx, blockNum); err != nil {
			return nil, err
		}
		if block == nil {
			return nil, fmt.Errorf("could not find block %d", blockNum)
		}

		isLatestBlock := i == len(batchBlockNumbers)-1 && latestbatch

		canBlockNumber := blockNum - 1
		if blockNum == 0 {
			canBlockNumber = 0
		}

		if stateReader, err = rpchelper.CreateStateReaderFromBlockNumber(ctx, dbtx, canBlockNumber, isLatestBlock, 0, api.ethApi.stateCache, api.ethApi.historyV3(dbtx), chainConfig.ChainName); err != nil {
			return nil, err
		}

		header := block.Header()
		ibs := state.New(stateReader)
		blockCtx := core.NewEVMBlockContext(header, core.GetHashFn(header, nil), engine, nil)
		rules := chainConfig.Rules(blockNum, header.Time)
		signer := types.MakeSigner(chainConfig, blockNum, 0)
		if receipts, err = rawdb.ReadReceiptsByHash(dbtx, header.Hash()); err != nil {
			return nil, err
		}
		// execute blocks
		var txGasUsed uint64
		for _, tx := range block.Transactions() {
			if txGasUsed, err = api.execTransaction(tx, batchCounters, smtDepth, ibs, signer, header, rules, chainConfig, blockCtx, receipts, uint16(forkId)); err != nil {
				return nil, err
			}
			blockGasUsed += txGasUsed
		}

		totalGasUsed += blockGasUsed
	}

	if collected, err = batchCounters.CombineCollectors(l1InfoIndex != 0); err != nil {
		return nil, err
	}

	return populateBatchCounters(&collected, smtDepth, batchNum, earliestBlockNum, latestBlockNum, totalGasUsed)
}

func (api *ZkEvmAPIImpl) execTransaction(
	tx types.Transaction,
	batchCounters *vm.BatchCounterCollector,
	smtDepth int,
	ibs *state.IntraBlockState,
	signer *types.Signer,
	header *types.Header,
	rules *chain.Rules,
	chainConfig *chain.Config,
	blockCtx evmtypes.BlockContext,
	receipts types.Receipts,
	forkId uint16,
) (gasUsed uint64, err error) {
	var (
		msg        core.Message
		execResult *core.ExecutionResult
	)
	txCounters := vm.NewTransactionCounter(tx, smtDepth, forkId, api.config.Zk.VirtualCountersSmtReduction, false)

	if _, err = batchCounters.AddNewTransactionCounters(txCounters); err != nil {
		return 0, err
	}

	if msg, err = tx.AsMessage(*signer, header.BaseFee, rules); err != nil {
		return 0, err
	}
	zkConfig := vm.ZkConfig{Config: vm.Config{NoBaseFee: true}, CounterCollector: txCounters.ExecutionCounters()}
	evm := vm.NewZkEVM(blockCtx, core.NewEVMTxContext(msg), ibs, chainConfig, zkConfig)
	gp := new(core.GasPool).AddGas(msg.Gas())
	ibs.Init(tx.Hash(), header.Hash(), 0)

	if execResult, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */); err != nil {
		return 0, err
	}

	// checks to see if we executed txs correctly
	receiptForTx := receipts.ReceiptForTx(tx.Hash())
	if receiptForTx == nil {
		return 0, fmt.Errorf("receipt not found for tx %s", tx.Hash().String())
	}

	if execResult == nil {
		return 0, fmt.Errorf("execResult is nil")
	}

	if (execResult.Err == nil) != (receiptForTx.Status == 1) {
		return 0, fmt.Errorf("execResult error and receipt status mismatch")
	}

	return execResult.UsedGas, nil
}

type batchCountersResponse struct {
	SmtDepth       int              `json:"smtDepth"`
	BatchNumber    uint64           `json:"batchNumber"`
	BlockFrom      uint64           `json:"blockFrom"`
	BlockTo        uint64           `json:"blockTo"`
	CountersUsed   combinecCounters `json:"countersUsed"`
	CoutnersLimits combinecCounters `json:"countersLimits"`
}

func populateBatchCounters(collected *vm.Counters, smtDepth int, batchNum, blockFrom, blockTo, totalGasUsed uint64) (jsonRes json.RawMessage, err error) {

	res := batchCountersResponse{
		SmtDepth:    smtDepth,
		BatchNumber: batchNum,
		BlockFrom:   blockFrom,
		BlockTo:     blockTo,
		CountersUsed: combinecCounters{
			Gas:              totalGasUsed,
			KeccakHashes:     collected.GetKeccakHashes().Used(),
			Poseidonhashes:   collected.GetPoseidonHashes().Used(),
			PoseidonPaddings: collected.GetPoseidonPaddings().Used(),
			MemAligns:        collected.GetMemAligns().Used(),
			Arithmetics:      collected.GetArithmetics().Used(),
			Binaries:         collected.GetBinaries().Used(),
			Steps:            collected.GetSteps().Used(),
			SHA256hashes:     collected.GetSHA256Hashes().Used(),
		},
		CoutnersLimits: combinecCounters{
			KeccakHashes:     collected.GetKeccakHashes().Limit(),
			Poseidonhashes:   collected.GetPoseidonHashes().Limit(),
			PoseidonPaddings: collected.GetPoseidonPaddings().Limit(),
			MemAligns:        collected.GetMemAligns().Limit(),
			Arithmetics:      collected.GetArithmetics().Limit(),
			Binaries:         collected.GetBinaries().Limit(),
			Steps:            collected.GetSteps().Limit(),
			SHA256hashes:     collected.GetSHA256Hashes().Limit(),
		},
	}

	return json.Marshal(res)
}
