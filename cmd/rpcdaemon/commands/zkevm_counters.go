package commands

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/hexutility"
	"github.com/holiman/uint256"
	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/eth/tracers"
	db2 "github.com/ledgerwatch/erigon/smt/pkg/db"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/transactions"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

type zkevmRPCTransaction struct {
	Gas      hexutil.Uint64   `json:"gas"`
	GasPrice *hexutil.Big     `json:"gasPrice,omitempty"`
	Input    hexutility.Bytes `json:"input"`
	Nonce    hexutil.Uint64   `json:"nonce"`
	To       *common.Address  `json:"to"`
	Value    *hexutil.Big     `json:"value"`
	V        *hexutil.Big     `json:"v"`
	R        *hexutil.Big     `json:"r"`
	S        *hexutil.Big     `json:"s"`
}

// Tx return types.Transaction from rpcTransaction
func (tx *zkevmRPCTransaction) Tx() types.Transaction {
	if tx == nil {
		return nil
	}

	gasPrice := uint256.NewInt(1)
	if tx.GasPrice != nil {
		gasPrice = uint256.MustFromBig(tx.GasPrice.ToInt())
	}
	var legacy *types.LegacyTx
	if tx.To == nil {
		legacy = types.NewContractCreation(
			uint64(tx.Nonce),
			uint256.MustFromBig(tx.Value.ToInt()),
			uint64(tx.Gas),
			gasPrice,
			tx.Input,
		)
	} else {
		legacy = types.NewTransaction(
			uint64(tx.Nonce),
			*tx.To,
			uint256.MustFromBig(tx.Value.ToInt()),
			uint64(tx.Gas),
			gasPrice,
			tx.Input,
		)
	}

	if tx.V != nil && tx.R != nil && tx.S != nil {
		// parse signature raw values V, R, S from local hex strings
		legacy.V = *uint256.MustFromBig(tx.V.ToInt())
		legacy.R = *uint256.MustFromBig(tx.R.ToInt())
		legacy.S = *uint256.MustFromBig(tx.S.ToInt())
	}

	return legacy
}

// EstimateGas implements eth_estimateGas. Returns an estimate of how much gas is necessary to allow the transaction to complete. The transaction will not be added to the blockchain.
func (zkapi *ZkEvmAPIImpl) EstimateCounters(ctx context.Context, rpcTx *zkevmRPCTransaction) (json.RawMessage, error) {
	api := zkapi.ethApi

	dbtx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer dbtx.Rollback()

	tx := rpcTx.Tx()

	chainConfig, err := api.chainConfig(dbtx)
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
		block, err = api.blockWithSenders(dbtx, latestCanHash, latestCanBlockNumber)
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

	blockCtx := core.NewEVMBlockContext(header, core.GetHashFn(header, nil), engine, nil, nil)

	rules := chainConfig.Rules(block.NumberU64(), header.Time)

	signer := types.MakeSigner(chainConfig, header.Number.Uint64())

	msg, err := tx.AsMessage(*signer, header.BaseFee, rules)
	if err != nil {
		return nil, err
	}
	txCtx := core.NewEVMTxContext(msg)

	eriDb := db2.NewRoEriDb(dbtx)
	smt := smt.NewRoSMT(eriDb)
	hermezDb := hermez_db.NewHermezDbReader(dbtx)

	forkId, err := hermezDb.GetForkIdByBlockNum(block.NumberU64())
	if err != nil {
		return nil, err
	}

	smtDepth := smt.GetDepth()

	batchCounters := vm.NewBatchCounterCollector(smtDepth, uint16(forkId), false)
	txCounters := vm.NewTransactionCounter(tx, smtDepth, false)

	_, err = batchCounters.AddNewTransactionCounters(txCounters)
	if err != nil {
		return nil, err
	}

	zkConfig := vm.ZkConfig{Config: vm.Config{NoBaseFee: true}, CounterCollector: txCounters.ExecutionCounters()}
	evm := vm.NewZkEVM(blockCtx, txCtx, ibs, chainConfig, zkConfig)

	gp := new(core.GasPool).AddGas(msg.Gas())

	ibs.Prepare(tx.Hash(), header.Hash(), 0)

	execResult, oocError := core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)

	collected, err := batchCounters.CombineCollectors()
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

// TraceTransaction implements debug_traceTransaction. Returns Geth style transaction traces.
func (api *ZkEvmAPIImpl) TraceTransactionCounters(ctx context.Context, hash common.Hash, config *tracers.TraceConfig_ZkEvm, stream *jsoniter.Stream) error {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	defer tx.Rollback()
	chainConfig, err := api.ethApi.chainConfig(tx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	// Retrieve the transaction and assemble its EVM context
	blockNum, ok, err := api.ethApi.txnLookup(ctx, tx, hash)
	if err != nil {
		stream.WriteNil()
		return err
	}
	if !ok {
		stream.WriteNil()
		return nil
	}

	// check pruning to ensure we have history at this block level
	if err = api.ethApi.BaseAPI.checkPruneHistory(tx, blockNum); err != nil {
		stream.WriteNil()
		return err
	}

	// Private API returns 0 if transaction is not found.
	if blockNum == 0 && chainConfig.Bor != nil {
		blockNumPtr, err := rawdb.ReadBorTxLookupEntry(tx, hash)
		if err != nil {
			stream.WriteNil()
			return err
		}
		if blockNumPtr == nil {
			stream.WriteNil()
			return nil
		}
		blockNum = *blockNumPtr
	}
	block, err := api.ethApi.blockByNumberWithSenders(tx, blockNum)
	if err != nil {
		stream.WriteNil()
		return err
	}
	if block == nil {
		stream.WriteNil()
		return nil
	}
	var txnIndex uint64
	var txn types.Transaction
	for i, transaction := range block.Transactions() {
		if transaction.Hash() == hash {
			txnIndex = uint64(i)
			txn = transaction
			break
		}
	}
	if txn == nil {
		borTx, _, _, _, err := rawdb.ReadBorTransaction(tx, hash)
		if err != nil {
			stream.WriteNil()
			return err
		}

		if borTx != nil {
			stream.WriteNil()
			return nil
		}
		stream.WriteNil()
		return fmt.Errorf("transaction %#x not found", hash)
	}
	engine := api.ethApi.engine()

	txEnv, err := transactions.ComputeTxEnv_ZkEvm(ctx, engine, block, chainConfig, api.ethApi._blockReader, tx, int(txnIndex), api.ethApi.historyV3(tx))
	if err != nil {
		stream.WriteNil()
		return err
	}

	// counters work
	hermezDb := hermez_db.NewHermezDbReader(tx)
	forkId, err := hermezDb.GetForkIdByBlockNum(blockNum)
	if err != nil {
		stream.WriteNil()
		return err
	}

	smtDepth, err := getSmtDepth(hermezDb, blockNum, config)
	if err != nil {
		stream.WriteNil()
		return err
	}

	txCounters := vm.NewTransactionCounter(txn, int(smtDepth), false)
	batchCounters := vm.NewBatchCounterCollector(int(smtDepth), uint16(forkId), false)
	if _, err = batchCounters.AddNewTransactionCounters(txCounters); err != nil {
		stream.WriteNil()
		return err
	}

	// set tracer to counter tracer
	if config == nil {
		config = &tracers.TraceConfig_ZkEvm{}
	}
	config.CounterCollector = txCounters.ExecutionCounters()

	// Trace the transaction and return
	return transactions.TraceTx(ctx, txEnv.Msg, txEnv.BlockContext, txEnv.TxContext, txEnv.Ibs, config, chainConfig, stream, api.ethApi.evmCallTimeout)
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
