package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	zktx "github.com/ledgerwatch/erigon/zk/tx"

	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/transactions"
	"github.com/ledgerwatch/erigon/zk/hermez_db"

	"errors"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/holiman/uint256"
)

type TxInfo struct {
	chainId             *big.Int
	value               *uint256.Int
	gasPrice            *uint256.Int
	nonce               uint64
	txGasLimit          uint64
	to                  *common.Address
	from                *common.Address
	data                []byte
	l2TxHash            *common.Hash
	txIndex             int
	receipt             *types.Receipt
	logIndex            uint64
	cumulativeGasUsed   uint64
	effectivePercentage uint8
}

type BlockInfoTreeData struct {
	coinbase          common.Address
	blockNumber       uint64
	blockTime         uint64
	blockGasLimit     uint64
	blockGasUsed      uint64
	ger               *common.Hash
	l1BlockHash       *common.Hash
	previousStateRoot common.Hash
	txInfos           []TxInfo
}

func (api *ZkEvmAPIImpl) GetL2BlockInfoTree(ctx context.Context, blockNum rpc.BlockNumberOrHash) (json.RawMessage, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var (
		block    *types.Block
		number   rpc.BlockNumber
		numberOk bool
		hash     common.Hash
		hashOk   bool
	)
	if number, numberOk = blockNum.Number(); numberOk {
		block, err = api.ethApi.blockByRPCNumber(number, tx)
	} else if hash, hashOk = blockNum.Hash(); hashOk {
		block, err = api.ethApi.blockByHashWithSenders(tx, hash)
	} else {
		return nil, fmt.Errorf("invalid arguments; neither block nor hash specified")
	}

	if err != nil {
		return nil, err
	}

	if block.NumberU64() == 0 {
		return nil, fmt.Errorf("block 0 doesn't have block info tree")
	}

	previousBlock, err := api.ethApi.blockByNumberWithSenders(tx, block.NumberU64())
	if err != nil {
		return nil, err
	}
	if block == nil {
		if numberOk {
			return nil, fmt.Errorf("invalid arguments; block with number %d not found", number)
		}
		return nil, fmt.Errorf("invalid arguments; block with hash %x not found", hash)
	}
	chainConfig, err := api.ethApi.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	vmConfig := vm.NewTraceVmConfig()
	vmConfig.Debug = false
	vmConfig.NoReceipts = false

	txEnv, err := transactions.ComputeTxEnv_ZkEvm(ctx, api.ethApi._engine, block, chainConfig, api.ethApi._blockReader, tx, 0, api.ethApi.historyV3(tx))
	if err != nil {
		return nil, err
	}
	blockContext := txEnv.BlockContext
	ibs := txEnv.Ibs
	ger := txEnv.GlobalExitRoot
	l1BlockHash := txEnv.L1BlockHash

	usedGas := new(uint64)
	gp := new(core.GasPool).AddGas(block.GasLimit())
	roHermezDb := hermez_db.NewHermezDbReader(tx)

	txInfos := []TxInfo{}
	logIndex := uint64(0)
	for txIndex, tx := range block.Transactions() {
		txHash := tx.Hash()
		evm, effectiveGasPricePercentage, err := core.PrepareForTxExecution(chainConfig, &vmConfig, &blockContext, roHermezDb, ibs, block, &txHash, txIndex)
		if err != nil {
			return nil, err
		}

		receipt, execResult, err := core.ApplyTransaction_zkevm(chainConfig, api.ethApi._engine, evm, gp, ibs, state.NewNoopWriter(), block.Header(), tx, usedGas, effectiveGasPricePercentage, true)
		if err != nil {
			return nil, err
		}

		localReceipt := *receipt
		if !chainConfig.IsForkID8Elderberry(block.NumberU64()) && errors.Is(execResult.Err, vm.ErrUnsupportedPrecompile) {
			localReceipt.Status = 1
		}

		txSender, ok := tx.GetSender()
		if !ok {
			signer := types.MakeSigner(chainConfig, block.NumberU64())
			txSender, err = tx.Sender(*signer)
			if err != nil {
				return nil, err
			}
		}

		l2TxHash, err := zktx.ComputeL2TxHash(
			tx.GetChainID().ToBig(),
			tx.GetValue(),
			tx.GetPrice(),
			tx.GetNonce(),
			tx.GetGas(),
			tx.GetTo(),
			&txSender,
			tx.GetData(),
		)
		if err != nil {
			return nil, err
		}

		txInfos = append(txInfos, TxInfo{
			chainId:             tx.GetChainID().ToBig(),
			value:               tx.GetValue(),
			gasPrice:            tx.GetPrice(),
			nonce:               tx.GetNonce(),
			txGasLimit:          tx.GetGas(),
			to:                  tx.GetTo(),
			from:                &txSender,
			data:                tx.GetData(),
			l2TxHash:            &l2TxHash,
			txIndex:             txIndex,
			receipt:             &localReceipt,
			logIndex:            logIndex,
			cumulativeGasUsed:   *usedGas,
			effectivePercentage: effectiveGasPricePercentage,
		})

		logIndex += uint64(len(localReceipt.Logs))
	}

	blockInfoTreeData := BlockInfoTreeData{
		coinbase:          block.Coinbase(),
		blockNumber:       block.NumberU64(),
		blockTime:         block.Time(),
		blockGasLimit:     block.GasLimit(),
		ger:               ger,
		l1BlockHash:       l1BlockHash,
		previousStateRoot: previousBlock.Root(),
		blockGasUsed:      *usedGas,
		txInfos:           txInfos,
	}

	return populateBlockInfoTreeFields(&blockInfoTreeData)
}

func populateTxInfoFields(txInfo *TxInfo) map[string]interface{} {
	jTxInfo := map[string]interface{}{}
	jTxInfo["chainId"] = txInfo.chainId
	jTxInfo["value"] = txInfo.value
	jTxInfo["gasPrice"] = txInfo.gasPrice
	jTxInfo["nonce"] = txInfo.nonce
	jTxInfo["txGasLimit"] = txInfo.txGasLimit
	jTxInfo["to"] = txInfo.to
	jTxInfo["from"] = txInfo.from
	jTxInfo["data"] = txInfo.data
	jTxInfo["l2TxHash"] = txInfo.l2TxHash
	jTxInfo["txIndex"] = txInfo.txIndex
	jTxInfo["receipt"] = txInfo.receipt
	jTxInfo["logIndex"] = txInfo.logIndex
	jTxInfo["cumulativeGasUsed"] = txInfo.cumulativeGasUsed
	jTxInfo["effectivePercentage"] = txInfo.effectivePercentage

	return jTxInfo
}

func populateBlockInfoTreeFields(blockInfoTreeData *BlockInfoTreeData) (json.RawMessage, error) {
	jBatch := map[string]interface{}{}
	jBatch["blockNumber"] = blockInfoTreeData.blockNumber
	jBatch["coinbase"] = blockInfoTreeData.coinbase
	jBatch["blockTime"] = blockInfoTreeData.blockTime
	jBatch["blockGasLimit"] = blockInfoTreeData.blockGasLimit
	jBatch["blockGasUsed"] = blockInfoTreeData.blockGasUsed
	if blockInfoTreeData.ger != nil {
		jBatch["ger"] = blockInfoTreeData.ger
	}
	if blockInfoTreeData.l1BlockHash != nil {
		jBatch["l1BlockHash"] = blockInfoTreeData.l1BlockHash
	}
	jBatch["previousStateRoot"] = blockInfoTreeData.previousStateRoot

	txInfoFields := []map[string]interface{}{}
	for _, txInfo := range blockInfoTreeData.txInfos {
		txInfoFields = append(txInfoFields, populateTxInfoFields(&txInfo))
	}
	jBatch["txInfos"] = txInfoFields

	return json.Marshal(jBatch)
}
