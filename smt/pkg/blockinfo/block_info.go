package blockinfo

import (
	"context"
	"fmt"
	"math/big"

	ethTypes "github.com/ledgerwatch/erigon/core/types"

	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	zktx "github.com/ledgerwatch/erigon/zk/tx"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/log/v3"
)

type ExecutedTxInfo struct {
	Tx                ethTypes.Transaction
	EffectiveGasPrice uint8
	Receipt           *ethTypes.Receipt
	Signer            *common.Address
}

func BuildBlockInfoTree(
	coinbase *common.Address,
	blockNumber,
	blockTime,
	blockGasLimit,
	blockGasUsed uint64,
	ger common.Hash,
	l1BlockHash common.Hash,
	previousStateRoot common.Hash,
	transactionInfos *[]ExecutedTxInfo,
) (*common.Hash, error) {
	infoTree := NewBlockInfoTree()
	if err := infoTree.InitBlockHeader(&previousStateRoot, coinbase, blockNumber, blockGasLimit, blockTime, &ger, &l1BlockHash); err != nil {
		return nil, err
	}
	log.Trace("info-tree-header",
		"blockNumber", blockNumber,
		"previousStateRoot", previousStateRoot.String(),
		"coinbase", coinbase.String(),
		"blockGasLimit", blockGasLimit,
		"blockTime", blockTime,
		"ger", ger.String(),
		"l1BlockHash", l1BlockHash.String(),
	)
	var err error
	var logIndex int64 = 0
	var keys []*utils.NodeKey
	var vals []*utils.NodeValue8
	for i, txInfo := range *transactionInfos {
		receipt := txInfo.Receipt
		t := txInfo.Tx

		l2TxHash, err := zktx.ComputeL2TxHash(
			t.GetChainID().ToBig(),
			t.GetValue(),
			t.GetPrice(),
			t.GetNonce(),
			t.GetGas(),
			t.GetTo(),
			txInfo.Signer,
			t.GetData(),
		)
		if err != nil {
			return nil, err
		}

		log.Trace("info-tree-tx", "block", blockNumber, "idx", i, "hash", l2TxHash.String())

		genKeys, genVals, err := infoTree.GenerateBlockTxKeysVals(&l2TxHash, i, receipt, logIndex, receipt.CumulativeGasUsed, txInfo.EffectiveGasPrice)
		if err != nil {
			return nil, err
		}
		keys = append(keys, genKeys...)
		vals = append(vals, genVals...)

		logIndex += int64(len(receipt.Logs))
	}

	key, val, err := generateBlockGasUsed(blockGasUsed)
	if err != nil {
		return nil, err
	}
	keys = append(keys, key)
	vals = append(vals, val)

	root, err := infoTree.smt.InsertBatch(context.Background(), "", keys, vals, nil, nil)
	if err != nil {
		return nil, err
	}
	rootHash := common.BigToHash(root.NewRootScalar.ToBigInt())

	log.Trace("info-tree-root", "block", blockNumber, "root", rootHash.String())

	return &rootHash, nil
}

type BlockInfoTree struct {
	smt *smt.SMT
}

func NewBlockInfoTree() *BlockInfoTree {
	return &BlockInfoTree{
		smt: smt.NewSMT(nil),
	}
}
func (b *BlockInfoTree) GetRoot() *big.Int {
	return b.smt.LastRoot()
}

func (b *BlockInfoTree) InitBlockHeader(oldBlockHash *common.Hash, coinbase *common.Address, blockNumber, gasLimit, timestamp uint64, ger, l1BlochHash *common.Hash) error {
	_, err := setL2BlockHash(b.smt, oldBlockHash)
	if err != nil {
		return err
	}
	_, err = setCoinbase(b.smt, coinbase)
	if err != nil {
		return err
	}

	_, err = setBlockNumber(b.smt, blockNumber)
	if err != nil {
		return err
	}

	_, err = setGasLimit(b.smt, gasLimit)
	if err != nil {
		return err
	}
	_, err = setTimestamp(b.smt, timestamp)
	if err != nil {
		return err
	}
	_, err = setGer(b.smt, ger)
	if err != nil {
		return err
	}
	_, err = setL1BlockHash(b.smt, l1BlochHash)
	if err != nil {
		return err
	}
	return nil
}

func (b *BlockInfoTree) SetBlockTx(
	l2TxHash *common.Hash,
	txIndex int,
	receipt *ethTypes.Receipt,
	logIndex int64,
	cumulativeGasUsed uint64,
	effectivePercentage uint8,
) (*big.Int, error) {
	txIndexBig := big.NewInt(int64(txIndex))
	_, err := setL2TxHash(b.smt, txIndexBig, l2TxHash.Big())
	if err != nil {
		return nil, err
	}

	bigStatus := big.NewInt(0).SetUint64(receipt.Status)
	_, err = setTxStatus(b.smt, txIndexBig, bigStatus)
	if err != nil {
		return nil, err
	}

	bigCumulativeGasUsed := big.NewInt(0).SetUint64(cumulativeGasUsed)
	_, err = setCumulativeGasUsed(b.smt, txIndexBig, bigCumulativeGasUsed)
	if err != nil {
		return nil, err
	}

	log.Trace("info-tree-tx-inner",
		"tx-index", txIndex,
		"log-index", logIndex,
		"cumulativeGasUsed", cumulativeGasUsed,
		"effective-percentage", effectivePercentage,
		"receipt-status", receipt.Status,
	)

	// now encode the logs
	for _, rLog := range receipt.Logs {
		reducedTopics := ""
		for _, topic := range rLog.Topics {
			reducedTopics += fmt.Sprintf("%x", topic)
		}

		logToEncode := fmt.Sprintf("0x%x%s", rLog.Data, reducedTopics)

		hash, err := utils.HashContractBytecode(logToEncode)
		if err != nil {
			return nil, err
		}

		logEncodedBig := utils.ConvertHexToBigInt(hash)
		_, err = setTxLog(b.smt, txIndexBig, big.NewInt(logIndex), logEncodedBig)
		if err != nil {
			return nil, err
		}

		log.Trace("info-tree-tx-receipt-log",
			"topics", reducedTopics,
			"to-encode", logToEncode,
			"log-index", logIndex,
		)

		// increment log index
		logIndex += 1
	}

	bigEffectivePercentage := big.NewInt(0).SetUint64(uint64(effectivePercentage))
	root, err := setTxEffectivePercentage(b.smt, txIndexBig, bigEffectivePercentage)
	if err != nil {
		return nil, err
	}

	return root, nil
}

func (b *BlockInfoTree) SetBlockGasUsed(gasUsed uint64) (*big.Int, error) {
	key, err := KeyBlockHeaderParams(big.NewInt(IndexBlockHeaderParamGasUsed))
	if err != nil {
		return nil, err
	}
	gasUsedBig := big.NewInt(0).SetUint64(gasUsed)
	resp, err := b.smt.InsertKA(key, gasUsedBig)
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setL2TxHash(smt *smt.SMT, txIndex *big.Int, l2TxHash *big.Int) (*big.Int, error) {
	key, err := KeyTxHash(txIndex)
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, l2TxHash)
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setTxStatus(smt *smt.SMT, txIndex *big.Int, status *big.Int) (*big.Int, error) {
	key, err := KeyTxStatus(txIndex)
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, status)
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setCumulativeGasUsed(smt *smt.SMT, txIndex, cumulativeGasUsed *big.Int) (*big.Int, error) {
	key, err := KeyCumulativeGasUsed(txIndex)
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, cumulativeGasUsed)
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setTxEffectivePercentage(smt *smt.SMT, txIndex, effectivePercentage *big.Int) (*big.Int, error) {
	key, err := KeyEffectivePercentage(txIndex)
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, effectivePercentage)
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setTxLog(smt *smt.SMT, txIndex *big.Int, logIndex *big.Int, log *big.Int) (*big.Int, error) {
	key, err := KeyTxLogs(txIndex, logIndex)
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, log)
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setL2BlockHash(smt *smt.SMT, blockHash *common.Hash) (*big.Int, error) {
	key, err := KeyBlockHeaderParams(big.NewInt(IndexBlockHeaderParamBlockHash))
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, blockHash.Big())
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setCoinbase(smt *smt.SMT, coinbase *common.Address) (*big.Int, error) {
	key, err := KeyBlockHeaderParams(big.NewInt(IndexBlockHeaderParamCoinbase))
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, coinbase.Hash().Big())
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setGasLimit(smt *smt.SMT, gasLimit uint64) (*big.Int, error) {
	key, err := KeyBlockHeaderParams(big.NewInt(IndexBlockHeaderParamGasLimit))
	if err != nil {
		return nil, err
	}
	gasLimitBig := big.NewInt(0).SetUint64(gasLimit)

	resp, err := smt.InsertKA(key, gasLimitBig)
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setBlockNumber(smt *smt.SMT, blockNumber uint64) (*big.Int, error) {
	key, err := KeyBlockHeaderParams(big.NewInt(IndexBlockHeaderParamNumber))
	if err != nil {
		return nil, err
	}

	blockNumberBig := big.NewInt(0).SetUint64(blockNumber)
	resp, err := smt.InsertKA(key, blockNumberBig)
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setTimestamp(smt *smt.SMT, timestamp uint64) (*big.Int, error) {
	key, err := KeyBlockHeaderParams(big.NewInt(IndexBlockHeaderParamTimestamp))
	if err != nil {
		return nil, err
	}
	timestampBig := big.NewInt(0).SetUint64(timestamp)

	resp, err := smt.InsertKA(key, timestampBig)
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setGer(smt *smt.SMT, ger *common.Hash) (*big.Int, error) {
	key, err := KeyBlockHeaderParams(big.NewInt(IndexBlockHeaderParamGer))
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, ger.Big())
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func setL1BlockHash(smt *smt.SMT, blockHash *common.Hash) (*big.Int, error) {
	key, err := KeyBlockHeaderParams(big.NewInt(IndexBlockHeaderParamBlockHashL1))
	if err != nil {
		return nil, err
	}
	resp, err := smt.InsertKA(key, blockHash.Big())
	if err != nil {
		return nil, err
	}

	return resp.NewRootScalar.ToBigInt(), nil
}

func bigInt2NodeVal8(val *big.Int) (*utils.NodeValue8, error) {
	x := utils.ScalarToArrayBig(val)
	v, err := utils.NodeValue8FromBigIntArray(x)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func generateL2TxHash(txIndex *big.Int, l2TxHash *big.Int) (*utils.NodeKey, *utils.NodeValue8, error) {
	key, err := KeyTxHash(txIndex)
	if err != nil {
		return nil, nil, err
	}
	val, err := bigInt2NodeVal8(l2TxHash)
	if err != nil {
		return nil, nil, err
	}

	return &key, val, nil
}

func generateTxStatus(txIndex *big.Int, status *big.Int) (*utils.NodeKey, *utils.NodeValue8, error) {
	key, err := KeyTxStatus(txIndex)
	if err != nil {
		return nil, nil, err
	}
	val, err := bigInt2NodeVal8(status)
	if err != nil {
		return nil, nil, err
	}

	return &key, val, nil
}

func generateCumulativeGasUsed(txIndex, cumulativeGasUsed *big.Int) (*utils.NodeKey, *utils.NodeValue8, error) {
	key, err := KeyCumulativeGasUsed(txIndex)
	if err != nil {
		return nil, nil, err
	}
	val, err := bigInt2NodeVal8(cumulativeGasUsed)
	if err != nil {
		return nil, nil, err
	}
	return &key, val, nil
}

func generateTxLog(txIndex *big.Int, logIndex *big.Int, log *big.Int) (*utils.NodeKey, *utils.NodeValue8, error) {
	key, err := KeyTxLogs(txIndex, logIndex)
	if err != nil {
		return nil, nil, err
	}
	val, err := bigInt2NodeVal8(log)
	if err != nil {
		return nil, nil, err
	}

	return &key, val, nil
}

func generateTxEffectivePercentage(txIndex, effectivePercentage *big.Int) (*utils.NodeKey, *utils.NodeValue8, error) {
	key, err := KeyEffectivePercentage(txIndex)
	if err != nil {
		return nil, nil, err
	}
	val, err := bigInt2NodeVal8(effectivePercentage)
	if err != nil {
		return nil, nil, err
	}

	return &key, val, nil
}

func generateBlockGasUsed(gasUsed uint64) (*utils.NodeKey, *utils.NodeValue8, error) {
	key, err := KeyBlockHeaderParams(big.NewInt(IndexBlockHeaderParamGasUsed))
	if err != nil {
		return nil, nil, err
	}
	gasUsedBig := big.NewInt(0).SetUint64(gasUsed)
	val, err := bigInt2NodeVal8(gasUsedBig)
	if err != nil {
		return nil, nil, err
	}

	return &key, val, nil
}

func (b *BlockInfoTree) GenerateBlockTxKeysVals(
	l2TxHash *common.Hash,
	txIndex int,
	receipt *ethTypes.Receipt,
	logIndex int64,
	cumulativeGasUsed uint64,
	effectivePercentage uint8,
) ([]*utils.NodeKey, []*utils.NodeValue8, error) {
	var keys []*utils.NodeKey
	var vals []*utils.NodeValue8
	txIndexBig := big.NewInt(int64(txIndex))

	key, val, err := generateL2TxHash(txIndexBig, l2TxHash.Big())
	if err != nil {
		return nil, nil, err
	}
	keys = append(keys, key)
	vals = append(vals, val)

	bigStatus := big.NewInt(0).SetUint64(receipt.Status)
	key, val, err = generateTxStatus(txIndexBig, bigStatus)
	if err != nil {
		return nil, nil, err
	}
	keys = append(keys, key)
	vals = append(vals, val)

	bigCumulativeGasUsed := big.NewInt(0).SetUint64(cumulativeGasUsed)
	key, val, err = generateCumulativeGasUsed(txIndexBig, bigCumulativeGasUsed)
	if err != nil {
		return nil, nil, err
	}
	keys = append(keys, key)
	vals = append(vals, val)

	log.Trace("info-tree-tx-inner",
		"tx-index", txIndex,
		"log-index", logIndex,
		"cumulativeGasUsed", cumulativeGasUsed,
		"effective-percentage", effectivePercentage,
		"receipt-status", receipt.Status,
	)

	// now encode the logs
	for _, rLog := range receipt.Logs {
		reducedTopics := ""
		for _, topic := range rLog.Topics {
			reducedTopics += fmt.Sprintf("%x", topic)
		}

		logToEncode := fmt.Sprintf("0x%x%s", rLog.Data, reducedTopics)

		hash, err := utils.HashContractBytecode(logToEncode)
		if err != nil {
			return nil, nil, err
		}

		logEncodedBig := utils.ConvertHexToBigInt(hash)
		key, val, err = generateTxLog(txIndexBig, big.NewInt(logIndex), logEncodedBig)
		keys = append(keys, key)
		vals = append(vals, val)

		log.Trace("info-tree-tx-receipt-log",
			"topics", reducedTopics,
			"to-encode", logToEncode,
			"log-index", logIndex,
		)

		// increment log index
		logIndex += 1
	}

	// setTxEffectivePercentage
	bigEffectivePercentage := big.NewInt(0).SetUint64(uint64(effectivePercentage))
	key, val, err = generateTxEffectivePercentage(txIndexBig, bigEffectivePercentage)
	if err != nil {
		return nil, nil, err
	}
	keys = append(keys, key)
	vals = append(vals, val)

	return keys, vals, nil
}
