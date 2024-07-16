package blockinfo

import (
	"errors"
	"math/big"

	"github.com/ledgerwatch/erigon/smt/pkg/utils"
)

// SMT block header data leaf keys
const IndexBlockHeaderParamBlockHash = 0
const IndexBlockHeaderParamCoinbase = 1
const IndexBlockHeaderParamNumber = 2
const IndexBlockHeaderParamGasLimit = 3
const IndexBlockHeaderParamTimestamp = 4
const IndexBlockHeaderParamGer = 5
const IndexBlockHeaderParamBlockHashL1 = 6
const IndexBlockHeaderParamGasUsed = 7

// SMT block header constant keys
const IndexBlockHeaderParam = 7
const IndexBlockHeaderTransactionHash = 8
const IndexBlockHeaderStatus = 9
const IndexBlockHeaderCumulativeGasUsed = 10
const IndexBlockHeaderLogs = 11
const IndexBlockHeaderEffectivePercentage = 12

func KeyBlockHeaderParams(paramKey *big.Int) (*utils.NodeKey, error) {
	return utils.KeyBig(paramKey, IndexBlockHeaderParam)
}

func KeyTxLogs(txIndex, logIndex *big.Int) (*utils.NodeKey, error) {
	if txIndex == nil || logIndex == nil {
		return nil, errors.New("nil key")
	}

	txIndexKey := utils.ScalarToArrayBig(txIndex)
	key1 := utils.NodeValue8{txIndexKey[0], txIndexKey[1], txIndexKey[2], txIndexKey[3], txIndexKey[4], txIndexKey[5], big.NewInt(int64(IndexBlockHeaderLogs)), big.NewInt(0)}

	logIndexArray := utils.ScalarToArrayBig(logIndex)
	lia, err := utils.NodeValue8FromBigIntArray(logIndexArray)
	if err != nil {
		return nil, err
	}

	hk0, err := utils.Hash(lia.ToUintArray(), utils.BranchCapacity)
	if err != nil {
		return nil, err
	}
	hkRes, err := utils.Hash(key1.ToUintArray(), hk0)
	if err != nil {
		return nil, err
	}

	return &utils.NodeKey{hkRes[0], hkRes[1], hkRes[2], hkRes[3]}, nil
}

func KeyTxStatus(paramKey *big.Int) (*utils.NodeKey, error) {
	return utils.KeyBig(paramKey, IndexBlockHeaderStatus)
}

func KeyCumulativeGasUsed(paramKey *big.Int) (*utils.NodeKey, error) {
	return utils.KeyBig(paramKey, IndexBlockHeaderCumulativeGasUsed)
}

func KeyTxHash(paramKey *big.Int) (*utils.NodeKey, error) {
	return utils.KeyBig(paramKey, IndexBlockHeaderTransactionHash)
}

func KeyEffectivePercentage(paramKey *big.Int) (*utils.NodeKey, error) {
	return utils.KeyBig(paramKey, IndexBlockHeaderEffectivePercentage)
}
