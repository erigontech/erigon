package blockinfo

import (
	"fmt"
	ethTypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"math/big"
)

func BuildBlockInfoTree(
	smt *smt.SMT,
	txIndex *big.Int,
	logs []*ethTypes.Log,
	logIndex *big.Int,
	status *big.Int,
	l2TxHash *big.Int,
	cumulativeGasUsed *big.Int,
	effectivePercentage *big.Int) (*big.Int, error) {

	_, err := setL2TxHash(smt, txIndex, l2TxHash)
	if err != nil {
		return nil, err
	}
	_, err = setTxStatus(smt, txIndex, status)
	if err != nil {
		return nil, err
	}
	_, err = setCumulativeGasUsed(smt, txIndex, cumulativeGasUsed)
	if err != nil {
		return nil, err
	}

	// now encode the logs
	for _, log := range logs {

		reducedTopics := ""
		for _, topic := range log.Topics {
			reducedTopics += fmt.Sprintf("%x", topic)
		}

		logToEncode := fmt.Sprintf("0x%x%s", log.Data, reducedTopics)

		hash, err := utils.HashContractBytecode(logToEncode)
		if err != nil {
			return nil, err
		}

		logEncodedBig := utils.ConvertHexToBigInt(hash)
		_, err = setTxLog(smt, txIndex, logIndex, logEncodedBig)
		if err != nil {
			return nil, err
		}

		// increment log index
		logIndex.Add(logIndex, big.NewInt(1))
	}

	_, err = setTxEffectivePercentage(smt, txIndex, effectivePercentage)
	if err != nil {
		return nil, err
	}

	return smt.LastRoot(), nil
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
