package checker

import (
	"encoding/binary"
	"fmt"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
)

type SeekResponse struct {
	StartTxNum    uint64
	EndTxNum      uint64
	StartBlockNum uint64
	EndBlockNum   uint64
}

func wrongTxNumSeeker(tx kv.TemporalTx, txNum uint64) (resp SeekResponse, err error) {
	wrongGas, _, _, err := ReceiptAsOf(tx, txNum)
	if err != nil {
		return resp, err
	}
	for i := uint64(1); ; i++ {
		gas, _, _, err := ReceiptAsOf(tx, txNum-i)
		if err != nil {
			return resp, err
		}

		if gas != wrongGas {
			resp.StartTxNum = txNum - i
			ok, blockNum, err := rawdbv3.TxNums.FindBlockNum(tx, txNum-i)
			if err != nil {
				return resp, err
			}
			if !ok {
				return resp, fmt.Errorf("not found %d", txNum-i)
			}
			resp.StartBlockNum = blockNum
		}

		gas, _, _, err = ReceiptAsOf(tx, txNum+i)
		if err != nil {
			return resp, err
		}

		if gas != wrongGas {
			resp.EndTxNum = txNum + i
			ok, blockNum, err := rawdbv3.TxNums.FindBlockNum(tx, txNum+i)
			if err != nil {
				return resp, err
			}
			if !ok {
				return resp, fmt.Errorf("not found %d", txNum-i)
			}
			resp.EndBlockNum = blockNum
		}
	}
}

func ReceiptsCheck(tx kv.TemporalTx) (badReceipts bool, err error) {
	blockNumStart := uint64(1_000_000)
	latestBlock, _, err := rawdbv3.TxNums.Last(tx)
	if err != nil {
		return false, err
	}
	for blockNum := blockNumStart; blockNum < latestBlock; blockNum += 100_000 {
		startTxNum, err := rawdbv3.TxNums.Min(tx, blockNum-5)
		if err != nil {
			return false, err
		}
		endTxNum, err := rawdbv3.TxNums.Max(tx, blockNum+5)
		if err != nil {
			return false, err
		}
		prevGas := -1
		for txNum := startTxNum; txNum < endTxNum; txNum++ {
			gas, _, _, err := ReceiptAsOf(tx, txNum)
			if err != nil {
				return false, err
			}
			if int(gas) == prevGas {
				println("bad block:", txNum, blockNum)
				resp, err := wrongTxNumSeeker(tx, txNum)
				if err != nil {
					return true, err
				}
				println(fmt.Sprintf("wrong stat: %+v", resp))
				return true, nil
			}
			prevGas = int(gas)
		}
	}
	return false, nil
}

var (
	CumulativeGasUsedInBlockKey     = []byte{0x0}
	CumulativeBlobGasUsedInBlockKey = []byte{0x1}
	FirstLogIndexKey                = []byte{0x2}
)

func uvarint(in []byte) (res uint64) {
	res, _ = binary.Uvarint(in)
	return res
}

func ReceiptAsOf(tx kv.TemporalTx, txNum uint64) (cumGasUsed uint64, cumBlobGasused uint64, firstLogIndexWithinBlock uint32, err error) {
	var v []byte
	var ok bool

	v, ok, err = tx.GetAsOf(kv.ReceiptDomain, CumulativeGasUsedInBlockKey, txNum)
	if err != nil {
		return
	}
	if ok && v != nil {
		cumGasUsed = uvarint(v)
	}

	v, ok, err = tx.GetAsOf(kv.ReceiptDomain, CumulativeBlobGasUsedInBlockKey, txNum)
	if err != nil {
		return
	}
	if ok && v != nil {
		cumBlobGasused = uvarint(v)
	}

	//if txnIdx == 0 {
	//logIndex always 0
	//}

	v, ok, err = tx.GetAsOf(kv.ReceiptDomain, FirstLogIndexKey, txNum)
	if err != nil {
		return
	}
	if ok && v != nil {
		firstLogIndexWithinBlock = uint32(uvarint(v))
	}
	return
}
