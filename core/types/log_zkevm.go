package types

import (
	"encoding/hex"

	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
)

func (_this *Log) Clone() *Log {
	address := libcommon.Address{}
	copy(address[:], _this.Address[:])

	topics := make([]libcommon.Hash, len(_this.Topics))
	for i, t := range _this.Topics {
		copy(topics[i][:], t[:])
	}

	data := make([]byte, len(_this.Data))
	copy(data, _this.Data)

	txHash := libcommon.Hash{}
	copy(txHash[:], _this.TxHash[:])

	blockHash := libcommon.Hash{}
	copy(blockHash[:], _this.BlockHash[:])

	return &Log{
		Address:     address,
		Topics:      topics,
		Data:        data,
		BlockNumber: _this.BlockNumber,
		TxHash:      txHash,
		TxIndex:     _this.TxIndex,
		BlockHash:   blockHash,
		Index:       _this.Index,
		Removed:     _this.Removed,
	}
}

func (_this *Log) ApplyPaddingToLogsData(isForkId8, isForkId12 bool) {
	if isForkId12 {
		// we want to skip this behaviour completely for fork 12
		return
	}

	d := _this.Data
	mSize := len(d)

	if isForkId8 {
		d = applyHexPadBug(d, mSize)
	} else {
		// [zkEvm] fill 0 at the end
		lenMod32 := mSize & 31
		if lenMod32 != 0 {
			d = append(d, make([]byte, 32-lenMod32)...)
		}
	}

	_this.Data = d
}

func applyHexPadBug(d []byte, msInt int) []byte {
	fullMs := msInt

	var dLastWord []byte
	if len(d) <= 32 {
		dLastWord = append(d, make([]byte, 32-len(d))...)
		d = []byte{}
	} else {
		dLastWord, msInt = getLastWordBytes(d, fullMs)
		d = d[:len(d)-len(dLastWord)]
	}

	dataHex := hex.EncodeToString(dLastWord)

	dataHex = appendZeros(dataHex, 64)

	for len(dataHex) > 0 && dataHex[0] == '0' {
		dataHex = dataHex[1:]
	}

	if len(dataHex) < msInt*2 {
		dataHex = prependZeros(dataHex, msInt*2)
	}
	outputStr := takeFirstN(dataHex, msInt*2)
	op, _ := hex.DecodeString(outputStr) // there error is always nil here, because we prepare outputStr to have even length
	d = append(d, op...)
	d = d[:fullMs]

	return d
}

func getLastWordBytes(data []byte, originalMsInt int) ([]byte, int) {
	wordLength := 32
	dataLength := len(data)

	remainderLength := dataLength % wordLength
	if remainderLength == 0 {
		return data[dataLength-wordLength:], 32
	}

	toRemove := dataLength / wordLength

	msInt := originalMsInt - (toRemove * wordLength)

	return data[dataLength-remainderLength:], msInt
}

func prependZeros(data string, size int) string {
	for len(data) < size {
		data = "0" + data
	}
	return data
}

func takeFirstN(data string, n int) string {
	if len(data) < n {
		return data
	}
	return data[:n]
}

func appendZeros(dataHex string, targetLength int) string {
	for len(dataHex) < targetLength {
		dataHex += "0"
	}
	return dataHex
}
