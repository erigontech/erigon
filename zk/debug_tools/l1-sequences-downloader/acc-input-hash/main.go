package main

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/zk/syncer"
	"github.com/ledgerwatch/erigon/zk/types"
)

func main() {
	fileSeq, err := os.Open("sequencesMainnet.json")
	if err != nil {
		panic(err)
	}
	defer fileSeq.Close()
	sequences := make([]types.L1BatchInfo, 0)

	encSeq := json.NewDecoder(fileSeq)
	if err := encSeq.Decode(&sequences); err != nil {
		panic(err)
	}
	fileSeq.Close()
	fileCalldata, err := os.Open("calldataMainnet.json")
	if err != nil {
		panic(err)
	}
	defer fileCalldata.Close()
	calldata := make(map[string]string)

	encCalldata := json.NewDecoder(fileCalldata)
	if err := encCalldata.Decode(&calldata); err != nil {
		panic(err)
	}
	fileCalldata.Close()
	fileAccInputHash, err := os.Open("accInputHashesMainnet.json")
	if err != nil {
		panic(err)
	}
	defer fileAccInputHash.Close()
	accInputHashes := make(map[uint64]string)

	encAccInputHash := json.NewDecoder(fileAccInputHash)
	if err := encAccInputHash.Decode(&accInputHashes); err != nil {
		panic(err)
	}
	fileAccInputHash.Close()

	for i := 0; i < 40000; i++ {
		delete(calldata, sequences[i].L1TxHash.String())
		delete(accInputHashes, sequences[i].BatchNo)
	}

	prevSeq := sequences[40001]
	for i := 40002; i < len(sequences); i++ {
		nextSeq := sequences[i]
		nextCalldata, ok := calldata[nextSeq.L1TxHash.String()]
		if !ok {
			panic(fmt.Errorf("calldata for tx %s not found", nextSeq.L1TxHash.String()))
		}
		prevAccInputHash, ok := accInputHashes[prevSeq.BatchNo]
		if !ok {
			panic(fmt.Errorf("accInputHash for batch %d not found", prevSeq.BatchNo))
		}
		lastAccInputHash, ok := accInputHashes[nextSeq.BatchNo]
		if !ok {
			panic(fmt.Errorf("accInputHash for batch %d not found", nextSeq.BatchNo))
		}

		decodedSequenceInterface, err := syncer.DecodeSequenceBatchesCalldata(common.FromHex(nextCalldata))
		if err != nil {
			panic(fmt.Errorf("failed to decode calldata for tx %s: %w", nextSeq.L1TxHash, err))
		}

		accInputHashCalcFn, totalSequenceBatches, err := syncer.GetAccInputDataCalcFunction(nextSeq.L1InfoRoot, decodedSequenceInterface)
		if err != nil {
			panic(fmt.Errorf("failed to get accInputHash calculation func: %w", err))
		}

		if totalSequenceBatches == 0 || nextSeq.BatchNo-prevSeq.BatchNo > uint64(totalSequenceBatches) {
			panic(fmt.Errorf("batch %d is out of range of sequence calldata: %d %d", nextSeq.BatchNo, prevSeq.BatchNo, totalSequenceBatches))
		}

		prevAccInputBigInt := new(big.Int).SetBytes(common.FromHex(prevAccInputHash))
		preVAccInputHash := common.BigToHash(prevAccInputBigInt)
		accInputHash := &preVAccInputHash
		// calculate acc input hash
		for i := 0; i < int(nextSeq.BatchNo-prevSeq.BatchNo); i++ {
			accInputHash = accInputHashCalcFn(*accInputHash, i)
		}

		if accInputHash.Hex() != lastAccInputHash {
			panic(fmt.Errorf("accInputHash for tx %s and batchNum %d does not match", nextSeq.L1TxHash.String(), nextSeq.BatchNo))
		}

		prevSeq = nextSeq
		if i%1000 == 0 {
			fmt.Println(i, " sequence checked: ", nextSeq.BatchNo)
		}
	}

}
