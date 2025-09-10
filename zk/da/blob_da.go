package da

import (
	"encoding/json"
	"fmt"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/zk/hermez_db"
	"github.com/erigontech/erigon/zk/tx"
	"github.com/erigontech/erigon/zkevm/jsonrpc/client"
)

type OffChainBlob struct {
	TxHash      string      `json:"txHash"`
	BlobInputs  []BlobInput `json:"blobInputs"`
	StartBatch  uint64      `json:"startBatch"`
	EndBatch    uint64      `json:"endBatch"`
	Compression uint64      `json:"compression"`
	Timestamp   int64       `json:"timestamp"`
}

type BlobInput struct {
	BatchNumber    string         `json:"batchNumber"`
	Coinbase       common.Address `json:"coinbase"`
	LimitTimestamp string         `json:"limitTimestamp"`
	GER            common.Hash    `json:"globalExitRoot"`
	BatchL2Data    string         `json:"batchL2Data"`
}

func GetOffChainBlobs(url string, limit, offset uint64) ([]*OffChainBlob, bool, error) {
	res, err := client.JSONRPCCall(url, "data_getOffChainBlobs", limit, offset)
	if err != nil {
		return nil, false, err
	}

	var ocb []*OffChainBlob
	if err = json.Unmarshal(res.Result, &ocb); err != nil {
		return nil, false, err
	}

	// we have reached the end of the blobs
	if ocb == nil {
		return nil, true, nil
	}

	return ocb, false, nil
}

func CreateL1BatchDataFromBlobInput(hermezDb *hermez_db.HermezDb, input BlobInput) (uint64, []byte, error) {
	batchNumber, err := hexutil.DecodeUint64(input.BatchNumber)
	if err != nil {
		return 0, nil, err
	}

	batchData, err := hexutil.Decode(input.BatchL2Data)
	if err != nil {
		return 0, nil, err
	}

	limitTimestamp, err := hexutil.Decode(input.LimitTimestamp)
	if err != nil {
		return 0, nil, err
	}

	allForks, allBatches, err := hermezDb.GetAllForkHistory()
	if err != nil {
		return 0, nil, err
	}

	var forkId uint64
	for idx, batch := range allBatches {
		if batchNumber >= batch {
			forkId = allForks[idx]
		}
	}

	decodedBatchL2Data, err := tx.DecodeBatchL2Blocks(batchData, forkId)
	if err != nil {
		return 0, nil, err
	}

	var highestIndexUsed uint32
	for _, block := range decodedBatchL2Data {
		if block.L1InfoTreeIndex > highestIndexUsed {
			highestIndexUsed = block.L1InfoTreeIndex
		}
	}

	l1InfoRoot, err := hermezDb.GetL1InfoRootByIndex(uint64(highestIndexUsed))
	if err != nil {
		return 0, nil, err
	}

	// coinbase + l1InfoRoot + limitTimestamp + batchData
	size := 20 + 32 + 8 + len(batchData)

	// smart contract size limit
	if size > 120_000 {
		return 0, nil, fmt.Errorf("l1 batch data is too large: %d", size)
	}

	data := make([]byte, size)
	copy(data, input.Coinbase.Bytes())
	copy(data[20:], l1InfoRoot.Bytes())
	copy(data[52:], limitTimestamp)
	copy(data[60:], batchData)

	return batchNumber, data, nil
}
