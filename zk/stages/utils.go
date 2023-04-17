package stages

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"

	db2 "github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/sync_stages"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

func ShouldShortCircuitExecution(tx kv.RwTx) (bool, uint64, error) {
	intersProgress, err := sync_stages.GetStageProgress(tx, sync_stages.IntermediateHashes)
	if err != nil {
		return false, 0, err
	}

	hermezDb, err := hermez_db.NewHermezDb(tx)
	if err != nil {
		return false, 0, err
	}

	// if there is no inters progress - i.e. first sync, don't skip exec, and execute to the highest block in the highest verified batch
	if intersProgress == 0 {
		highestVerifiedBatchNo, err := sync_stages.GetStageProgress(tx, sync_stages.L1VerificationsBatchNo)
		if err != nil {
			return false, 0, err
		}

		// we could find ourselves with a batch with no blocks here, so we want to go back one batch at
		// a time until we find a batch with blocks
		max := uint64(0)
		killSwitch := 0
		for {
			max, err = hermezDb.GetHighestBlockInBatch(highestVerifiedBatchNo)
			if err != nil {
				return false, 0, err
			}
			if max != 0 {
				break
			}
			highestVerifiedBatchNo--
			killSwitch++
			if killSwitch > 100 {
				return false, 0, fmt.Errorf("could not find a batch with blocks when checking short circuit")
			}
		}

		return false, max, nil
	}

	highestHashableL2BlockNo, err := sync_stages.GetStageProgress(tx, sync_stages.HighestHashableL2BlockNo)
	if err != nil {
		return false, 0, err
	}
	highestHashableBatchNo, err := hermezDb.GetBatchNoByL2Block(highestHashableL2BlockNo)
	if err != nil {
		return false, 0, err
	}
	intersProgressBatchNo, err := hermezDb.GetBatchNoByL2Block(intersProgress)
	if err != nil {
		return false, 0, err
	}

	// check to skip execution: 1. there is inters progress, 2. the inters progress is less than the highest hashable, 3. we're in the tip batch range
	if intersProgress != 0 && intersProgress < highestHashableL2BlockNo && highestHashableBatchNo-intersProgressBatchNo <= 1 {
		return true, highestHashableL2BlockNo, nil
	}

	return false, 0, nil
}

func ShouldIncrementInterHashes(tx kv.RwTx) (bool, uint64, error) {
	return ShouldShortCircuitExecution(tx)
}

func TrimHexString(s string) string {
	s = strings.TrimPrefix(s, "0x")

	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return "0x" + s[i:]
		}
	}

	return "0x0"
}

func RpcStateRootByTxNo(rpcUrl string, txNo *big.Int) (*libcommon.Hash, error) {
	requestBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_getBlockByNumber",
		"params":  []interface{}{txNo.Uint64(), true},
		"id":      1,
	}

	requestBytes, err := json.Marshal(requestBody)
	if err != nil {
		return nil, err
	}

	response, err := http.Post("rpcUrl", "application/json", bytes.NewBuffer(requestBytes))
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()

	responseBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	responseMap := make(map[string]interface{})
	if err := json.Unmarshal(responseBytes, &responseMap); err != nil {
		return nil, err
	}

	result, ok := responseMap["result"].(map[string]interface{})
	if !ok {
		return nil, err
	}

	stateRoot, ok := result["stateRoot"].(string)
	if !ok {
		return nil, err
	}
	h := libcommon.HexToHash(stateRoot)

	return &h, nil
}

// [zkevm] - print state
func DumpDb(eridb *db2.EriDb) {
	toPrint := eridb.GetDb()
	op, err := json.Marshal(toPrint)
	if err != nil {
		fmt.Println(err)
	}
	// write to file
	if err := ioutil.WriteFile("db.json", op, 0644); err != nil {
		fmt.Println(err)
	}
}

func RpcGetHighestTxNo(rpcEndpoint string) (uint64, error) {

	data := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  "eth_getBlockByNumber",
		"params":  []interface{}{"latest", true},
		"id":      1,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return 0, err
	}

	resp, err := http.Post(rpcEndpoint, "application/json", bytes.NewBuffer(jsonData))

	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, err
	}

	number := result["result"].(map[string]interface{})["number"]
	hexString := strings.TrimPrefix(number.(string), "0x")
	val, err := strconv.ParseUint(hexString, 16, 64)
	if err != nil {
		return 0, err
	}

	return val, nil
}
