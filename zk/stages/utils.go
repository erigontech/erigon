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

	"net/url"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	db2 "github.com/erigontech/erigon/smt/pkg/db"
	jsonClient "github.com/erigontech/erigon/zkevm/jsonrpc/client"
	jsonTypes "github.com/erigontech/erigon/zkevm/jsonrpc/types"
)

const (
	SEQUENCER_DATASTREAM_RPC_CALL = "zkevm_getLatestDataStreamBlock"
	BATCH_NUMBER_BY_BLOCK_NUMBER  = "zkevm_batchNumberByBlockNumber"
	ZK_BLOCK_BY_NUMBER            = "zkevm_getFullBlockByNumber"
)

func TrimHexString(s string) string {
	s = strings.TrimPrefix(s, "0x")

	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return "0x" + s[i:]
		}
	}

	return "0x0"
}

func RpcStateRootByTxNo(rpcUrl string, txNo *big.Int) (*common.Hash, error) {
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
	h := common.HexToHash(stateRoot)

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
	if err := ioutil.WriteFile("db.json", op, 0600); err != nil {
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

	safeUrl, err := url.Parse(rpcEndpoint)
	if err != nil {
		return 0, err
	}
	resp, err := http.Post(safeUrl.String(), "application/json", bytes.NewBuffer(jsonData))

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

func GetSequencerHighestDataStreamBlock(endpoint string) (uint64, error) {
	res, err := jsonClient.JSONRPCCall(endpoint, SEQUENCER_DATASTREAM_RPC_CALL)
	if err != nil {
		return 0, err
	}

	return trimHexAndHandleUint64Result(res)
}

type l2BlockReaderRpc struct {
}

func (l2BlockReaderRpc) GetZKBlockByNumberHash(endpoint string, blockNo uint64) (common.Hash, error) {
	asHex := fmt.Sprintf("0x%x", blockNo)
	res, err := jsonClient.JSONRPCCall(endpoint, ZK_BLOCK_BY_NUMBER, asHex, false)
	if err != nil {
		return common.Hash{}, err
	}

	type ZkBlock struct {
		Hash common.Hash `json:"hash"`
	}

	var zkBlock ZkBlock
	if err := json.Unmarshal(res.Result, &zkBlock); err != nil {
		return common.Hash{}, err
	}

	return zkBlock.Hash, nil
}

func (l2BlockReaderRpc) GetBatchNumberByBlockNumber(endpoint string, blockNo uint64) (uint64, error) {
	asHex := fmt.Sprintf("0x%x", blockNo)
	res, err := jsonClient.JSONRPCCall(endpoint, BATCH_NUMBER_BY_BLOCK_NUMBER, asHex)
	if err != nil {
		return 0, err
	}

	return trimHexAndHandleUint64Result(res)
}

func trimHexAndHandleUint64Result(res jsonTypes.Response) (uint64, error) {
	// hash comes in escaped quotes, so we trim them here
	// \"0x1234\" -> 0x1234
	hashHex := strings.Trim(string(res.Result), "\"")

	// now convert to a uint
	decoded, err := hexutil.DecodeUint64(hashHex)
	if err != nil {
		return 0, err
	}

	return decoded, nil
}
