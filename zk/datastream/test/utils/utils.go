package utils

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/iden3/go-iden3-crypto/keccak256"
	"github.com/ledgerwatch/erigon-lib/common"
)

type RequestData struct {
	Method  string   `json:"method"`
	Params  []string `json:"params"`
	ID      int      `json:"id"`
	Jsonrpc string   `json:"jsonrpc"`
}

type HTTPResponse struct {
	JsonRpc string `json:"jsonrpc"`
	Id      int    `json:"id"`
	Result  Result `json:"result"`
}

type Result struct {
	Number       string   `json:"number"`
	StateRoot    string   `json:"stateRoot"`
	Timestamp    string   `json:"timestamp"`
	Transactions []string `json:"transactions"`
}

func GetBlockByHash(blockHash string) (Result, error) {
	payloadbytecode := RequestData{
		Method:  "eth_getBlockByHash",
		Params:  []string{blockHash},
		ID:      1,
		Jsonrpc: "2.0",
	}

	jsonPayload, err := json.Marshal(payloadbytecode)
	if err != nil {
		return Result{}, err
	}

	req, err := http.NewRequest("POST", "https://zkevm-rpc.com", bytes.NewBuffer(jsonPayload))
	if err != nil {
		return Result{}, err
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return Result{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return Result{}, err
	}

	if resp.StatusCode != 200 {
		return Result{}, fmt.Errorf(string(body))
	}

	var httpResp HTTPResponse
	err = json.Unmarshal(body, &httpResp)
	if err != nil {
		return Result{}, err
	}
	result := httpResp.Result

	return result, nil
}

type GerHTTPResponse struct {
	JsonRpc string `json:"jsonrpc"`
	Id      int    `json:"id"`
	Result  string `json:"result"`
}

var GLOBAL_EXIT_ROOT_STORAGE_POS uint64 = 0

func CompareValuesString(vlockNum, ts string, ger common.Hash) error {
	//get Global Exit Root position
	gerb := make([]byte, 32)
	binary.BigEndian.PutUint64(gerb, GLOBAL_EXIT_ROOT_STORAGE_POS)

	// concat global exit root and global_exit_root_storage_pos
	rootPlusStorage := append(ger[:], gerb...)
	globalExitRootPosBytes := keccak256.Hash(rootPlusStorage)
	gerp := common.BytesToHash(globalExitRootPosBytes)
	payloadbytecode := RequestData{
		Method:  "eth_getStorageAt",
		Params:  []string{"0xa40D5f56745a118D0906a34E69aeC8C0Db1cB8fA", gerp.Hex(), "latest"},
		ID:      1,
		Jsonrpc: "2.0",
	}

	jsonPayload, err := json.Marshal(payloadbytecode)
	if err != nil {
		fmt.Println("Error preparing request:", err)
		return err
	}

	resp, err := http.Post("https://zkevm-rpc.com", "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		fmt.Println("Error sending request:", err)
		return err
	}

	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	var httpResp GerHTTPResponse
	json.Unmarshal(body, &httpResp)

	rpcTs := httpResp.Result

	ts = common.HexToHash("0x" + ts).Hex()
	if rpcTs != ts {
		return fmt.Errorf("mismatch detected for %s and key %s. Local: %s, Remote: %s", vlockNum, ger, ts, rpcTs)
	}

	return nil
}
