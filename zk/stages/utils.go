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

	"github.com/gateway-fm/cdk-erigon-lib/common"

	"net/url"

	"github.com/ledgerwatch/erigon/core/types"
	db2 "github.com/ledgerwatch/erigon/smt/pkg/db"
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

func DeriveEffectiveGasPrice(cfg SequenceBlockCfg, tx types.Transaction) uint8 {
	if tx.GetTo() == nil {
		return cfg.zk.EffectiveGasPriceForContractDeployment
	}

	data := tx.GetData()
	dataLen := len(data)
	if dataLen != 0 {
		if dataLen >= 8 {
			// transfer's method id 0xa9059cbb
			isTransfer := data[0] == 169 && data[1] == 5 && data[2] == 156 && data[3] == 187
			// transfer's method id 0x23b872dd
			isTransferFrom := data[0] == 35 && data[1] == 184 && data[2] == 114 && data[3] == 221
			if isTransfer || isTransferFrom {
				return cfg.zk.EffectiveGasPriceForErc20Transfer
			}
		}

		return cfg.zk.EffectiveGasPriceForContractInvocation
	}

	return cfg.zk.EffectiveGasPriceForEthTransfer
}
