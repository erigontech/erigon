package rpctest

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/state"
)

func Bench3(erigon_url, geth_url string) error {
	var client = &http.Client{
		Timeout: time.Second * 600,
	}
	blockhash := libcommon.HexToHash("0xdf15213766f00680c6a20ba76ba2cc9534435e19bc490039f3a7ef42095c8d13")
	req_id := 1

	pageSize := 256
	req_id++
	template := `{ "jsonrpc": "2.0", "method": "debug_accountRange", "params": ["0x1", "%s", %d, true, true, true], "id":%d}`

	page := libcommon.Hash{}.Bytes()

	accRangeTG := make(map[libcommon.Address]state.DumpAccount)

	for len(page) > 0 {
		encodedKey := base64.StdEncoding.EncodeToString(page)
		var sr DebugAccountRange
		if err := post(client, erigon_url, fmt.Sprintf(template, encodedKey, pageSize, req_id), &sr); err != nil {
			return fmt.Errorf("Could not get accountRange: %v\n", err)
		}
		if sr.Error != nil {
			fmt.Printf("Error getting accountRange: %d %s\n", sr.Error.Code, sr.Error.Message)
			break
		} else {
			page = sr.Result.Next
			for k, v := range sr.Result.Accounts {
				accRangeTG[k] = v
			}
		}
	}

	accRangeGeth := make(map[libcommon.Address]state.DumpAccount)

	page = libcommon.Hash{}.Bytes()
	for len(page) > 0 {
		encodedKey := base64.StdEncoding.EncodeToString(page)
		var sr DebugAccountRange
		if err := post(client, geth_url, fmt.Sprintf(template, encodedKey, pageSize, req_id), &sr); err != nil {
			return fmt.Errorf("Could not get accountRange: %v\n", err)
		}
		if sr.Error != nil {
			fmt.Printf("Error getting accountRange: %d %s\n", sr.Error.Code, sr.Error.Message)
			break
		} else {
			page = sr.Result.Next
			for k, v := range sr.Result.Accounts {
				accRangeTG[k] = v
			}
		}
	}

	if !compareAccountRanges(accRangeTG, accRangeGeth) {
		return fmt.Errorf("Different in account ranges tx\n")
	}
	fmt.Println("debug_accountRanges... OK!")

	template = `{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x%x",true],"id":%d}`
	var b EthBlockByNumber
	if err := post(client, erigon_url, fmt.Sprintf(template, 1720000, req_id), &b); err != nil {
		return fmt.Errorf("Could not retrieve block %d: %v\n", 1720000, err)
	}
	if b.Error != nil {
		fmt.Printf("Error retrieving block: %d %s\n", b.Error.Code, b.Error.Message)
	}
	for txindex := 0; txindex < 18; txindex++ {
		txhash := b.Result.Transactions[txindex].Hash
		req_id++
		template = `
		{"jsonrpc":"2.0","method":"debug_traceTransaction","params":["%s"],"id":%d}
			`
		var trace EthTxTrace
		if err := post(client, erigon_url, fmt.Sprintf(template, txhash, req_id), &trace); err != nil {
			print(client, erigon_url, fmt.Sprintf(template, txhash, req_id))
			return fmt.Errorf("Could not trace transaction %s: %v\n", txhash, err)
		}
		if trace.Error != nil {
			fmt.Printf("Error tracing transaction: %d %s\n", trace.Error.Code, trace.Error.Message)
		}
		var traceg EthTxTrace
		if err := post(client, geth_url, fmt.Sprintf(template, txhash, req_id), &traceg); err != nil {
			print(client, geth_url, fmt.Sprintf(template, txhash, req_id))
			return fmt.Errorf("Could not trace transaction g %s: %v\n", txhash, err)
		}
		if traceg.Error != nil {
			return fmt.Errorf("Error tracing transaction g: %d %s\n", traceg.Error.Code, traceg.Error.Message)
		}
		//print(client, erigon_url, fmt.Sprintf(template, txhash, req_id))
		if !compareTraces(&trace, &traceg) {
			return fmt.Errorf("Different traces block %d, tx %s\n", 1720000, txhash)
		}
	}
	to := libcommon.HexToAddress("0xbb9bc244d798123fde783fcc1c72d3bb8c189413")
	sm := make(map[libcommon.Hash]storageEntry)
	start := libcommon.HexToHash("0x5aa12c260b07325d83f0c9170a2c667948d0247cad4ad999cd00148658b0552d")

	req_id++
	template = `
		{"jsonrpc":"2.0","method":"debug_storageRangeAt","params":["0x%x", %d,"0x%x","0x%x",%d],"id":%d}
			`
	i := 18
	nextKey := &start
	for nextKey != nil {
		var sr DebugStorageRange
		if err := post(client, erigon_url, fmt.Sprintf(template, blockhash, i, to, *nextKey, 1024, req_id), &sr); err != nil {
			return fmt.Errorf("Could not get storageRange: %v\n", err)
		}
		if sr.Error != nil {
			fmt.Printf("Error getting storageRange: %d %s\n", sr.Error.Code, sr.Error.Message)
			break
		} else {
			nextKey = sr.Result.NextKey
			for k, v := range sr.Result.Storage {
				sm[k] = v
			}
		}
	}
	fmt.Printf("storageRange: %d\n", len(sm))
	smg := make(map[libcommon.Hash]storageEntry)
	nextKey = &start
	for nextKey != nil {
		var srg DebugStorageRange
		if err := post(client, geth_url, fmt.Sprintf(template, blockhash, i, to, *nextKey, 1024, req_id), &srg); err != nil {
			return fmt.Errorf("Could not get storageRange g: %v\n", err)
		}
		if srg.Error != nil {
			fmt.Printf("Error getting storageRange g: %d %s\n", srg.Error.Code, srg.Error.Message)
			break
		} else {
			nextKey = srg.Result.NextKey
			for k, v := range srg.Result.Storage {
				smg[k] = v
			}
		}
	}
	fmt.Printf("storageRange g: %d\n", len(smg))
	if !compareStorageRanges(sm, smg) {
		return fmt.Errorf("Different in storage ranges tx\n")
	}

	return nil

}
