package rpctest

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
)

type CallResult struct {
	Target      string
	Took        time.Duration
	RequestID   int
	Method      string
	RequestBody string
	Err         error
}
type RequestGenerator struct {
	reqID  int
	client *http.Client
}

func (g *RequestGenerator) blockNumber() string {
	const template = `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":%d}`
	return fmt.Sprintf(template, g.reqID)
}
func (g *RequestGenerator) getBlockByNumber(blockNum int) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x%x",true],"id":%d}`
	return fmt.Sprintf(template, blockNum, g.reqID)
}

func (g *RequestGenerator) storageRangeAt(hash common.Hash, i int, to *common.Address, nextKey common.Hash) string {
	const template = `{"jsonrpc":"2.0","method":"debug_storageRangeAt","params":["0x%x", %d,"0x%x","0x%x",%d],"id":%d}`
	return fmt.Sprintf(template, hash, i, to, nextKey, 1024, g.reqID)
}

func (g *RequestGenerator) traceTransaction(hash string) string {
	const template = `{"jsonrpc":"2.0","method":"debug_traceTransaction","params":["%s"],"id":%d}`
	return fmt.Sprintf(template, hash, g.reqID)
}

func (g *RequestGenerator) getTransactionReceipt(hash string) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["%s"],"id":%d}`
	return fmt.Sprintf(template, hash, g.reqID)
}

func (g *RequestGenerator) getBalance(miner common.Address, bn int) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x%x", "0x%x"],"id":%d}`
	return fmt.Sprintf(template, miner, bn, g.reqID)
}

func (g *RequestGenerator) getModifiedAccountsByNumber(prevBn int, bn int) string {
	const template = `{"jsonrpc":"2.0","method":"debug_getModifiedAccountsByNumber","params":[%d, %d],"id":%d}`
	return fmt.Sprintf(template, prevBn, bn, g.reqID)
}

func (g *RequestGenerator) getLogs(prevBn int, bn int, account common.Address) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x%x", "toBlock": "0x%x", "address": "0x%x"}],"id":%d}`
	return fmt.Sprintf(template, prevBn, bn, account, g.reqID)
}

func (g *RequestGenerator) getLogs1(prevBn int, bn int, account common.Address, topic common.Hash) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x%x", "toBlock": "0x%x", "address": "0x%x", "topics": ["0x%x"]}],"id":%d}`
	return fmt.Sprintf(template, prevBn, bn, account, topic, g.reqID)
}

func (g *RequestGenerator) getLogs2(prevBn int, bn int, account common.Address, topic1, topic2 common.Hash) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x%x", "toBlock": "0x%x", "address": "0x%x", "topics": ["0x%x", "0x%x"]}],"id":%d}`
	return fmt.Sprintf(template, prevBn, bn, account, topic1, topic2, g.reqID)
}

func (g *RequestGenerator) accountRange(bn int, page []byte, num int) string { //nolint
	const template = `{ "jsonrpc": "2.0", "method": "debug_accountRange", "params": ["0x%x", "%s", %d, false, false, false], "id":%d}`
	encodedKey := base64.StdEncoding.EncodeToString(page)
	return fmt.Sprintf(template, bn, encodedKey, num, g.reqID)
}

func (g *RequestGenerator) getProof(bn int, account common.Address, storageList []common.Hash) string {
	const template = `{ "jsonrpc": "2.0", "method": "eth_getProof", "params": ["0x%x", [%s], "0x%x"], "id":%d}`
	var storageStr = make([]string, len(storageList))
	for i, location := range storageList {
		storageStr[i] = fmt.Sprintf(`"x%x"`, location)
	}
	return fmt.Sprintf(template, account, strings.Join(storageStr, ","), bn, g.reqID)
}

func (g *RequestGenerator) traceCall(from common.Address, to *common.Address, gas *hexutil.Big, gasPrice *hexutil.Big, value *hexutil.Big, data hexutil.Bytes, bn int) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, `{ "jsonrpc": "2.0", "method": "trace_call", "params": [{"from":"0x%x"`, from)
	if to != nil {
		fmt.Fprintf(&sb, `,"to":"0x%x"`, *to)
	}
	if gas != nil {
		fmt.Fprintf(&sb, `,"gas":"%s"`, gas)
	}
	if gasPrice != nil {
		fmt.Fprintf(&sb, `,"gasPrice":"%s"`, gasPrice)
	}
	if value != nil {
		fmt.Fprintf(&sb, `,"value":"%s"`, value)
	}
	if len(data) > 0 {
		fmt.Fprintf(&sb, `,"data":"%s"`, data)
	}
	fmt.Fprintf(&sb, `},["trace"],"0x%x"], "id":%d}`, bn, g.reqID)
	return sb.String()
}

func (g *RequestGenerator) call(target string, method, body string, response interface{}) CallResult {
	start := time.Now()
	err := post(g.client, routes[target], body, response)
	return CallResult{
		RequestBody: body,
		Target:      target,
		Took:        time.Since(start),
		RequestID:   g.reqID,
		Method:      method,
		Err:         err,
	}
}
func (g *RequestGenerator) Geth(method, body string, response interface{}) CallResult {
	return g.call(Geth, method, body, response)
}

func (g *RequestGenerator) TurboGeth(method, body string, response interface{}) CallResult {
	return g.call(TurboGeth, method, body, response)
}
