// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package rpctest

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/valyala/fastjson"
)

type CallResult struct {
	Target      string
	Took        time.Duration
	RequestID   int
	Method      string
	RequestBody string
	Response    []byte
	Result      *fastjson.Value
	Err         error
}
type RequestGenerator struct {
	reqID atomic.Int64
}

func (g *RequestGenerator) blockNumber() string {
	const template = `{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":%d}`
	return fmt.Sprintf(template, g.reqID.Add(1))
}
func (g *RequestGenerator) getBlockByNumber(blockNum uint64, withTxs bool) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x%x",%t],"id":%d}`
	return fmt.Sprintf(template, blockNum, withTxs, g.reqID.Add(1))
}

func (g *RequestGenerator) getBlockByHash(hash common.Hash, withTxs bool) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getBlockByHash","params":["0x%x",%t],"id":%d}`
	return fmt.Sprintf(template, hash, withTxs, g.reqID.Add(1))
}

func (g *RequestGenerator) getTransactionByHash(hash string) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getTransactionByHash","params":["%s"],"id":%d}`
	return fmt.Sprintf(template, hash, g.reqID.Add(1))
}

func (g *RequestGenerator) storageRangeAt(hash common.Hash, i int, to *common.Address, nextKey common.Hash) string {
	const template = `{"jsonrpc":"2.0","method":"debug_storageRangeAt","params":["0x%x", %d,"0x%x","0x%x",%d],"id":%d}`
	return fmt.Sprintf(template, hash, i, to, nextKey, 1024, g.reqID.Add(1))
}

func (g *RequestGenerator) traceBlockByHash(hash string) string {
	const template = `{"jsonrpc":"2.0","method":"debug_traceBlockByHash","params":["%s"],"id":%d}`
	return fmt.Sprintf(template, hash, g.reqID.Add(1))
}

func (g *RequestGenerator) debugTraceBlockByNumber(blockNum uint64) string {
	const template = `{"jsonrpc":"2.0","method":"debug_traceBlockByNumber","params":[%d],"id":%d}`
	return fmt.Sprintf(template, blockNum, g.reqID.Add(1))
}

func (g *RequestGenerator) debugTraceTransaction(hash string, additionalParams string) string {
	if additionalParams != "" {
		additionalParams = ", {" + additionalParams + "}"
	}
	const template = `{"jsonrpc":"2.0","method":"debug_traceTransaction","params":["%s"%s],"id":%d}`
	return fmt.Sprintf(template, hash, additionalParams, g.reqID.Add(1))
}

func (g *RequestGenerator) getTransactionReceipt(hash string) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getTransactionReceipt","params":["%s"],"id":%d}`
	return fmt.Sprintf(template, hash, g.reqID.Add(1))
}

func (g *RequestGenerator) getBlockReceipts(bn uint64) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getBlockReceipts","params":["0x%x"],"id":%d}`
	return fmt.Sprintf(template, bn, g.reqID.Add(1))
}

func (g *RequestGenerator) getBalance(miner common.Address, bn uint64) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getBalance","params":["0x%x", "0x%x"],"id":%d}`
	return fmt.Sprintf(template, miner, bn, g.reqID.Add(1))
}

func (g *RequestGenerator) getModifiedAccountsByNumber(prevBn uint64, bn uint64) string {
	const template = `{"jsonrpc":"2.0","method":"debug_getModifiedAccountsByNumber","params":[%d, %d],"id":%d}`
	return fmt.Sprintf(template, prevBn, bn, g.reqID.Add(1))
}

func (g *RequestGenerator) getLogs(prevBn uint64, bn uint64, account common.Address) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x%x", "toBlock": "0x%x", "address": "0x%x"}],"id":%d}`
	return fmt.Sprintf(template, prevBn, bn, account, g.reqID.Add(1))
}
func (g *RequestGenerator) getLogsNoFilters(prevBn uint64, bn uint64) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x%x", "toBlock": "0x%x"}],"id":%d}`
	return fmt.Sprintf(template, prevBn, bn, g.reqID.Add(1))
}
func (g *RequestGenerator) getLogsForAddresses(prevBn uint64, bn uint64, accounts []common.Address) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, `{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x%x", "toBlock": "0x%x", "address": [`, prevBn, bn)
	for i, account := range accounts {
		if i > 0 {
			fmt.Fprintf(&sb, `,`)
		}
		fmt.Fprintf(&sb, `"0x%x"`, account)
	}
	fmt.Fprintf(&sb, `]}],"id":%d}`, g.reqID.Add(1))
	return sb.String()
}

func (g *RequestGenerator) getOverlayLogs(prevBn uint64, bn uint64, account common.Address) string {
	const template = `{"jsonrpc":"2.0","method":"overlay_getLogs","params":[{"fromBlock": "0x%x", "toBlock": "0x%x", "address": "0x%x"},{}],"id":%d}`
	return fmt.Sprintf(template, prevBn, bn, account, g.reqID.Add(1))
}

func (g *RequestGenerator) getLogs1(prevBn uint64, bn uint64, account common.Address, topic common.Hash) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x%x", "toBlock": "0x%x", "address": "0x%x", "topics": ["0x%x"]}],"id":%d}`
	return fmt.Sprintf(template, prevBn, bn, account, topic, g.reqID.Add(1))
}

func (g *RequestGenerator) getOverlayLogs1(prevBn uint64, bn uint64, account common.Address, topic common.Hash) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x%x", "toBlock": "0x%x", "address": "0x%x", "topics": ["0x%x"]},{}],"id":%d}`
	return fmt.Sprintf(template, prevBn, bn, account, topic, g.reqID.Add(1))
}

func (g *RequestGenerator) getLogs2(prevBn uint64, bn uint64, account common.Address, topic1, topic2 common.Hash) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x%x", "toBlock": "0x%x", "address": "0x%x", "topics": ["0x%x", "0x%x"]}],"id":%d}`
	return fmt.Sprintf(template, prevBn, bn, account, topic1, topic2, g.reqID.Add(1))
}

func (g *RequestGenerator) getOverlayLogs2(prevBn uint64, bn uint64, account common.Address, topic1, topic2 common.Hash) string {
	const template = `{"jsonrpc":"2.0","method":"eth_getLogs","params":[{"fromBlock": "0x%x", "toBlock": "0x%x", "address": "0x%x", "topics": ["0x%x", "0x%x"]},{}],"id":%d}`
	return fmt.Sprintf(template, prevBn, bn, account, topic1, topic2, g.reqID.Add(1))
}

func (g *RequestGenerator) accountRange(bn uint64, page []byte, num int) string { //nolint
	const template = `{ "jsonrpc": "2.0", "method": "debug_accountRange", "params": ["0x%x", "%s", %d, false, false], "id":%d}`
	encodedKey := base64.StdEncoding.EncodeToString(page)
	return fmt.Sprintf(template, bn, encodedKey, num, g.reqID.Add(1))
}

func (g *RequestGenerator) getProof(bn uint64, account common.Address, storageList []common.Hash) string {
	var template string
	if bn == 0 {
		template = `{ "jsonrpc": "2.0", "method": "eth_getProof", "params": ["0x%x", [%s], "%s"], "id":%d}`
	} else {
		template = `{ "jsonrpc": "2.0", "method": "eth_getProof", "params": ["0x%x", [%s], "0x%x"], "id":%d}`
	}
	var storageStr = make([]string, len(storageList))
	for i, location := range storageList {
		storageStr[i] = fmt.Sprintf(`"0x%x"`, location)
	}
	if bn == 0 {
		return fmt.Sprintf(template, account, strings.Join(storageStr, ","), "latest", g.reqID.Add(1))
	} else {
		return fmt.Sprintf(template, account, strings.Join(storageStr, ","), bn, g.reqID.Add(1))
	}
}

func (g *RequestGenerator) traceCall(from common.Address, to *common.Address, gas *hexutil.Big, gasPrice *hexutil.Big, value *hexutil.Big, data hexutil.Bytes, bn uint64) string {
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
	fmt.Fprintf(&sb, `},["trace", "stateDiff"],"0x%x"], "id":%d}`, bn, g.reqID.Add(1))
	//fmt.Fprintf(&sb, `},["trace"],"0x%x"], "id":%d}`, bn, g.reqID.Add(1))
	return sb.String()
}

func (g *RequestGenerator) traceCallMany(from []common.Address, to []*common.Address, gas []*hexutil.Big, gasPrice []*hexutil.Big, value []*hexutil.Big, data []hexutil.Bytes, bn uint64) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, `{ "jsonrpc": "2.0", "method": "trace_callMany", "params": [[`)
	for i, f := range from {
		if i > 0 {
			fmt.Fprintf(&sb, `,`)
		}
		fmt.Fprintf(&sb, `[{"from":"0x%x"`, f)
		if to[i] != nil {
			fmt.Fprintf(&sb, `,"to":"0x%x"`, *to[i])
		}
		if gas[i] != nil {
			fmt.Fprintf(&sb, `,"gas":"%s"`, gas[i])
		}
		if gasPrice[i] != nil {
			fmt.Fprintf(&sb, `,"gasPrice":"%s"`, gasPrice[i])
		}
		if value[i] != nil {
			fmt.Fprintf(&sb, `,"value":"%s"`, value[i])
		}
		if len(data[i]) > 0 {
			fmt.Fprintf(&sb, `,"data":"%s"`, data[i])
		}
		fmt.Fprintf(&sb, `},["trace", "stateDiff", "vmTrace"]]`)
	}
	fmt.Fprintf(&sb, `],"0x%x"], "id":%d}`, bn, g.reqID.Add(1))
	return sb.String()
}

func (g *RequestGenerator) debugTraceCall(from common.Address, to *common.Address, gas *hexutil.Big, gasPrice *hexutil.Big, value *hexutil.Big, data hexutil.Bytes, bn uint64) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, `{ "jsonrpc": "2.0", "method": "debug_traceCall", "params": [{"from":"0x%x"`, from)
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
	fmt.Fprintf(&sb, `},"0x%x"], "id":%d}`, bn, g.reqID.Add(1))
	return sb.String()
}

func (g *RequestGenerator) traceBlock(bn uint64) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, `{ "jsonrpc": "2.0", "method": "trace_block", "params": ["0x%x"]`, bn)
	fmt.Fprintf(&sb, `, "id":%d}`, g.reqID.Add(1))
	return sb.String()
}

func (g *RequestGenerator) traceFilterFrom(prevBn uint64, bn uint64, account common.Address) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, `{ "jsonrpc": "2.0", "method": "trace_filter", "params": [{"fromBlock":"0x%x", "toBlock": "0x%x", "fromAddress": ["0x%x"]}]`, prevBn, bn, account)
	fmt.Fprintf(&sb, `, "id":%d}`, g.reqID.Add(1))
	return sb.String()
}

func (g *RequestGenerator) traceFilterTo(prevBn uint64, bn uint64, account common.Address) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, `{ "jsonrpc": "2.0", "method": "trace_filter", "params": [{"fromBlock":"0x%x", "toBlock": "0x%x", "toAddress": ["0x%x"]}]`, prevBn, bn, account)
	fmt.Fprintf(&sb, `, "id":%d}`, g.reqID.Add(1))
	return sb.String()
}

func (g *RequestGenerator) traceReplayTransaction(hash string) string {
	const template = `{"jsonrpc":"2.0","method":"trace_replayTransaction","params":["%s", ["trace", "stateDiff"]],"id":%d}`
	return fmt.Sprintf(template, hash, g.reqID.Add(1))
}

func (g *RequestGenerator) traceTransaction(hash string) string {
	const template = `{"jsonrpc":"2.0","method":"trace_transaction","params":["%s"],"id":%d}`
	return fmt.Sprintf(template, hash, g.reqID.Add(1))
}

func (g *RequestGenerator) ethCall(from common.Address, to *common.Address, gas *hexutil.Big, gasPrice *hexutil.Big, value *hexutil.Big, data hexutil.Bytes, bn uint64) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, `{ "jsonrpc": "2.0", "method": "eth_call", "params": [{"from":"0x%x"`, from)
	if to != nil {
		fmt.Fprintf(&sb, `,"to":"0x%x"`, *to)
	}
	if gas != nil {
		fmt.Fprintf(&sb, `,"gas":"%s"`, gas)
	}
	if gasPrice != nil {
		fmt.Fprintf(&sb, `,"gasPrice":"%s"`, gasPrice)
	}
	if len(data) > 0 {
		fmt.Fprintf(&sb, `,"data":"%s"`, data)
	}
	if value != nil {
		fmt.Fprintf(&sb, `,"value":"%s"`, value)
	}
	fmt.Fprintf(&sb, `},"0x%x"], "id":%d}`, bn, g.reqID.Add(1))
	return sb.String()
}

func (g *RequestGenerator) ethCreateAccessList(from common.Address, to *common.Address, gas *hexutil.Big, gasPrice *hexutil.Big, value *hexutil.Big, data hexutil.Bytes, bn uint64) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, `{ "jsonrpc": "2.0", "method": "eth_createAccessList", "params": [{"from":"0x%x"`, from)
	if to != nil {
		fmt.Fprintf(&sb, `,"to":"0x%x"`, *to)
	}
	if gas != nil {
		fmt.Fprintf(&sb, `,"gas":"%s"`, gas)
	}
	if gasPrice != nil {
		fmt.Fprintf(&sb, `,"gasPrice":"%s"`, gasPrice)
	}
	if len(data) > 0 {
		fmt.Fprintf(&sb, `,"data":"%s"`, data)
	}
	if value != nil {
		fmt.Fprintf(&sb, `,"value":"%s"`, value)
	}
	fmt.Fprintf(&sb, `},"0x%x"], "id":%d}`, bn, g.reqID.Add(1))
	return sb.String()
}

func (g *RequestGenerator) ethCallLatest(from common.Address, to *common.Address, gas *hexutil.Big, gasPrice *hexutil.Big, value *hexutil.Big, data hexutil.Bytes) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, `{ "jsonrpc": "2.0", "method": "eth_call", "params": [{"from":"0x%x"`, from)
	if to != nil {
		fmt.Fprintf(&sb, `,"to":"0x%x"`, *to)
	}
	if gas != nil {
		fmt.Fprintf(&sb, `,"gas":"%s"`, gas)
	}
	if gasPrice != nil {
		fmt.Fprintf(&sb, `,"gasPrice":"%s"`, gasPrice)
	}
	if len(data) > 0 {
		fmt.Fprintf(&sb, `,"data":"%s"`, data)
	}
	if value != nil {
		fmt.Fprintf(&sb, `,"value":"%s"`, value)
	}
	fmt.Fprintf(&sb, `},"latest"], "id":%d}`, g.reqID.Add(1))
	return sb.String()
}
func (g *RequestGenerator) otsGetBlockTransactions(block_number uint64, page_number uint64, page_size uint64) string {
	const template = `{"id":1,"jsonrpc":"2.0","method":"ots_getBlockTransactions","params":[%d, %d, %d]}`
	return fmt.Sprintf(template, block_number, page_number, page_size)
}

var client = &http.Client{
	Transport: &http.Transport{
		MaxIdleConns:        600,
		MaxIdleConnsPerHost: 600,
		MaxConnsPerHost:     600,
		IdleConnTimeout:     90 * time.Second,
		ReadBufferSize:      64 * 1024,
		WriteBufferSize:     16 * 1024,
	},
	Timeout: 600 * time.Second, // Per-request timeout
}

func (g *RequestGenerator) call(target string, method, body string, response interface{}) CallResult {
	start := time.Now()
	err := post(client, routes[target], body, response)
	return CallResult{
		RequestBody: body,
		Target:      target,
		Took:        time.Since(start),
		RequestID:   response.(HasRequestID).GetRequestId(),
		Method:      method,
		Err:         err,
	}
}

type HasRequestID interface {
	GetRequestId() int
}

func (g *RequestGenerator) call2(target string, method, body string) CallResult {
	start := time.Now()
	response, val, err := post2(client, routes[target], body)
	return CallResult{
		RequestBody: body,
		Target:      target,
		Took:        time.Since(start),
		RequestID:   int(g.reqID.Load()),
		Method:      method,
		Response:    response,
		Result:      val,
		Err:         err,
	}
}

func (g *RequestGenerator) Geth(method, body string, response interface{}) CallResult {
	return g.call(Geth, method, body, response)
}

func (g *RequestGenerator) Erigon(method, body string, response interface{}) CallResult {
	return g.call(Erigon, method, body, response)
}

func (g *RequestGenerator) Geth2(method, body string) CallResult {
	return g.call2(Geth, method, body)
}

func (g *RequestGenerator) Erigon2(method, body string) CallResult {
	return g.call2(Erigon, method, body)
}
