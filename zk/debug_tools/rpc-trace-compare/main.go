package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"

	"github.com/ledgerwatch/erigon/ethclient"
	"github.com/ledgerwatch/erigon/zk/debug_tools"
	"github.com/ledgerwatch/log/v3"
)

func main() {
	ctx := context.Background()
	rpcConfig, err := debug_tools.GetConf()
	if err != nil {
		log.Error("RPGCOnfig", "err", err)
		return
	}

	log.Warn("Starting trace comparison", "blockNumber", rpcConfig.Block)
	defer log.Warn("Check finished.")

	rpcClientRemote, err := ethclient.Dial(rpcConfig.Url)
	if err != nil {
		log.Error("rpcClientRemote.Dial", "err", err)
	}
	rpcClientLocal, err := ethclient.Dial(rpcConfig.LocalUrl)
	if err != nil {
		log.Error("rpcClientLocal.Dial", "err", err)
	}

	blockNum := big.NewInt(rpcConfig.Block)
	// get local block
	blockLocal, err := rpcClientLocal.BlockByNumber(ctx, blockNum)
	if err != nil {
		log.Error("rpcClientRemote.BlockByNumber", "err", err)
	}

	// get remote block
	blockRemote, err := rpcClientRemote.BlockByNumber(ctx, blockNum)
	if err != nil {
		log.Error("rpcClientLocal.BlockByNumber", "err", err)
	}

	// compare block tx hashes
	txHashesLocal := make([]string, len(blockLocal.Transactions()))
	for i, tx := range blockLocal.Transactions() {
		txHashesLocal[i] = tx.Hash().String()
	}

	txHashesRemote := make([]string, len(blockRemote.Transactions()))
	for i, tx := range blockRemote.Transactions() {
		txHashesRemote[i] = tx.Hash().String()
	}

	// just print errorand continue
	if len(txHashesLocal) != len(txHashesRemote) {
		log.Error("txHashesLocal != txHashesRemote", "txHashesLocal", txHashesLocal, "txHashesRemote", txHashesRemote)
	}

	if len(txHashesLocal) == 0 {
		log.Warn("Block has no txs to compare")
		return
	}

	// use the txs on local node since we might be limiting them for debugging purposes \
	// and those are the ones we want to check
	for _, txHash := range txHashesLocal {
		log.Warn("----------------------------------------------")
		log.Warn("Comparing tx", "txHash", txHash)

		localTrace, err := getRpcTrace(rpcConfig.LocalUrl, txHash)
		if err != nil {
			log.Error("Getting localTrace failed:", "err", err)
			continue
		}

		remoteTrace, err := getRpcTrace(rpcConfig.Url, txHash)
		if err != nil {
			log.Error("Getting remoteTrace failed:", "err", err)
			continue
		}

		if !compareTraces(localTrace, remoteTrace) {
			log.Warn("traces don't match", "txHash", txHash)
		}
		log.Warn("----------------------------------------------")
		// if err = os.Remove("./traces/" + traceFile); err != nil {
		// 	fmt.Println(err)
		// }
	}
}

func getRpcTrace(url string, txHash string) (*HttpResult, error) {
	payloadbytecode := RequestData{
		Method:  "debug_traceTransaction",
		Params:  []string{txHash},
		ID:      1,
		Jsonrpc: "2.0",
	}

	jsonPayload, err := json.Marshal(payloadbytecode)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return nil, err
	}
	// req.SetBasicAuth(cfg.Username, cfg.Pass)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to get rpc: %v", resp.Body)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var httpResp HTTPResponse
	json.Unmarshal(body, &httpResp)

	if httpResp.Error.Code != 0 {
		return nil, fmt.Errorf("failed to get trace: %v", httpResp.Error)
	}
	return &httpResp.Result, nil
}

func compareTraces(localTrace, remoteTrace *HttpResult) bool {
	traceMatches := true
	if localTrace.Failed != remoteTrace.Failed {
		log.Warn("\"failed\" field mismatch", "Local", localTrace.Failed, "Remote", remoteTrace.Failed)
		traceMatches = false
	}

	if localTrace.Gas != remoteTrace.Gas {
		log.Warn("\"gas\" field mismatch", "Local", localTrace.Gas, "Remote", remoteTrace.Gas)
		traceMatches = false
	}

	if localTrace.ReturnValue != remoteTrace.ReturnValue {
		log.Warn("\"returnValue\" field mismatch", "Local", localTrace.ReturnValue, "Remote", remoteTrace.ReturnValue)
		traceMatches = false
	}

	localLogs := localTrace.StructLogs
	remoteLogs := remoteTrace.StructLogs
	localTracelen := len(localLogs)
	remoteTracelen := len(remoteLogs)
	if localTracelen != remoteTracelen {
		log.Warn("opcode counts mismatch.", "Local count", localTracelen, "Remote count", remoteTracelen)
		traceMatches = false
	}

	log.Info("Getting the first opcode mismatch...")
	for i, loc := range localLogs {
		roc := remoteLogs[i]
		if !loc.cmp(roc) {

			if i == 0 {
				log.Warn("First opcode mismatch", "Local", loc, "Remote", roc)
			} else {
				prevLoc := localLogs[i-1]
				prevRoc := remoteLogs[i-1]
				log.Warn("First opcode mismatch", "Previous Local", prevLoc, "Previous Remote", prevRoc)
				traceMatches = false
			}
			break
		}
	}

	return traceMatches
}

type HTTPResponse struct {
	Result HttpResult `json:"result"`
	Error  HttpError  `json:"error"`
}

type HttpError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type HttpResult struct {
	Gas         uint64      `json:"gas"`
	Failed      bool        `json:"failed"`
	ReturnValue interface{} `json:"returnValue"`
	StructLogs  []OpContext `json:"structLogs"`
}

type RequestData struct {
	Method  string   `json:"method"`
	Params  []string `json:"params"`
	ID      int      `json:"id"`
	Jsonrpc string   `json:"jsonrpc"`
}

type OpContext struct {
	Pc      uint64   `json:"pc"`
	Op      string   `json:"op"`
	Gas     uint64   `json:"gas"`
	GasCost uint64   `json:"gasCost"`
	Depth   int      `json:"depth"`
	Stack   []string `json:"stack"`
	Refund  uint64   `json:"refund"`
}

func (oc *OpContext) cmp(b OpContext) bool {
	opMatches := true
	if oc.Pc != b.Pc {
		log.Warn("pc mismatch", "Local", oc.Pc, "Remote", b.Pc)
		opMatches = false
	}
	if oc.Op != b.Op {
		log.Warn("op mismatch", "Local", oc.Op, "Remote", b.Op)
		opMatches = false
	}

	if oc.Gas != b.Gas {
		log.Warn("gas mismatch", "Local", oc.Gas, "Remote", b.Gas)
		opMatches = false
	}

	if oc.GasCost != b.GasCost {
		log.Warn("gasCost mismatch", "Local", oc.GasCost, "Remote", b.GasCost)
		opMatches = false
	}

	if oc.Depth != b.Depth {
		log.Warn("depth mismatch", "Local", oc.Depth, "Remote", b.Depth)
		opMatches = false
	}

	if oc.Refund != b.Refund {
		log.Warn("refund mismatch", "Local", oc.Refund, "Remote", b.Refund)
		opMatches = false
	}

	if len(oc.Stack) != len(b.Stack) {
		log.Warn("stack length mismatch", "Local", len(oc.Stack), "Remote", len(b.Stack))
		opMatches = false
	}

	foundMismatchingStack := false
	for i, localValue := range oc.Stack {
		remoteValue := b.Stack[i]
		if localValue != remoteValue {
			log.Warn("stack value mismatch", "index", i, "total local", len(oc.Stack), "Local", localValue, "Remote", remoteValue)
			opMatches = false
			break
		}
	}

	if !foundMismatchingStack && !opMatches {
		log.Warn("Didn't find a mismatching stack entry. The stack matches up until the local stack's end.")
	}

	return opMatches
}
