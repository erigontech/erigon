package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/ledgerwatch/erigon/eth/tracers/logger"
	"gopkg.in/yaml.v2"
)

type HTTPResponse struct {
	JsonRpc string `json:"jsonrpc"`
	Id      int    `json:"id"`
	Result  Result `json:"result"`
}

type Result struct {
	Gas        int                `json:"gas"`
	Failed     bool               `json:"failed"`
	StructLogs []logger.OpContext `json:"structLogs"`
}

type RequestData struct {
	Method  string   `json:"method"`
	Params  []string `json:"params"`
	ID      int      `json:"id"`
	Jsonrpc string   `json:"jsonrpc"`
}

type Config struct {
	Url      string `yaml: "url"`
	Username string `yaml: "username"`
	Pass     string `yaml: "pass"`
}

// var localTraceFile = "localOpDump.json"
// var rpcTraceFile = "rpcOpDump.json"

func main() {
	fmt.Println("Check started.")

	yamlFile, err := os.ReadFile("rpcConfig.yaml")
	if err != nil {
		panic(err)
	}

	var config Config

	err = yaml.Unmarshal(yamlFile, &config)
	if err != nil {
		panic(err)
	}

	files, err := os.ReadDir("traces/")
	if err != nil {
		log.Fatal(err)
	}

	for _, file := range files {
		txHash := file.Name()
		// remove .json ending
		txHash = txHash[:len(txHash)-5]
		fmt.Println("Checking tx:", txHash)

		jsonFile, err := os.Open("traces/" + file.Name())
		if err != nil {
			fmt.Println(err)
		}
		defer jsonFile.Close()

		var localTrace []logger.OpContext
		byteValue, _ := io.ReadAll(jsonFile)
		err = json.Unmarshal(byteValue, &localTrace)
		if err != nil {
			fmt.Println("Error parsing JSON data:", err)
			return
		}

		rpcTrace, err := getRpcTrace(config, txHash)
		if err != nil {
			panic(err)
		}

		err = compareTraces(localTrace, rpcTrace)
		if err != nil {
			panic(err)
		}

		e := os.Remove("traces/" + file.Name())
		if e != nil {
			log.Fatal(e)
		}
	}

	fmt.Println("Check finished.")
}

func compareTraces(localTrace, rpcTrace []logger.OpContext) error {

	localTracelen := len(localTrace)
	rpcTracelen := len(rpcTrace)
	if localTracelen != rpcTracelen {
		fmt.Printf("opcode counts mismatch. Local count: %d, RPC count: %d\n", len(localTrace), len(rpcTrace))
	}

	var length int
	if len(localTrace) > len(rpcTrace) {
		length = len(rpcTrace)
	} else {
		length = len(localTrace)
	}
	for i := 0; i < length; i++ {
		loc := localTrace[i]
		roc := rpcTrace[i]
		areEqual := loc.Cmp(roc)

		if !areEqual {
			return fmt.Errorf("opcodes at index {%d} are not equal.\nLocal:\t%v\nRPC:\t%v", i, loc, roc)
		}
	}

	return nil
}

func getRpcTrace(cfg Config, txHash string) ([]logger.OpContext, error) {
	payloadbytecode := RequestData{
		Method:  "debug_traceTransaction",
		Params:  []string{txHash},
		ID:      1,
		Jsonrpc: "2.0",
	}

	jsonPayload, err := json.Marshal(payloadbytecode)
	if err != nil {
		return []logger.OpContext{}, err
	}

	req, err := http.NewRequest("POST", cfg.Url, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return []logger.OpContext{}, err
	}

	req.SetBasicAuth(cfg.Username, cfg.Pass)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return []logger.OpContext{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return []logger.OpContext{}, err
	}

	if resp.StatusCode != 200 {
		return []logger.OpContext{}, fmt.Errorf(string(body))
	}

	var httpResp HTTPResponse
	err = json.Unmarshal(body, &httpResp)
	if err != nil {
		return []logger.OpContext{}, err
	}
	result := httpResp.Result

	return result.StructLogs, nil
}
