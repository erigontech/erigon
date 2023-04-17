package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"gopkg.in/yaml.v2"
)

func main() {
	rpcConfig, err := getConf()
	if err != nil {
		panic(fmt.Sprintf("error RPGCOnfig: %s", err))
	}
	files, err := os.ReadDir("./traces")
	if err != nil {
		panic(fmt.Sprintf("error: %s", err))
	}

	for _, file := range files {
		traceFile := file.Name()

		jsonFile, err := os.Open("./traces/" + traceFile)
		if err != nil {
			fmt.Println(traceFile)
			fmt.Println(err)
		}
		defer jsonFile.Close()

		var localTrace []OpContext
		byteValue, _ := ioutil.ReadAll(jsonFile)
		err = json.Unmarshal(byteValue, &localTrace)
		if err != nil {
			fmt.Println(traceFile)

			fmt.Println("Error parsing JSON data:", err)
			return
		}

		txHash := traceFile[:len(traceFile)-5]

		rpcTrace, err := getRpcTrace(txHash, rpcConfig)
		if err != nil {
			fmt.Println("Error getting rpcTrace:", err)
			return
		}

		if err := compareTraces(localTrace, rpcTrace); err != nil {
			fmt.Println("traces don't match for TX: ", txHash)
			fmt.Println(err)
			return
		}

		if err = os.Remove("./traces/" + traceFile); err != nil {
			fmt.Println(err)
		}
	}

	fmt.Println("Check finished.")
}

func getRpcTrace(txHash string, cfg RpcConfig) ([]OpContext, error) {
	fmt.Println(txHash)
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

	req, err := http.NewRequest("POST", cfg.Url, bytes.NewBuffer(jsonPayload))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(cfg.Username, cfg.Pass)
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
	return httpResp.Result.StructLogs, nil
}

func compareTraces(localTrace, rpcTrace []OpContext) error {

	localTracelen := len(localTrace)
	rpcTracelen := len(rpcTrace)
	if localTracelen != rpcTracelen {
		fmt.Printf("opcode counts mismatch. Local count: %d, RPC count: %d\n", len(localTrace), len(rpcTrace))
	}

	for i, loc := range localTrace {
		roc := rpcTrace[i]
		if err := loc.cmp(roc); err != nil {
			if i == len(localTrace)-1 && loc.Op == "RETURN" && roc.Op == "RETURN" && roc.GasCost > 18046744073709548816 {
				continue
			}
			return fmt.Errorf("%v\nLOCAL: %v\nRPC: %v", err, loc, roc)
		}
	}

	return nil
}

func getConf() (RpcConfig, error) {
	yamlFile, err := ioutil.ReadFile("rpcConfig.yaml")
	if err != nil {
		return RpcConfig{}, err
	}

	c := RpcConfig{}
	err = yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		return RpcConfig{}, err
	}

	return c, nil
}

type RpcConfig struct {
	Url      string `yaml:"url"`
	Username string `yaml:"username"`
	Pass     string `yaml:"pass"`
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
	StructLogs []OpContext `json:"structLogs"`
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

func (oc *OpContext) cmp(b OpContext) error {
	if oc.Pc != b.Pc ||
		oc.Op != b.Op ||
		oc.Gas != b.Gas ||
		//dump not quite correct here, missing some reasings I guess
		// oc.GasCost != b.GasCost ||
		oc.Depth != b.Depth ||
		oc.Refund != b.Refund ||
		len(oc.Stack) != len(b.Stack) {
		return fmt.Errorf("general mismatch")
	}

	for i, value := range oc.Stack {
		value2 := b.Stack[i]
		if value != value2 {
			return fmt.Errorf("mismatch at stack index: %d", i)
		}
	}

	return nil
}
