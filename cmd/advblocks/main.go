// Command advblocks builds worst-case blocks for the parallel executor as
// blockchain_test_stateful_engine fixtures, through the testing_ namespace of a
// running node at the snapshot head.
package main

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types"
)

type shape struct {
	name     string
	fan      bool
	gasTotal uint64
	gasPerTx uint64
	prefix   uint16 // keccak rounds before the read, so a short writer has flushed first
}

// Chain shapes call one contract that reads and writes slot 0 first, then burns.
// Fan shapes run a long head tx (keeps the frontier low), a short writer of slot 0,
// and readers of slot 0 that burn after the read.
var shapes = []shape{
	{name: "chain_16M", gasTotal: 300_000_000, gasPerTx: 16_666_666},
	{name: "chain_2M", gasTotal: 300_000_000, gasPerTx: 2_000_000},
	{name: "chain_300k", gasTotal: 300_000_000, gasPerTx: 300_000},
	{name: "chain_60k", gasTotal: 300_000_000, gasPerTx: 60_000},
	{name: "fan_16M", fan: true, gasTotal: 300_000_000, gasPerTx: 16_666_666, prefix: 1400},
	{name: "fan_2M", fan: true, gasTotal: 300_000_000, gasPerTx: 2_000_000, prefix: 1400},
	{name: "chain_2M_60M", gasTotal: 60_000_000, gasPerTx: 2_000_000},
	{name: "chain_300k_60M", gasTotal: 60_000_000, gasPerTx: 300_000},
}

const (
	gasPrice       = 460
	blockGasLimit  = 0xe8d4a51000
	burnThreshold  = 0x0200
	feeRecipient   = "0x2adc25665018aa1fe0e6bc666dac8fc2697ff9ba"
	seedKeyHex     = "0000000000000000000000000000000000000000000000000000000000000001"
	writerCalldata = 0x01
)

var chainID = uint256.NewInt(1)

type rpcClient struct {
	http string
	auth string
	jwt  []byte
}

func (c *rpcClient) call(url string, authed bool, method string, params ...any) (json.RawMessage, error) {
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if authed {
		req.Header.Set("Authorization", "Bearer "+c.jwtToken())
	}
	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", method, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("%s: %w: %s", method, err, data)
	}
	if out.Error != nil {
		return nil, fmt.Errorf("%s: %d %s", method, out.Error.Code, out.Error.Message)
	}
	return out.Result, nil
}

func (c *rpcClient) jwtToken() string {
	enc := base64.RawURLEncoding
	head := enc.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claims := enc.EncodeToString([]byte(fmt.Sprintf(`{"iat":%d}`, time.Now().Unix())))
	mac := hmac.New(sha256.New, c.jwt)
	mac.Write([]byte(head + "." + claims))
	return head + "." + claims + "." + enc.EncodeToString(mac.Sum(nil))
}

// burnLoop appends a keccak loop that runs until gas left is below burnThreshold.
func burnLoop(code []byte) []byte {
	code = append(code, 0x60, 0x01, 0x60, 0x00, 0x52) // mem[0]=1
	label := len(code)
	code = append(code, 0x5b)                                                               // JUMPDEST
	code = append(code, 0x60, 0x20, 0x60, 0x00, 0x20, 0x60, 0x00, 0x52)                     // mem[0]=keccak(mem[0:32])
	code = append(code, 0x61, byte(burnThreshold>>8), byte(burnThreshold&0xff), 0x5a, 0x11) // GAS > threshold
	return append(code, 0x61, byte(label>>8), byte(label), 0x57)                            // JUMPI label
}

func chainRuntime() []byte {
	code := []byte{0x60, 0x00, 0x54, 0x60, 0x01, 0x01, 0x60, 0x00, 0x55} // slot0 = slot0 + 1
	code = burnLoop(code)
	return append(code, 0x00) // STOP
}

func fanRuntime(prefix uint16) []byte {
	code := []byte{0x36}                  // CALLDATASIZE
	code = append(code, 0x61, 0x00, 0x00) // PUSH2 writer, patched below
	patch := len(code) - 2
	code = append(code, 0x57) // JUMPI

	code = append(code, 0x60, 0x01, 0x60, 0x00, 0x52) // mem[0]=1
	code = append(code, 0x61, byte(prefix>>8), byte(prefix))
	loop := len(code)
	code = append(code, 0x5b)
	code = append(code, 0x60, 0x20, 0x60, 0x00, 0x20, 0x60, 0x00, 0x52)
	code = append(code, 0x60, 0x01, 0x90, 0x03)                      // n = n - 1
	code = append(code, 0x80, 0x61, byte(loop>>8), byte(loop), 0x57) // DUP1, JUMPI loop
	code = append(code, 0x50)                                        // POP

	code = append(code, 0x60, 0x00, 0x54, 0x50) // SLOAD(0), POP
	code = burnLoop(code)
	code = append(code, 0x00) // STOP

	code[patch], code[patch+1] = byte(len(code)>>8), byte(len(code))
	code = append(code, 0x5b, 0x60, 0x07, 0x60, 0x00, 0x55, 0x00) // writer: slot0 = 7
	return code
}

// initCode sets slot 0 to 1 and returns the runtime.
func initCode(runtime []byte) []byte {
	prologue := []byte{0x60, 0x01, 0x60, 0x00, 0x55} // SSTORE(0, 1)
	tail := []byte{0x61, 0x00, 0x00, 0x60, 0x00, 0x60, 0x00, 0x39, 0x61, 0x00, 0x00, 0x60, 0x00, 0xf3}
	offset := len(prologue) + len(tail)
	size := len(runtime)
	tail[1], tail[2] = byte(size>>8), byte(size) // PUSH2 size
	tail[4] = byte(offset)                       // PUSH1 offset
	tail[9], tail[10] = byte(size>>8), byte(size)
	return append(append(prologue, tail...), runtime...)
}

func eoaKey(name string, i int) *ecdsa.PrivateKey {
	h := crypto.Keccak256([]byte(fmt.Sprintf("adv-%s-%d", name, i)))
	key, err := crypto.ToECDSA(h)
	if err != nil {
		log.Fatal(err)
	}
	return key
}

func signedTx(key *ecdsa.PrivateKey, nonce, gas uint64, to *common.Address, value uint64, data []byte) []byte {
	var v uint256.Int
	v.SetUint64(value)
	tx := &types.LegacyTx{CommonTx: types.CommonTx{Nonce: nonce, GasLimit: gas, To: to, Value: v, Data: data}, GasPrice: *uint256.NewInt(gasPrice)}
	signer := types.LatestSignerForChainID(chainID)
	signed, err := types.SignNewTx(key, *signer, tx)
	if err != nil {
		log.Fatal(err)
	}
	var buf bytes.Buffer
	if err := signed.MarshalBinary(&buf); err != nil {
		log.Fatal(err)
	}
	return buf.Bytes()
}

func hexs(raw [][]byte) []string {
	out := make([]string, len(raw))
	for i, b := range raw {
		out[i] = "0x" + hex.EncodeToString(b)
	}
	return out
}

func mustUint(r json.RawMessage) uint64 {
	var s string
	if err := json.Unmarshal(r, &s); err != nil {
		log.Fatal(err)
	}
	var v uint64
	if _, err := fmt.Sscanf(s, "0x%x", &v); err != nil {
		log.Fatalf("%s: %v", s, err)
	}
	return v
}

// buildAndApply builds a block on parent, sends it via newPayload and advances head to it.
func buildAndApply(c *rpcClient, parent string, timestamp, slot uint64, txs [][]byte) (json.RawMessage, string, error) {
	zero := "0x" + hex.EncodeToString(make([]byte, 32))
	attrs := map[string]any{
		"timestamp":             fmt.Sprintf("0x%x", timestamp),
		"prevRandao":            zero,
		"suggestedFeeRecipient": feeRecipient,
		"withdrawals":           []any{},
		"parentBeaconBlockRoot": zero,
		"slotNumber":            fmt.Sprintf("0x%x", slot),
		"targetGasLimit":        fmt.Sprintf("0x%x", uint64(blockGasLimit)),
	}
	var resp struct {
		ExecutionPayload json.RawMessage `json:"executionPayload"`
	}
	var hdr struct {
		BlockHash string   `json:"blockHash"`
		GasUsed   string   `json:"gasUsed"`
		Txs       []string `json:"transactions"`
	}
	// The builder reads committed state, which trails the fork choice update of the parent.
	for attempt := 0; ; attempt++ {
		raw, err := c.call(c.http, false, "testing_buildBlockV1", parent, attrs, hexs(txs), "0x00")
		if err != nil {
			return nil, "", fmt.Errorf("build: %w", err)
		}
		if err := json.Unmarshal(raw, &resp); err != nil {
			return nil, "", err
		}
		hdr.Txs = nil
		if err := json.Unmarshal(resp.ExecutionPayload, &hdr); err != nil {
			return nil, "", err
		}
		if len(hdr.Txs) == len(txs) {
			break
		}
		if attempt == 2 {
			return nil, "", fmt.Errorf("built block has %d of %d txs", len(hdr.Txs), len(txs))
		}
		time.Sleep(2 * time.Second)
	}
	log.Printf("built %s txs=%d gasUsed=%s", hdr.BlockHash, len(hdr.Txs), hdr.GasUsed)
	statusRaw, err := c.call(c.auth, true, "engine_newPayloadV5", resp.ExecutionPayload, []any{}, zero, []any{})
	if err != nil {
		return nil, "", err
	}
	var status struct {
		Status string `json:"status"`
	}
	json.Unmarshal(statusRaw, &status)
	if status.Status != "VALID" {
		return nil, "", fmt.Errorf("newPayload: %s", statusRaw)
	}
	fcu := map[string]string{"headBlockHash": hdr.BlockHash, "safeBlockHash": hdr.BlockHash, "finalizedBlockHash": hdr.BlockHash}
	if _, err := c.call(c.auth, true, "engine_forkchoiceUpdatedV4", fcu, nil); err != nil {
		return nil, "", err
	}
	return resp.ExecutionPayload, hdr.BlockHash, nil
}

func payloadEntry(payload json.RawMessage, phase string) map[string]any {
	zero := "0x" + hex.EncodeToString(make([]byte, 32))
	return map[string]any{
		"params":                   []any{payload, []any{}, zero, []any{}},
		"newPayloadVersion":        "5",
		"forkchoiceUpdatedVersion": "4",
		"phase":                    phase,
	}
}

func generate(c *rpcClient, s shape, startHash, outDir string, snapshotNumber uint64, nonceBase uint64) error {
	seedKey, err := crypto.HexToECDSA(seedKeyHex)
	if err != nil {
		return err
	}
	seed := crypto.PubkeyToAddress(seedKey.PublicKey)
	if nonceBase == ^uint64(0) {
		r, err := c.call(c.http, false, "eth_getTransactionCount", seed.Hex(), "latest")
		if err != nil {
			return err
		}
		nonceBase = mustUint(r)
	}

	var runtime []byte
	if s.fan {
		runtime = fanRuntime(s.prefix)
	} else {
		runtime = chainRuntime()
	}
	contract := types.CreateAddress(seed, nonceBase)

	type call struct {
		gas  uint64
		data []byte
	}
	var calls []call
	if s.fan {
		calls = append(calls, call{gas: s.gasPerTx}, call{gas: 150_000, data: []byte{writerCalldata}})
		for gasLeft := s.gasTotal - s.gasPerTx; gasLeft >= s.gasPerTx; gasLeft -= s.gasPerTx {
			calls = append(calls, call{gas: s.gasPerTx})
		}
	} else {
		for i := uint64(0); i*s.gasPerTx+s.gasPerTx <= s.gasTotal; i++ {
			calls = append(calls, call{gas: s.gasPerTx})
		}
	}

	nonce := nonceBase
	setup := [][]byte{signedTx(seedKey, nonce, 2_000_000, nil, 0, initCode(runtime))}
	nonce++
	for i := range calls {
		to := crypto.PubkeyToAddress(eoaKey(s.name, i).PublicKey)
		setup = append(setup, signedTx(seedKey, nonce, 21_000, &to, calls[i].gas*gasPrice*2, nil))
		nonce++
	}
	test := make([][]byte, 0, len(calls))
	for i, call := range calls {
		test = append(test, signedTx(eoaKey(s.name, i), 0, call.gas, &contract, 0, call.data))
	}

	parentRaw, err := c.call(c.http, false, "eth_getBlockByHash", startHash, false)
	if err != nil {
		return err
	}
	var parent struct {
		Timestamp string `json:"timestamp"`
	}
	if err := json.Unmarshal(parentRaw, &parent); err != nil {
		return err
	}
	var ts uint64
	fmt.Sscanf(parent.Timestamp, "0x%x", &ts)
	log.Printf("%s: seedNonce=%d contract=%x txs=%d setupTxs=%d", s.name, nonceBase, contract, len(test), len(setup))

	setupPayload, setupHash, err := buildAndApply(c, startHash, ts+12, 0, setup)
	if err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	testPayload, testHash, err := buildAndApply(c, setupHash, ts+24, 1, test)
	if err != nil {
		first := crypto.PubkeyToAddress(eoaKey(s.name, 0).PublicKey)
		for _, q := range [][]any{{"eth_blockNumber"}, {"eth_getBalance", first.Hex(), "latest"}, {"eth_getTransactionCount", seed.Hex(), "latest"}, {"eth_getCode", contract.Hex(), "latest"}, {"eth_getBlockReceipts", setupHash}} {
			r, qerr := c.call(c.http, false, q[0].(string), q[1:]...)
			log.Printf("debug %s: %.3000s err=%v", q[0], r, qerr)
		}
		return fmt.Errorf("test: %w", err)
	}
	var built struct {
		GasUsed string `json:"gasUsed"`
	}
	if err := json.Unmarshal(testPayload, &built); err != nil {
		return err
	}
	if used := mustUint(json.RawMessage(`"` + built.GasUsed + `"`)); used*10 < s.gasTotal*9 {
		return fmt.Errorf("test block used %d of %d gas: contract did not burn", used, s.gasTotal)
	}

	fixture := map[string]any{
		"network": "Amsterdam", "lastblockhash": testHash,
		"config":                 map[string]string{"network": "Amsterdam", "chainid": "0x01"},
		"snapshotBlockNumber":    fmt.Sprintf("0x%x", snapshotNumber),
		"snapshotBlockHash":      startHash,
		"startBlockNumber":       fmt.Sprintf("0x%x", snapshotNumber),
		"startBlockHash":         startHash,
		"setupEngineNewPayloads": []any{payloadEntry(setupPayload, "setup")},
		"engineNewPayloads":      []any{payloadEntry(testPayload, "testing")},
		"_info":                  map[string]any{"fixture-format": "blockchain_test_stateful_engine", "description": "adversarial parallel-execution fixture " + s.name},
	}
	mgas := s.gasTotal / 1_000_000
	key := fmt.Sprintf("tests/benchmark/compute/adversarial/test_adversarial.py::test_%s[fork_Amsterdam-blockchain_test_stateful_engine-benchmark-gas-value_%dM]", s.name, mgas)
	data, err := json.Marshal(map[string]any{key: fixture})
	if err != nil {
		return err
	}
	path := filepath.Join(outDir, "blockchain_tests_stateful_engine", fmt.Sprintf("for_amsterdam_at_%04dM", mgas), "compute", "adversarial", s.name+".json")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	log.Printf("wrote %s", path)
	return os.WriteFile(path, data, 0o644)
}

func bootstrapFCU(c *rpcClient, head string) error {
	fcu := map[string]string{"headBlockHash": head, "safeBlockHash": head, "finalizedBlockHash": head}
	var last string
	for range 240 {
		raw, err := c.call(c.auth, true, "engine_forkchoiceUpdatedV4", fcu, nil)
		if err != nil {
			last = err.Error()
		} else {
			var out struct {
				PayloadStatus struct {
					Status string `json:"status"`
				} `json:"payloadStatus"`
			}
			if err := json.Unmarshal(raw, &out); err != nil {
				return err
			}
			if out.PayloadStatus.Status == "VALID" {
				return nil
			}
			last = string(raw)
		}
		time.Sleep(5 * time.Second)
	}
	return fmt.Errorf("bootstrap FCU never VALID: %s", last)
}

func main() {
	httpURL := flag.String("rpc", "", "http RPC endpoint")
	authURL := flag.String("auth", "", "engine RPC endpoint")
	jwtPath := flag.String("jwt", "", "jwt secret file")
	out := flag.String("out", "", "fixture output dir")
	start := flag.String("start", "", "snapshot head hash to build on")
	shapeName := flag.String("shape", "", "single shape to generate")
	listOnly := flag.Bool("list", false, "print shape names")
	flag.Parse()

	if *listOnly {
		for _, s := range shapes {
			fmt.Println(s.name)
		}
		return
	}

	jwtData, err := os.ReadFile(*jwtPath)
	if err != nil {
		log.Fatal(err)
	}
	secret, err := hex.DecodeString(string(bytes.TrimPrefix(bytes.TrimSpace(jwtData), []byte("0x"))))
	if err != nil {
		log.Fatal(err)
	}
	c := &rpcClient{http: *httpURL, auth: *authURL, jwt: secret}

	if err := bootstrapFCU(c, *start); err != nil {
		log.Fatal(err)
	}
	blockRaw, err := c.call(c.http, false, "eth_getBlockByHash", *start, false)
	if err != nil {
		log.Fatal(err)
	}
	var head struct {
		Hash   string `json:"hash"`
		Number string `json:"number"`
	}
	if err := json.Unmarshal(blockRaw, &head); err != nil {
		log.Fatal(err)
	}
	var number uint64
	fmt.Sscanf(head.Number, "0x%x", &number)
	log.Printf("head=%s number=%d", head.Hash, number)

	for _, s := range shapes {
		if s.name != *shapeName {
			continue
		}
		if err := generate(c, s, head.Hash, *out, number, ^uint64(0)); err != nil {
			log.Fatalf("%s: %v", s.name, err)
		}
		return
	}
	log.Fatalf("unknown shape %q", *shapeName)
}
