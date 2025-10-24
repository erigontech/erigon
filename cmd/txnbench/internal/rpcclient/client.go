package rpcclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	url    string
	client *http.Client
}

func New(url string, timeout time.Duration) *Client {
	return &Client{
		url: url,
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

type rpcReq struct {
	JsonRPC string      `json:"jsonrpc"`
	ID      int         `json:"id"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
}

type rpcResp[T any] struct {
	JsonRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Result  T      `json:"result"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func (c *Client) Call(ctx context.Context, method string, params interface{}, out any) error {
	body, _ := json.Marshal(rpcReq{JsonRPC: "2.0", ID: 1, Method: method, Params: params})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// decode into generic map, then map to out
	var raw map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return err
	}

	if e, ok := raw["error"]; ok && len(e) > 0 && string(e) != "null" {
		type eresp struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		}
		var er eresp
		_ = json.Unmarshal(e, &er)
		return fmt.Errorf("rpc error %d: %s", er.Code, er.Message)
	}
	if r, ok := raw["result"]; ok {
		return json.Unmarshal(r, out)
	}
	return fmt.Errorf("rpc: no result field")
}

// Helpers

func ParseHexUint64(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		return strconv.ParseUint(s[2:], 16, 64)
	}
	return strconv.ParseUint(s, 10, 64)
}

func ToHex(n uint64) string {
	return fmt.Sprintf("0x%x", n)
}

// Methods we need

func (c *Client) EthBlockNumber(ctx context.Context) (uint64, error) {
	var hex string
	if err := c.Call(ctx, "eth_blockNumber", []any{}, &hex); err != nil {
		return 0, err
	}
	return ParseHexUint64(hex)
}

func (c *Client) EthChainID(ctx context.Context) (uint64, error) {
	var hex string
	if err := c.Call(ctx, "eth_chainId", []any{}, &hex); err != nil {
		return 0, err
	}
	return ParseHexUint64(hex)
}

type Block struct {
	Number       string   `json:"number"`
	Hash         string   `json:"hash"`
	Transactions []string `json:"transactions"` // because we pass false => hashes only
}

func (c *Client) EthGetBlockByNumber(ctx context.Context, numHex string, full bool) (Block, error) {
	var b Block
	if err := c.Call(ctx, "eth_getBlockByNumber", []any{numHex, full}, &b); err != nil {
		return Block{}, err
	}
	return b, nil
}

// EthGetTransactionByHash Raw call for eth_getTransactionByHash; we don't need to decode result
func (c *Client) EthGetTransactionByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	var v json.RawMessage
	if err := c.Call(ctx, "eth_getTransactionByHash", []any{hash}, &v); err != nil {
		return nil, err
	}
	return v, nil
}
