// Copyright 2026 The Erigon Authors
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

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v3"
)

var (
	FetchRPCFlag = cli.StringFlag{
		Name:  "rpc",
		Usage: "JSON-RPC endpoint that serves debug_traceTransaction for the tx's block",
		Value: "http://127.0.0.1:8545",
	}
	FetchOutFlag = cli.StringFlag{
		Name:  "out",
		Usage: "directory the fixtures are written to",
		Value: "execution/vm/benchmark/testdata/txs",
	}
	FetchListFlag = cli.StringFlag{
		Name:  "list",
		Usage: "file with one `hash` or `name hash` per line; # starts a comment",
	}
)

var fetchTxCommand = cli.Command{
	Action:    fetchTxCmd,
	Name:      "fetchtx",
	Usage:     "downloads what replaying a transaction needs (tx, receipt, block header, prestate) for the vm benchmarks",
	ArgsUsage: "<txhash>...",
	Flags:     []cli.Flag{&FetchRPCFlag, &FetchOutFlag, &FetchListFlag},
}

func fetchTxCmd(ctx context.Context, cmd *cli.Command) error {
	names, err := fetchTxNames(cmd.Args().Slice(), cmd.String(FetchListFlag.Name))
	if err != nil {
		return err
	}
	if len(names) == 0 {
		return errors.New("no tx hashes: pass them as arguments or with --list")
	}
	out := cmd.String(FetchOutFlag.Name)
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	c := &jsonRPC{url: cmd.String(FetchRPCFlag.Name)}
	for _, n := range names {
		fixture, err := fetchTx(ctx, c, n.hash)
		if err != nil {
			return fmt.Errorf("%s: %w", n.hash, err)
		}
		path := filepath.Join(out, n.name+".json")
		if err := os.WriteFile(path, fixture, 0o644); err != nil {
			return err
		}
		fmt.Println(path)
	}
	return nil
}

type txName struct{ name, hash string }

// fetchTxNames names each fixture after its list entry, or after its hash.
func fetchTxNames(args []string, list string) ([]txName, error) {
	var names []txName
	for _, h := range args {
		names = append(names, txName{h, h})
	}
	if list == "" {
		return names, nil
	}
	f, err := os.Open(list)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line, _, _ := strings.Cut(sc.Text(), "#")
		switch fields := strings.Fields(line); len(fields) {
		case 0:
		case 1:
			names = append(names, txName{fields[0], fields[0]})
		case 2:
			names = append(names, txName{fields[0], fields[1]})
		default:
			return nil, fmt.Errorf("%s: want `hash` or `name hash`, got %q", list, line)
		}
	}
	return names, sc.Err()
}

// fetchTx keeps the RPC results as they came: the benchmark decodes what it needs.
func fetchTx(ctx context.Context, c *jsonRPC, hash string) ([]byte, error) {
	tx, err := c.call(ctx, "eth_getTransactionByHash", hash)
	if err != nil {
		return nil, err
	}
	var where struct {
		BlockNumber string `json:"blockNumber"`
	}
	if err := json.Unmarshal(tx, &where); err != nil {
		return nil, err
	}
	receipt, err := c.call(ctx, "eth_getTransactionReceipt", hash)
	if err != nil {
		return nil, err
	}
	block, err := c.call(ctx, "eth_getBlockByNumber", where.BlockNumber, false)
	if err != nil {
		return nil, err
	}
	prestate, err := c.call(ctx, "debug_traceTransaction", hash, map[string]string{"tracer": "prestateTracer"})
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]json.RawMessage{"tx": tx, "receipt": receipt, "block": block, "prestate": prestate})
}

type jsonRPC struct{ url string }

func (c *jsonRPC) call(ctx context.Context, method string, params ...any) (json.RawMessage, error) {
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var out struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("%s: HTTP %d: %.200s", method, resp.StatusCode, raw)
	}
	if out.Error != nil {
		return nil, fmt.Errorf("%s: %s", method, out.Error.Message)
	}
	if len(out.Result) == 0 || string(out.Result) == "null" {
		return nil, fmt.Errorf("%s: not found", method)
	}
	return out.Result, nil
}
