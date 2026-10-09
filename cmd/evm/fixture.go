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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/rpc"
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
		Usage: `file with one "hash" or "name hash" per line; # starts a comment`,
	}
)

var fixtureCommand = cli.Command{
	Action:    fetchTxCmd,
	Name:      "fixture",
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
	c, err := rpc.DialHTTP(cmd.String(FetchRPCFlag.Name), log.Root())
	if err != nil {
		return err
	}
	defer c.Close()
	return fetchTxs(ctx, c, names, out)
}

// fetchTxs writes every tx the node can serve and reports the ones it cannot.
func fetchTxs(ctx context.Context, c *rpc.Client, names []txName, out string) error {
	var failed []error
	for _, n := range names {
		fixture, err := fetchTx(ctx, c, n.hash)
		if err != nil {
			failed = append(failed, fmt.Errorf("%s: %w", n.hash, err))
			continue
		}
		path := filepath.Join(out, n.name+".json")
		if err := os.WriteFile(path, fixture, 0o644); err != nil {
			return err
		}
		fmt.Println(path)
	}
	return errors.Join(failed...)
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
func fetchTx(ctx context.Context, c *rpc.Client, hash string) ([]byte, error) {
	tx, err := call(ctx, c, "eth_getTransactionByHash", hash)
	if err != nil {
		return nil, err
	}
	var where struct {
		BlockNumber string `json:"blockNumber"`
	}
	if err := json.Unmarshal(tx, &where); err != nil {
		return nil, err
	}
	receipt, err := call(ctx, c, "eth_getTransactionReceipt", hash)
	if err != nil {
		return nil, err
	}
	block, err := call(ctx, c, "eth_getBlockByNumber", where.BlockNumber, false)
	if err != nil {
		return nil, err
	}
	prestate, err := call(ctx, c, "debug_traceTransaction", hash, map[string]string{"tracer": "prestateTracer"})
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]json.RawMessage{"tx": tx, "receipt": receipt, "block": block, "prestate": prestate})
}

func call(ctx context.Context, c *rpc.Client, method string, params ...any) (json.RawMessage, error) {
	var res json.RawMessage
	if err := c.CallContext(ctx, &res, method, params...); err != nil {
		return nil, fmt.Errorf("%s: %w", method, err)
	}
	if len(res) == 0 || string(res) == "null" {
		return nil, fmt.Errorf("%s: not found", method)
	}
	return res, nil
}
