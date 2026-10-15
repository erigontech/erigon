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

// Command eez decodes EEZ batches, compares EEZ chains and captures the L1
// data a follower reads.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/eez/tools"
)

func main() {
	app := &cli.Command{
		Name:  "eez",
		Usage: "EEZ batch, chain and capture tools",
		Commands: []*cli.Command{
			{
				Name:      "decode",
				Usage:     "Decode a postAndVerifyBatch input and its DA payload",
				ArgsUsage: "<hex input | @file | ->",
				Action:    decode,
			},
			{
				Name:  "diff",
				Usage: "Find the first block two EEZ chains disagree on",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "a", Usage: "JSON-RPC endpoint of the first chain", Required: true},
					&cli.StringFlag{Name: "b", Usage: "JSON-RPC endpoint of the second chain", Required: true},
					&cli.Uint64Flag{Name: "from", Usage: "First block compared"},
					&cli.Uint64Flag{Name: "to", Usage: "Last block compared", Required: true},
				},
				Action: diff,
			},
			{
				Name:  "capture",
				Usage: "Record the L1 blocks, registry logs and batch postings a follower reads",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "l1.rpc", Usage: "JSON-RPC endpoint of the L1 node", Required: true},
					&cli.StringFlag{Name: "registry", Usage: "EEZ registry address", Required: true},
					&cli.Uint64Flag{Name: "from", Usage: "First L1 block", Required: true},
					&cli.Uint64Flag{Name: "to", Usage: "Last L1 block", Required: true},
					&cli.StringFlag{Name: "out", Usage: "Directory the recording is written to", Required: true},
				},
				Action: capture,
			},
		},
	}
	if err := app.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func decode(_ context.Context, cmd *cli.Command) error {
	arg := cmd.Args().First()
	var raw string
	switch {
	case arg == "" || arg == "-":
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		raw = string(b)
	case strings.HasPrefix(arg, "@"):
		b, err := os.ReadFile(arg[1:])
		if err != nil {
			return err
		}
		raw = string(b)
	default:
		raw = arg
	}
	raw = strings.TrimSpace(raw)
	input, err := hex.DecodeString(strings.TrimPrefix(strings.TrimPrefix(raw, "0x"), "0X"))
	if err != nil {
		return fmt.Errorf("input: %w", err)
	}
	// Payload transactions may be of type 0x76.
	eez.Register()
	out, err := tools.Decode(input)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(out)
	return err
}

func diff(ctx context.Context, cmd *cli.Command) error {
	logger := log.New()
	a, err := tools.DialSource(ctx, cmd.String("a"), logger)
	if err != nil {
		return err
	}
	defer a.Close()
	b, err := tools.DialSource(ctx, cmd.String("b"), logger)
	if err != nil {
		return err
	}
	defer b.Close()
	d, err := tools.Diff(ctx, a, b, cmd.Uint64("from"), cmd.Uint64("to"))
	if err != nil {
		return err
	}
	if d == nil {
		fmt.Println("no difference")
		return nil
	}
	out, err := json.MarshalIndent(map[string]any{"block": d.Block, "fields": d.Fields, "a": d.A, "b": d.B}, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return cli.Exit("", 2)
}

func capture(ctx context.Context, cmd *cli.Command) error {
	if !common.IsHexAddress(cmd.String("registry")) {
		return fmt.Errorf("--registry %q is not an address", cmd.String("registry"))
	}
	reader, err := l1.DialRPC(ctx, cmd.String("l1.rpc"), log.New())
	if err != nil {
		return err
	}
	defer reader.Close()
	c, err := tools.Capture(ctx, reader, common.HexToAddress(cmd.String("registry")), cmd.Uint64("from"), cmd.Uint64("to"))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cmd.String("out"), 0o755); err != nil {
		return err
	}
	return recording.Write(cmd.String("out"), c)
}
