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

// Command eez decodes EEZ batches, compares EEZ chains, captures the L1 data
// a follower reads and mirrors the blobs of a network's batches.
package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/eez/tools"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
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
			{
				Name:  "mirror",
				Usage: "Index the batches of an EEZ network and store their blobs",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "chain", Usage: "EEZ network", Required: true},
					&cli.StringFlag{Name: "l1.rpc", Usage: "JSON-RPC endpoint of the L1 node", Required: true},
					&cli.StringFlag{Name: "beacon", Usage: "REST endpoint of the L1 beacon node; without it, stored blobs are only checked"},
					&cli.StringFlag{Name: "dir", Usage: "Mirror directory: <chain id>.jsonl indexes, blobs/<versioned hash>.blob", Required: true},
					&cli.Uint64Flag{Name: "to", Usage: "Last L1 block indexed (default: the finalized block)"},
				},
				Action: mirror,
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

func mirror(ctx context.Context, cmd *cli.Command) error {
	eez.Register()
	spec, err := chainspec.ChainSpecByName(cmd.String("chain"))
	if err != nil {
		return err
	}
	l2, ok := spec.Config.L2.(*config.L2Config)
	if !ok {
		return fmt.Errorf("chain %s is not an EEZ network", cmd.String("chain"))
	}
	logger := log.New()
	reader, err := l1.DialRPC(ctx, cmd.String("l1.rpc"), logger)
	if err != nil {
		return err
	}
	defer reader.Close()
	to := cmd.Uint64("to")
	if !cmd.IsSet("to") {
		f, err := reader.Finalized(ctx)
		if err != nil {
			return fmt.Errorf("L1 finalized block: %w", err)
		}
		to = f.Number
	}
	scanner := l1.NewScanner(reader, l2.Registry, l2.RollupID)
	if l2.RollupX != nil {
		scanner.UseRollupX()
	}
	var source l1.BlobSource
	if cmd.IsSet("beacon") {
		source = l1.NewBeacon(cmd.String("beacon"))
	}
	root := cmd.String("dir")
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	index := filepath.Join(root, spec.Config.ChainID.String()+".jsonl")
	stats, err := tools.Mirror(ctx, scanner, l1.NewBlobDir(filepath.Join(root, "blobs"), source), index, l2.RegistryDeployBlock, to, logger)
	if err != nil {
		return err
	}
	fmt.Printf("%s: indexed %d batches through L1 block %d, stored %d blobs\n", index, stats.Indexed, to, stats.Stored)
	if n := len(stats.Unavailable); n > 0 {
		return fmt.Errorf("%d batches with blobs not served, L1 blocks %d to %d", n, stats.Unavailable[0].L1Block, stats.Unavailable[n-1].L1Block)
	}
	return nil
}
