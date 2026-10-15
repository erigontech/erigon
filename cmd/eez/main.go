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

// Command eez decodes EEZ batches.
package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/eez"
	"github.com/erigontech/erigon/eez/tools"
)

func main() {
	app := &cli.Command{
		Name:  "eez",
		Usage: "EEZ batch tools",
		Commands: []*cli.Command{
			{
				Name:      "decode",
				Usage:     "Decode a postAndVerifyBatch input and its DA payload",
				ArgsUsage: "<hex input | @file | ->",
				Action:    decode,
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
