// Copyright 2017 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon/core/asm"
)

var disasmCommand = cli.Command{
	Action:    disasmCmd,
	Name:      "disasm",
	Usage:     "disassembles evm binary",
	ArgsUsage: "<file>",
}

func disasmCmd(ctx *cli.Context) error {
	var in string
	switch {
	case len(ctx.Args().First()) > 0:
		fn := ctx.Args().First()
		input, err := os.ReadFile(fn)
		if err != nil {
			return err
		}
		in = string(input)
	case ctx.IsSet(InputFlag.Name):
		in = ctx.String(InputFlag.Name)
	default:
		return errors.New("missing filename or --input value")
	}

	code := strings.TrimSpace(in)
	fmt.Printf("%v\n", code)
	return asm.PrintDisassembled(code)
}
