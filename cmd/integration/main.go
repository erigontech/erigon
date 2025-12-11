// Copyright 2024 The Erigon Authors
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
	"fmt"
	"os"

	_ "github.com/erigontech/erigon/db/snaptype2"     //hack
	_ "github.com/erigontech/erigon/polygon/heimdall" //hack

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cmd/integration/commands"
)

func main() {
	rootCmd := commands.RootCommand()
	ctx, _ := common.RootContext()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
