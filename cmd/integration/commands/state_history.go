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

package commands

import (
	"os"

	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/node/debug"
	"github.com/spf13/cobra"
)

func init() {
	printCmd.Flags().Uint64Var(&fromStep, "from", 0, "step from which history to be printed")
	printCmd.Flags().Uint64Var(&toStep, "to", 1e18, "step to which history to be printed")
	withDataDir2(printCmd)
	withHistoryDomain(printCmd)

	historyCmd.AddCommand(printCmd)
	rootCmd.AddCommand(historyCmd)
}

func withHistoryDomain(cmd *cobra.Command) {
	cmd.Flags().StringVar(&historyDomain, "domain", "", "Name of the domain (accounts, code, etc)")
	must(cmd.MarkFlagRequired("domain"))
}

var (
	fromStep      uint64
	toStep        uint64
	historyDomain string
)

var historyCmd = &cobra.Command{
	Use: "history",
}

var printCmd = &cobra.Command{
	Use: "print",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")

		dirs, l, err := datadir.New(datadirCli).MustFlock()
		if err != nil {
			logger.Error("Opening Datadir", "error", err)
			return
		}
		defer l.Unlock()

		domainKV, err := kv.String2Domain(historyDomain)
		if err != nil {
			logger.Error("Failed to resolve domain", "error", err)
			return
		}

		history, err := state.NewHistory(
			statecfg.Schema.GetDomainCfg(domainKV).Hist,
			config3.DefaultStepSize,
			config3.DefaultStepsInFrozenFile,
			dirs,
			logger,
		)
		if err != nil {
			logger.Error("Failed to init history", "error", err)
			return
		}
		history.Scan(toStep * config3.DefaultStepSize)

		roTx := history.BeginFilesRo()
		defer roTx.Close()

		err = roTx.HistoryDump(
			int(fromStep)*config3.DefaultStepSize,
			int(toStep)*config3.DefaultStepSize,
			os.Stdout,
		)
		if err != nil {
			logger.Error("Failed to print history", "error", err)
			return
		}
	},
}
