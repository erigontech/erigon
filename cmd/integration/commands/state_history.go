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
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/node/debug"
)

func init() {
	printCmd.Flags().Uint64Var(&fromStep, "from", 0, "step from which history to be printed")
	printCmd.Flags().Uint64Var(&toStep, "to", 1e18, "step to which history to be printed")
	withDataDir2(printCmd)
	withHistoryDomain(printCmd)

	withDataDir2(rebuildCmd)
	withHistoryDomain(rebuildCmd)

	historyCmd.AddCommand(printCmd)
	historyCmd.AddCommand(rebuildCmd)

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

		vlogSet := state.NewVLogSet(filepath.Join(dirs.Chaindata, "vlog"))

		history, err := state.NewHistory(
			statecfg.Schema.GetDomainCfg(domainKV).Hist,
			vlogSet,
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

var rebuildCmd = &cobra.Command{
	Use:   "rebuild",
	Short: "Regenerate .ef .efi .v .vi domain history snapshots from step 0",
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

		vlogSet := state.NewVLogSet(filepath.Join(dirs.Chaindata, "vlog"))
		history, err := state.NewHistory(
			statecfg.Schema.GetDomainCfg(domainKV).Hist,
			vlogSet,
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

		for i := uint64(0); i < roTx.FirstStepNotInFiles().ToTxNum(config3.DefaultStepSize); {
			fromTxNum := i
			i += config3.DefaultStepSize * config3.DefaultStepsInFrozenFile

			if i > roTx.FirstStepNotInFiles().ToTxNum(config3.DefaultStepSize) {
				i = roTx.FirstStepNotInFiles().ToTxNum(config3.DefaultStepSize)
			}

			fmt.Printf("Compacting files %d-%d step\n", fromTxNum/config3.DefaultStepSize, i/config3.DefaultStepSize)

			err = roTx.CompactRange(context.TODO(), fromTxNum, i)
			if err != nil {
				logger.Error("Failed to rebuild history", "error", err)
				return
			}
		}
	},
}
