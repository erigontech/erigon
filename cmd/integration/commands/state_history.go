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
	"bytes"
	"context"
	"fmt"
	"sort"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
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
	withHistoryKey(printCmd)

	distributionCmd.Flags().Uint64Var(&fromStep, "from", 0, "step from which history to be printed")
	distributionCmd.Flags().Uint64Var(&toStep, "to", 1e18, "step to which history to be printed")
	withDataDir2(distributionCmd)
	withHistoryDomain(distributionCmd)

	duplicatesCmd.Flags().Uint64Var(&fromStep, "from", 0, "step from which to scan history")
	duplicatesCmd.Flags().Uint64Var(&toStep, "to", 1e18, "step up to which to scan history")
	duplicatesCmd.Flags().StringVar(&historyDomain, "domain", "", "restrict scan to one domain (accounts, storage, code, commitment, receipt, rcache); default: all present")
	duplicatesCmd.Flags().IntVar(&dupSamples, "samples", 3, "number of example keys with duplicates to print per domain")
	withDataDir2(duplicatesCmd)

	historyCmd.AddCommand(printCmd)
	historyCmd.AddCommand(distributionCmd)
	historyCmd.AddCommand(duplicatesCmd)

	rootCmd.AddCommand(historyCmd)
}

func withHistoryDomain(cmd *cobra.Command) {
	cmd.Flags().StringVar(&historyDomain, "domain", "", "Name of the domain (accounts, code, etc)")
	must(cmd.MarkFlagRequired("domain"))
}

func withHistoryKey(cmd *cobra.Command) {
	cmd.Flags().StringVar(&historyKey, "key", "", "Dump values of a specific key in hex format")
}

var (
	fromStep      uint64
	toStep        uint64
	historyKey    string
	historyDomain string
	dupSamples    int
)

var historyCmd = &cobra.Command{
	Use: "history",
}

func openHistory(ctx context.Context, dirs datadir.Dirs, domainName string, scanToStep uint64, logger log.Logger) (*state.History, *state.ErigonDBSettings, error) {
	settings, err := state.ResolveErigonDBSettings(dirs, logger, false)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve erigondb settings: %w", err)
	}
	domainKV, err := kv.String2Domain(domainName)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve domain: %w", err)
	}
	history, err := state.NewHistory(
		statecfg.Schema.GetDomainCfg(domainKV).Hist,
		settings.StepSize,
		settings.StepsInFrozenFile,
		dirs,
		logger,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("init history: %w", err)
	}
	history.Scan(ctx, scanToStep*settings.StepSize)
	return history, settings, nil
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

		history, settings, err := openHistory(cmd.Context(), dirs, historyDomain, toStep, logger)
		if err != nil {
			logger.Error("Failed to open history", "error", err)
			return
		}
		stepSize := settings.StepSize

		roTx := history.BeginFilesRoForDebug()
		defer roTx.Close()

		var keyToDump *[]byte

		if historyKey != "" {
			key := common.Hex2Bytes(historyKey)
			keyToDump = &key
		}

		err = roTx.HistoryDump(
			int(fromStep)*int(stepSize),
			int(toStep)*int(stepSize),
			keyToDump,
			func(key []byte, txNum uint64, val []byte) {
				fmt.Printf("key: %x, txn: %d, val: %x\n", key, txNum, val)
			},
		)
		if err != nil {
			logger.Error("Failed to print history", "error", err)
			return
		}
	},
}

var distributionCmd = &cobra.Command{
	Use: "distribution",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")

		dirs, l, err := datadir.New(datadirCli).MustFlock()
		if err != nil {
			logger.Error("Opening Datadir", "error", err)
			return
		}
		defer l.Unlock()

		history, settings, err := openHistory(cmd.Context(), dirs, historyDomain, toStep, logger)
		if err != nil {
			logger.Error("Failed to open history", "error", err)
			return
		}
		stepSize := settings.StepSize

		roTx := history.BeginFilesRoForDebug()
		defer roTx.Close()

		keysEntries := make(map[string]int)
		uniqueEntries := 0

		err = roTx.HistoryDump(
			int(fromStep)*int(stepSize),
			int(toStep)*int(stepSize),
			nil,
			func(key []byte, txNum uint64, val []byte) {
				keysEntries[string(key)] += 1
				uniqueEntries++

				//fmt.Printf("key: %x, txn: %d, val: %x\n", key, txNum, val)
			},
		)
		if err != nil {
			logger.Error("Failed to calculate history distribution", "error", err)
			return
		}

		var distribution []int

		for _, count := range keysEntries {
			distribution = append(distribution, count)
		}

		sort.Ints(distribution)

		if len(distribution) == 0 {
			return
		}

		type DistPecentile struct {
			P          int
			Value      int
			ExampleKey []byte
		}

		percentiles := []DistPecentile{
			{P: 50, Value: distribution[len(distribution)/2]},
			{P: 75, Value: distribution[len(distribution)/4*3]},
			{P: 90, Value: distribution[len(distribution)/10*9]},
			{P: 99, Value: distribution[len(distribution)/100*99]},
			{P: 999, Value: distribution[len(distribution)/1000*999]},
		}

		fmt.Printf("Unique entries: %d\n", uniqueEntries)
		fmt.Printf("Unique keys: %d\n\n", len(keysEntries))

		fmt.Println("Entries per key:")

		for i := range percentiles {
			for key, count := range keysEntries {
				if count != percentiles[i].Value {
					continue
				}

				percentiles[i].ExampleKey = []byte(key)
				break
			}

			fmt.Printf("%d percentile distribution: %d (example key: 0x%x)\n", percentiles[i].P, percentiles[i].Value, percentiles[i].ExampleKey)
		}
	},
}

// histDupScan counts, per domain, how many history entries repeat the previous
// value for the same key. HistoryDump yields entries grouped by key and ordered
// by txNum, so a consecutive equal value is a redundant row (an as-of read
// collapses it away). Pure and stateless w.r.t. storage — fed one entry at a time.
type histDupScan struct {
	sampleLimit int

	prevKey  []byte
	prevVal  []byte
	havePrev bool
	curDup   bool

	Entries      uint64
	DistinctKeys uint64
	KeysWithDup  uint64
	DupPairs     uint64
	SampleKeys   [][]byte
}

func (s *histDupScan) observe(key, val []byte) {
	s.Entries++
	if s.havePrev && bytes.Equal(key, s.prevKey) {
		if bytes.Equal(val, s.prevVal) {
			s.DupPairs++
			if !s.curDup {
				s.curDup = true
				if len(s.SampleKeys) < s.sampleLimit {
					s.SampleKeys = append(s.SampleKeys, common.Copy(key))
				}
			}
		}
	} else {
		s.closeKey()
		s.DistinctKeys++
		s.curDup = false
	}
	s.prevKey = append(s.prevKey[:0], key...)
	s.prevVal = append(s.prevVal[:0], val...)
	s.havePrev = true
}

func (s *histDupScan) closeKey() {
	if s.curDup {
		s.KeysWithDup++
	}
}

func (s *histDupScan) finish() { s.closeKey() }

func historyDomainNames() []string {
	names := make([]string, 0, kv.DomainLen)
	for d := kv.Domain(0); d < kv.DomainLen; d++ {
		names = append(names, d.String())
	}
	return names
}

func scanDomainDuplicates(ctx context.Context, dirs datadir.Dirs, name string, logger log.Logger) (*histDupScan, error) {
	history, settings, err := openHistory(ctx, dirs, name, toStep, logger)
	if err != nil {
		return nil, err
	}
	defer history.Close()

	roTx := history.BeginFilesRoForDebug()
	defer roTx.Close()

	if dupSamples < 0 {
		return nil, fmt.Errorf("--samples must be >= 0")
	}
	if toStep < fromStep {
		return nil, fmt.Errorf("--to (%d) must be >= --from (%d)", toStep, fromStep)
	}

	stepSize := settings.StepSize
	if stepSize == 0 {
		return nil, fmt.Errorf("invalid stepSize=0")
	}
	maxInt := int(^uint(0) >> 1)
	maxStepForInt := uint64(maxInt) / stepSize
	if fromStep > maxStepForInt {
		return nil, fmt.Errorf("--from too large (from=%d, stepSize=%d)", fromStep, stepSize)
	}
	fromTxNum := int(fromStep * stepSize)

	// Use -1 to mean "unbounded" (per HistoryDump contract) when --to doesn't fit in int.
	toTxNum := -1
	if toStep <= maxStepForInt {
		toTxNum = int(toStep * stepSize)
	}

	scan := &histDupScan{sampleLimit: dupSamples}
	if err := roTx.HistoryDump(
		fromTxNum,
		toTxNum,
		nil,
		func(key []byte, _ uint64, val []byte) { scan.observe(key, val) },
	); err != nil {
		return nil, err
	}
	scan.finish()
	return scan, nil
}

var duplicatesCmd = &cobra.Command{
	Use:   "duplicates",
	Short: "Report keys whose history has consecutive duplicate (redundant) values, per domain",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")

		dirs, l, err := datadir.New(datadirCli).MustFlock()
		if err != nil {
			logger.Error("Opening Datadir", "error", err)
			return
		}
		defer l.Unlock()

		names := historyDomainNames()
		if historyDomain != "" {
			names = []string{historyDomain}
		}

		ctx := cmd.Context()
		var withDup []string
		for _, name := range names {
			scan, err := scanDomainDuplicates(ctx, dirs, name, logger)
			if err != nil {
				logger.Warn("skipping domain", "domain", name, "err", err)
				continue
			}
			if scan.Entries == 0 {
				fmt.Printf("domain=%-11s no history entries (disabled or empty)\n", name)
				continue
			}
			pct := float64(scan.DupPairs) * 100 / float64(scan.Entries)
			fmt.Printf("domain=%-11s entries=%-12d distinctKeys=%-12d keysWithDup=%-10d dupPairs=%-10d (%.2f%% of entries)\n",
				name, scan.Entries, scan.DistinctKeys, scan.KeysWithDup, scan.DupPairs, pct)
			if scan.DupPairs > 0 {
				withDup = append(withDup, name)
				for _, k := range scan.SampleKeys {
					fmt.Printf("    example key with duplicates: %x\n", k)
				}
			}
		}
		if len(withDup) == 0 {
			fmt.Println("no consecutive duplicate history values found")
		} else {
			fmt.Printf("domains with duplicate history values: %v\n", withDup)
		}
	},
}
