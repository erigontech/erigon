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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/etl"
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
	scanToTxNum, err := stepToTxNum(scanToStep, settings.StepSize)
	if err != nil {
		return nil, nil, err
	}
	if err := history.Scan(ctx, scanToTxNum); err != nil {
		return nil, nil, fmt.Errorf("scan history files: %w", err)
	}
	return history, settings, nil
}

// stepToTxNum converts a step bound to a txNum bound, saturating at MaxUint64
// rather than wrapping: --to defaults to 1e18, which overflows for every real
// step size.
func stepToTxNum(step, stepSize uint64) (uint64, error) {
	if stepSize == 0 {
		return 0, errors.New("invalid stepSize=0")
	}
	if step > math.MaxUint64/stepSize {
		return math.MaxUint64, nil
	}
	return step * stepSize, nil
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
		defer func() {
			if err := l.Unlock(); err != nil {
				logger.Error("failed to unlock datadir", "err", err)
			}
		}()

		history, settings, err := openHistory(cmd.Context(), dirs, historyDomain, toStep, logger)
		if err != nil {
			logger.Error("Failed to open history", "error", err)
			return
		}
		_, _, dumpFrom, dumpTo, err := stepBounds(settings.StepSize)
		if err != nil {
			logger.Error("Invalid step range", "error", err)
			return
		}

		roTx := history.BeginFilesRoForDebug()
		defer roTx.Close()

		var keyToDump *[]byte

		if historyKey != "" {
			key := common.Hex2Bytes(historyKey)
			keyToDump = &key
		}

		err = roTx.HistoryDump(
			dumpFrom,
			dumpTo,
			keyToDump,
			func(key []byte, txNum uint64, val []byte) error {
				fmt.Printf("key: %x, txn: %d, val: %x\n", key, txNum, val)
				return nil
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
		defer func() {
			if err := l.Unlock(); err != nil {
				logger.Error("failed to unlock datadir", "err", err)
			}
		}()

		history, settings, err := openHistory(cmd.Context(), dirs, historyDomain, toStep, logger)
		if err != nil {
			logger.Error("Failed to open history", "error", err)
			return
		}
		_, _, dumpFrom, dumpTo, err := stepBounds(settings.StepSize)
		if err != nil {
			logger.Error("Invalid step range", "error", err)
			return
		}

		roTx := history.BeginFilesRoForDebug()
		defer roTx.Close()

		keysEntries := make(map[string]int)
		uniqueEntries := 0

		err = roTx.HistoryDump(
			dumpFrom,
			dumpTo,
			nil,
			func(key []byte, txNum uint64, val []byte) error {
				keysEntries[string(key)] += 1
				uniqueEntries++
				return nil
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
// value for the same key: a consecutive equal value is a redundant row, since an
// as-of read collapses it away. It is fed one entry at a time and requires its
// caller to deliver them grouped by key and ordered by txNum — see histDupSorter,
// which is what establishes that order.
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
					s.SampleKeys = append(s.SampleKeys, bytes.Clone(key))
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

// errHistoryNotInFiles marks a domain whose history has not been collated into
// files yet. HistoryDump reads frozen .ef/.v only, so that domain's DB-resident
// history is not covered and the run must not report it as clean.
var errHistoryNotInFiles = errors.New("history not collated into files yet, so it was not scanned")

// errHistoryDisabled marks a domain the schema writes no history for at all -
// commitment and rcache, unless the node enabled them. Nothing is missing, so
// it is not a gap in the scan.
var errHistoryDisabled = errors.New("history disabled for this domain")

// historyOff reports whether the schema writes no history for a domain at all:
// commitment carries its own flag, rcache instead has no inverted index.
func historyOff(cfg statecfg.HistCfg) bool {
	return cfg.HistoryDisabled || !cfg.IiCfg.Enabled
}

// stepBounds resolves the --from/--to step flags to exact txNum bounds plus the
// coarse int pre-filter HistoryDump takes.
func stepBounds(stepSize uint64) (fromTxNum, toTxNum uint64, dumpFrom, dumpTo int, err error) {
	if fromTxNum, err = stepToTxNum(fromStep, stepSize); err != nil {
		return
	}
	if toTxNum, err = stepToTxNum(toStep, stepSize); err != nil {
		return
	}
	dumpFrom, dumpTo = dumpBounds(fromTxNum, toTxNum)
	return
}

// dumpBounds converts txNum bounds to HistoryDump's int arguments. A bound too
// large for int saturates rather than becoming HistoryDump's -1 "unbounded": for
// the upper bound those mean the same thing, but an unbounded lower bound would
// dump every entry where the caller asked for none. HistoryDump filters whole
// files only, so these are a coarse pre-filter; the exact bound is per entry.
func dumpBounds(fromTxNum, toTxNum uint64) (int, int) {
	from, to := math.MaxInt, -1
	if fromTxNum <= math.MaxInt {
		from = int(fromTxNum)
	}
	if toTxNum <= math.MaxInt {
		to = int(toTxNum)
	}
	return from, to
}

// histDupSorter replays entries grouped by key and ordered by txNum. HistoryDump
// walks files outer and keys inner, so the same key reappears once per .ef file
// with every other key in between; sorting is what makes "the previous entry for
// this key" mean the previous entry chain-wide rather than within one file.
type histDupSorter struct {
	collector *etl.Collector
	keyBuf    []byte
}

func newHistDupSorter(logPrefix, tmpdir string, logger log.Logger) *histDupSorter {
	return &histDupSorter{collector: etl.NewCollectorWithAllocator(logPrefix, tmpdir, etl.SmallSortableBuffers, logger)}
}

func (s *histDupSorter) Close() { s.collector.Close() }

// keyLenPrefix is the fixed-width length that keeps entries of one key
// contiguous once sorted. Plain key||txNum does not: where one key is a prefix
// of another - commitment's nibble prefixes always are - the longer key's own
// bytes can sort between two entries of the shorter one and split its run.
const keyLenPrefix = 4

func (s *histDupSorter) add(key []byte, txNum uint64, val []byte) error {
	// Collect copies what it is handed, so one scratch buffer serves every entry.
	s.keyBuf = binary.BigEndian.AppendUint32(s.keyBuf[:0], uint32(len(key)))
	s.keyBuf = append(s.keyBuf, key...)
	s.keyBuf = binary.BigEndian.AppendUint64(s.keyBuf, txNum)
	return s.collector.Collect(s.keyBuf, val)
}

func (s *histDupSorter) scan(ctx context.Context, sampleLimit int) (*histDupScan, error) {
	scan := &histDupScan{sampleLimit: sampleLimit}
	// bucket "" with a nil tx: ETL is a sort scratch-pad here, and that pair
	// also keeps an empty value (a deletion marker) from being dropped.
	if err := s.collector.Load(nil, "", func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		scan.observe(k[keyLenPrefix:len(k)-8], v)
		return nil
	}, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return nil, err
	}
	scan.closeKey()
	return scan, nil
}

func scanDomainDuplicates(ctx context.Context, dirs datadir.Dirs, name string, logger log.Logger) (*histDupScan, error) {
	history, settings, err := openHistory(ctx, dirs, name, toStep, logger)
	if err != nil {
		return nil, err
	}
	defer history.Close()

	roTx := history.BeginFilesRoForDebug()
	defer roTx.Close()
	if len(roTx.Files()) == 0 {
		// Only meaningful with no files: a datadir built with the history enabled
		// has them regardless of what this standalone binary's schema says.
		if historyOff(history.HistCfg) {
			return nil, errHistoryDisabled
		}
		return nil, errHistoryNotInFiles
	}

	fromTxNum, toTxNum, dumpFrom, dumpTo, err := stepBounds(settings.StepSize)
	if err != nil {
		return nil, err
	}

	sorter := newHistDupSorter(name+" history duplicates", dirs.Tmp, logger)
	defer sorter.Close()

	if err := roTx.HistoryDump(dumpFrom, dumpTo, nil, func(key []byte, txNum uint64, val []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		// HistoryDump's own from/to filtering is per file, so a partially
		// overlapping file yields entries outside the requested range.
		if txNum < fromTxNum || txNum >= toTxNum {
			return nil
		}
		return sorter.add(key, txNum, val)
	}); err != nil {
		return nil, err
	}
	return sorter.scan(ctx, dupSamples)
}

var duplicatesCmd = &cobra.Command{
	Use:   "duplicates",
	Short: "Report keys whose history has consecutive duplicate (redundant) values, per domain",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := debug.SetupCobra(cmd, "integration")

		if dupSamples < 0 {
			return fmt.Errorf("--samples must be >= 0, got %d", dupSamples)
		}
		if toStep < fromStep {
			return fmt.Errorf("--to (%d) must be >= --from (%d)", toStep, fromStep)
		}

		dirs, l, err := datadir.New(datadirCli).MustFlock()
		if err != nil {
			return fmt.Errorf("opening datadir: %w", err)
		}
		defer func() {
			if err := l.Unlock(); err != nil {
				logger.Error("failed to unlock datadir", "err", err)
			}
		}()

		names := make([]string, 0, kv.DomainLen)
		for d := range kv.DomainLen {
			names = append(names, d.String())
		}
		if historyDomain != "" {
			if _, err := kv.String2Domain(historyDomain); err != nil {
				return fmt.Errorf("--domain: %w", err)
			}
			names = []string{historyDomain}
		}

		ctx := cmd.Context()
		var withDup, unscanned []string
		for _, name := range names {
			scan, err := scanDomainDuplicates(ctx, dirs, name, logger)
			switch {
			case errors.Is(err, errHistoryDisabled):
				fmt.Printf("domain=%-11s %s\n", name, err)
				continue
			case errors.Is(err, errHistoryNotInFiles):
				fmt.Printf("domain=%-11s %s\n", name, err)
				unscanned = append(unscanned, name)
				continue
			case err != nil:
				// Every domain must be accounted for: a partial scan reported as
				// clean is worse than no scan at all.
				return fmt.Errorf("scan domain %s: %w", name, err)
			}
			if scan.Entries == 0 {
				fmt.Printf("domain=%-11s no history entries in the requested range\n", name)
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
		return duplicatesVerdict(withDup, unscanned)
	},
}

// duplicatesVerdict prints the closing summary and returns the run's error. An
// unscanned domain outranks a duplicate finding: a report that skipped a domain
// must not exit 0 just because another domain had something to report.
func duplicatesVerdict(withDup, unscanned []string) error {
	if len(withDup) > 0 {
		fmt.Printf("domains with duplicate history values: %v\n", withDup)
	} else if len(unscanned) == 0 {
		fmt.Println("no consecutive duplicate history values found")
	}
	if len(unscanned) > 0 {
		return fmt.Errorf("scan incomplete: %v have history only in the DB", unscanned)
	}
	return nil
}
