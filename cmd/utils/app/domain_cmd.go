package app

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/node/debug"
	"github.com/urfave/cli/v2"
)

func domainStat(cliCtx *cli.Context) error {
	_, _, _, _, err := debug.Setup(cliCtx, true /* root logger */)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	domain := cliCtx.Uint("domain")
	if domain > uint(kv.DomainLen-1) {
		return fmt.Errorf("invalid domain: %d", domain)
	}

	// Open chaindata
	ctx := cliCtx.Context
	dirs := datadir.Open(cliCtx.String(utils.DataDirFlag.Name))
	db, err := dbCfg(dbcfg.ChainDB, dirs.Chaindata).Open(ctx)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	domainCfg := statecfg.Schema.GetDomainCfg(kv.Domain(domain))
	var cursor kv.Cursor
	if domainCfg.LargeValues {
		cursor, err = tx.Cursor(domainCfg.ValuesTable)
		if err != nil {
			return err
		}
	} else {
		cursor, err = tx.CursorDupSort(domainCfg.ValuesTable)
		if err != nil {
			return err
		}
	}
	defer cursor.Close()

	stats := &DomainStat{
		minStep:            math.MaxUint64,
		maxStep:            0,
		minKeySize:         math.MaxInt,
		maxKeySize:         0,
		keySizeHistogram:   make(map[int]uint64),
		minValueSize:       math.MaxInt,
		maxValueSize:       0,
		valueSizeHistogram: make(map[int]uint64),
	}
	var step uint64
	var prevK []byte
	k, v, err := cursor.First()
	if err != nil {
		return err
	}
	for k != nil {
		stats.count++
		keySize, valSize := len(k), len(v)

		if domainCfg.LargeValues || !bytes.Equal(prevK, k) {
			stats.minKeySize = min(stats.minKeySize, keySize)
			stats.maxKeySize = max(stats.maxKeySize, keySize)
			stats.totalKeySize += keySize

			if _, ok := stats.keySizeHistogram[keySize]; !ok {
				stats.keySizeHistogram[keySize] = 0
			}
			stats.keySizeHistogram[keySize]++

			prevK = append(prevK[:0], k...)
			stats.keyCount++
		}

		stats.minValueSize = min(stats.minValueSize, valSize)
		stats.maxValueSize = max(stats.maxValueSize, valSize)
		stats.totalValueSize += valSize

		if _, ok := stats.valueSizeHistogram[valSize]; !ok {
			stats.valueSizeHistogram[valSize] = 0
		}
		stats.valueSizeHistogram[valSize]++

		if domainCfg.LargeValues {
			step = ^binary.BigEndian.Uint64(k[len(k)-8:])
		} else {
			step = ^binary.BigEndian.Uint64(v[:8])
		}
		stats.minStep = min(stats.minStep, step)
		stats.maxStep = max(stats.maxStep, step)

		select {
		case <-ticker.C:
			fmt.Printf("count: %d\n", stats.count)
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		k, v, err = cursor.Next()
		if err != nil {
			return err
		}
	}

	fmt.Printf("Domain statistics:\n\n")
	if domainCfg.LargeValues {
		fmt.Println("LARGE VALUES")
	}
	fmt.Printf("Step range: %d-%d\n", stats.minStep, stats.maxStep)
	fmt.Printf("Key size range: %d-%d byte(s)\n", stats.minKeySize, stats.maxKeySize)
	fmt.Printf("Value size range: %d-%d byte(s)\n", stats.minValueSize, stats.maxValueSize)
	fmt.Printf("Count: %d entries\n\n", stats.count)
	fmt.Printf("Total key size: %d byte(s)\n", stats.totalKeySize)
	fmt.Printf("Total value size: %d byte(s)\n\n", stats.totalValueSize)

	fmt.Printf("Key size histogram (bytes: count):\n")
	printSortedHistogram(stats.keySizeHistogram, stats.keyCount)
	fmt.Printf("\nValue size histogram: (bytes: count)\n")
	printSortedHistogram(stats.valueSizeHistogram, stats.count)

	return nil
}

func printSortedHistogram(hist map[int]uint64, total uint64) {
	keys := make([]int, 0, len(hist))
	for size := range hist {
		keys = append(keys, size)
	}
	slices.Sort(keys)

	for _, size := range keys {
		fmt.Printf("%d: %d (%0.2f%%)\n", size, hist[size], float64(hist[size])/float64(total)*100)
	}
	fmt.Printf("Total: %d\n", total)
}

type DomainStat struct {
	minStep uint64
	maxStep uint64

	minKeySize       int
	maxKeySize       int
	totalKeySize     int
	keySizeHistogram map[int]uint64

	minValueSize       int
	maxValueSize       int
	totalValueSize     int
	valueSizeHistogram map[int]uint64

	keyCount uint64
	count    uint64
}
