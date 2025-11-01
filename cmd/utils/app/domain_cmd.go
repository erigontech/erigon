package app

import (
	"encoding/binary"
	"fmt"
	"math"
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

	cursor, err := tx.CursorDupSort(statecfg.Schema.CommitmentDomain.ValuesTable)
	if err != nil {
		return err
	}
	defer cursor.Close()

	stats := &DomainStat{
		minStep:      math.MaxUint64,
		maxStep:      0,
		minKeySize:   math.MaxInt,
		maxKeySize:   0,
		minValueSize: math.MaxInt,
		maxValueSize: 0,
	}
	k, v, err := cursor.First()
	if err != nil {
		return err
	}
	for k != nil {
		stats.count++
		if len(k) < stats.minKeySize {
			stats.minKeySize = len(k)
		}
		if len(k) > stats.maxKeySize {
			stats.maxKeySize = len(k)
		}
		stats.totalKeySize += len(k)
		if len(v) < stats.minValueSize {
			stats.minValueSize = len(v)
		}
		if len(v) > stats.maxValueSize {
			stats.maxValueSize = len(v)
		}
		stats.totalValueSize += len(v)

		step := ^binary.BigEndian.Uint64(v[:8])
		if step < stats.minStep {
			stats.minStep = step
		}
		if step > stats.maxStep {
			stats.maxStep = step
		}

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
	fmt.Printf("Step range: %d-%d\n", stats.minStep, stats.maxStep)
	fmt.Printf("Key size range: %d-%d byte(s)\n", stats.minKeySize, stats.maxKeySize)
	fmt.Printf("Value size range: %d-%d byte(s)\n", stats.minValueSize, stats.maxValueSize)
	fmt.Printf("Count: %d entries\n\n", stats.count)
	fmt.Printf("Total key size: %d byte(s)\n", stats.totalKeySize)
	fmt.Printf("Total value size: %d byte(s)\n", stats.totalValueSize)

	return nil
}

type DomainStat struct {
	minStep        uint64
	maxStep        uint64
	minKeySize     int
	maxKeySize     int
	totalKeySize   int
	minValueSize   int
	maxValueSize   int
	totalValueSize int
	count          uint64
}
