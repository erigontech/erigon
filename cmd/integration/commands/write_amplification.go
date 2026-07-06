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

package commands

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/seg"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/node/debug"
)

var writeAmpDomainsToAnalyze string

func init() {
	withDataDir(writeAmplificationCmd)
	withChain(writeAmplificationCmd)
	writeAmplificationCmd.Flags().StringVar(&writeAmpDomainsToAnalyze, "domains", "all",
		`Comma-separated list of domains to analyze. 
Available domains: accounts, storage, code, commitment.
Use "all" to analyze all domains.
Examples: --domains=all, --domains=storage,commitment, --domains=accounts`)
	rootCmd.AddCommand(writeAmplificationCmd)
}

var writeAmplificationCmd = &cobra.Command{
	Use:   "write_amplification",
	Short: "Calculate write amplification ratio for domain .kv files",
	Long: `Calculates write amplification ration for accounts, storage, code, and commitment domains.

The write amplification ratio is calculated as:
  total keys in .kv files / unique keys (via RangeLatest)

This measures how many times each unique key appears across all .kv files,
indicating data duplication in the snapshots.

Flags:
  --domains    Comma-separated list of domains to analyze.
               Available: accounts, storage, code, commitment
               Default: "all" (analyzes all domains)
               Examples: --domains=storage,commitment`,
	Example: `go run ./cmd/integration write_amplification --datadir=... --chain=mainnet
go run ./cmd/integration write_amplification --datadir=/path/to/db --chain=mainnet --domains=storage,commitment
go run ./cmd/integration write_amplification --datadir=/path/to/db --chain=mainnet --domains=accounts`,
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		ctx, _ := common.RootContext()

		dirs := datadir.New(datadirCli)
		chainDb, err := openDB(dbCfg(dbcfg.ChainDB, dirs.Chaindata), true, chain, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer chainDb.Close()

		// Parse domains flag
		domains, err := parseDomainsFlag(writeAmpDomainsToAnalyze)
		if err != nil {
			logger.Error("Invalid --domains flag", "error", err)
			return
		}

		if err := calculateWriteAmplification(ctx, chainDb, domains, logger); err != nil {
			logger.Error("Failed to calculate write amplification", "error", err)
			return
		}
	},
}

// parseDomainsFlag parses the --domains flag value and returns the list of domains to analyze.
// Returns all domains if "all" is specified.
func parseDomainsFlag(domainsStr string) ([]kv.Domain, error) {
	allDomains := []kv.Domain{
		kv.AccountsDomain,
		kv.StorageDomain,
		kv.CodeDomain,
		kv.CommitmentDomain,
	}

	if strings.ToLower(strings.TrimSpace(domainsStr)) == "all" {
		return allDomains, nil
	}

	domainMap := map[string]kv.Domain{
		"accounts":   kv.AccountsDomain,
		"account":    kv.AccountsDomain,
		"storage":    kv.StorageDomain,
		"code":       kv.CodeDomain,
		"commitment": kv.CommitmentDomain,
	}

	parts := strings.Split(domainsStr, ",")
	result := make([]kv.Domain, 0, len(parts))
	seen := make(map[kv.Domain]bool)

	for _, part := range parts {
		name := strings.ToLower(strings.TrimSpace(part))
		if name == "" {
			continue
		}
		domain, ok := domainMap[name]
		if !ok {
			return nil, fmt.Errorf("unknown domain %q, available: accounts, storage, code, commitment", part)
		}
		if !seen[domain] {
			result = append(result, domain)
			seen[domain] = true
		}
	}

	if len(result) == 0 {
		return nil, fmt.Errorf("no valid domains specified")
	}

	return result, nil
}

type domainStats struct {
	Domain        kv.Domain
	UniqueKeys    uint64
	TotalKeysKV   uint64
	FileCount     int
	WriteAmpRatio float64
}

func calculateWriteAmplification(ctx context.Context, chainDb kv.TemporalRwDB, domains []kv.Domain, logger log.Logger) error {
	tx, err := chainDb.BeginTemporalRo(ctx)
	if err != nil {
		return fmt.Errorf("begin temporal tx: %w", err)
	}
	defer tx.Rollback()

	aggTx := dbstate.AggTx(tx)
	if aggTx == nil {
		return fmt.Errorf("failed to get aggregator tx")
	}

	results := make([]domainStats, 0, len(domains))

	for _, domain := range domains {
		logger.Info("Processing domain", "domain", domain.String())

		stats, err := calculateDomainWriteAmplification(ctx, tx, aggTx, domain, logger)
		if err != nil {
			logger.Error("Failed to process domain", "domain", domain.String(), "error", err)
			continue
		}
		results = append(results, stats)
	}

	// Print summary
	logger.Info("=== Write Amplification Summary ===")
	fmt.Println()
	fmt.Printf("%-15s %15s %18s %10s %15s\n", "Domain", "Unique Keys", "Total Keys in .kv", "Files", "Write Amp")
	fmt.Println(strings.Repeat("-", 78))

	for _, stats := range results {
		fmt.Printf("%-15s %15s %18s %10d %15.2fx\n",
			stats.Domain.String(),
			common.PrettyCounter(stats.UniqueKeys),
			common.PrettyCounter(stats.TotalKeysKV),
			stats.FileCount,
			stats.WriteAmpRatio,
		)
	}

	return nil
}

func calculateDomainWriteAmplification(
	ctx context.Context,
	tx kv.TemporalTx,
	aggTx *dbstate.AggregatorRoTx,
	domain kv.Domain,
	logger log.Logger,
) (domainStats, error) {
	stats := domainStats{Domain: domain}

	// Count unique keys using RangeLatest iterator (files only, ignoring MDBX)
	startTime := time.Now()
	logger.Info("Counting unique keys via RangeLatestFromFiles", "domain", domain.String())

	iter, err := aggTx.DebugRangeLatestFromFiles(domain, nil, nil, -1)
	if err != nil {
		return stats, fmt.Errorf("DebugRangeLatestFromFiles: %w", err)
	}

	logTicker := time.NewTicker(10 * time.Second)
	defer logTicker.Stop()

	for iter.HasNext() {
		_, _, err := iter.Next()
		if err != nil {
			iter.Close()
			return stats, fmt.Errorf("iterator next: %w", err)
		}
		stats.UniqueKeys++

		select {
		case <-logTicker.C:
			logger.Info("Progress counting unique keys",
				"domain", domain.String(),
				"count", common.PrettyCounter(stats.UniqueKeys),
				"elapsed", time.Since(startTime).Round(time.Second))
		case <-ctx.Done():
			iter.Close()
			return stats, ctx.Err()
		default:
		}
	}
	iter.Close()

	logger.Info("Finished counting unique keys",
		"domain", domain.String(),
		"uniqueKeys", common.PrettyCounter(stats.UniqueKeys),
		"elapsed", time.Since(startTime).Round(time.Second))

	// Count total keys in .kv files
	startTime = time.Now()
	logger.Info("Counting total keys in .kv files", "domain", domain.String())

	files := tx.Debug().DomainFiles(domain)
	kvFiles := make([]string, 0)
	for _, f := range files {
		path := f.Fullpath()
		if strings.HasSuffix(path, ".kv") {
			kvFiles = append(kvFiles, path)
		}
	}
	stats.FileCount = len(kvFiles)

	logTicker.Reset(10 * time.Second)

	for i, filePath := range kvFiles {
		dec, err := seg.NewDecompressor(filePath)
		if err != nil {
			return stats, fmt.Errorf("decompressor for %s: %w", filePath, err)
		}

		getter := dec.MakeGetter()
		fileKeyCount := uint64(0)

		for getter.HasNext() {
			_, _ = getter.Next(nil)
			fileKeyCount++
			// Skip value
			getter.Skip()

			select {
			case <-logTicker.C:
				logger.Info("Progress counting .kv keys",
					"domain", domain.String(),
					"file", fmt.Sprintf("%d/%d", i+1, len(kvFiles)),
					"totalKeys", common.PrettyCounter(stats.TotalKeysKV+fileKeyCount),
					"elapsed", time.Since(startTime).Round(time.Second))
			case <-ctx.Done():
				dec.Close()
				return stats, ctx.Err()
			default:
			}
		}
		dec.Close()

		stats.TotalKeysKV += fileKeyCount
	}

	logger.Info("Finished counting .kv keys",
		"domain", domain.String(),
		"totalKeysKV", common.PrettyCounter(stats.TotalKeysKV),
		"files", stats.FileCount,
		"elapsed", time.Since(startTime).Round(time.Second))

	// Calculate write amplification ratio
	if stats.UniqueKeys > 0 {
		stats.WriteAmpRatio = float64(stats.TotalKeysKV) / float64(stats.UniqueKeys)
	}

	return stats, nil
}
