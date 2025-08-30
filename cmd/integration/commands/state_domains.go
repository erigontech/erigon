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
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/estimate"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/seg"
	downloadertype "github.com/erigontech/erigon/db/snaptype"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/eth/ethconfig"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/node/nodecfg"
	erigoncli "github.com/erigontech/erigon/turbo/cli"
	"github.com/erigontech/erigon/turbo/debug"

	_ "github.com/erigontech/erigon/polygon/chain" // Register Polygon chains
)

func init() {
	withDataDir(readDomains)
	withChain(readDomains)
	withHeimdall(readDomains)
	withWorkers(readDomains)
	withStartTx(readDomains)

	rootCmd.AddCommand(readDomains)

	withDataDir(compactDomains)
	withDomain(compactDomains)
	compactDomains.Flags().StringVar(&outDatadir, "out", "out-compacted", "")
	compactDomains.Flags().BoolVar(&replaceInDatadir, "replace-in-datadir", false, "replace the compacted domains directly in datadir (will remove .kvei and .bt too)")
	compactDomains.Flags().BoolVar(&doIndexBuild, "build-idx", false, "build index for compacted domains")
	compactDomains.Flags().Float64Var(&minSkipRatioL0, "min-skip-ratio-l0", 0.1, "deprecated: minimum ratio of keys to skip in L0")
	compactDomains.Flags().Float64Var(&minSkipRatio, "min-skip-ratio", 0.1, "minimum ratio of keys to skip - otherwise keep file unchanged")
	compactDomains.Flags().Uint64Var(&fromStepCompaction, "from", 0, "step from which domains would be compacted")
	compactDomains.Flags().Uint64Var(&toStepCompaction, "to", 1e18, "step to which domains would be compacted")
	rootCmd.AddCommand(compactDomains)
}

// if trie variant is not hex, we could not have another rootHash with to verify it
var (
	stepSize                     uint64
	lastStep                     uint64
	minSkipRatioL0, minSkipRatio float64
	outDatadir                   string
	replaceInDatadir             bool
	doIndexBuild                 bool
	fromStepCompaction           uint64
	toStepCompaction             uint64
)

// write command to just seek and query state by addr and domain from state db and files (if any)
var readDomains = &cobra.Command{
	Use:       "read_domains",
	Short:     `Run block execution and commitment with Domains.`,
	Example:   "go run ./cmd/integration read_domains --datadir=... --verbosity=3",
	ValidArgs: []string{"account", "storage", "code", "commitment"},
	Args:      cobra.ArbitraryArgs,
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		ctx, _ := common.RootContext()
		cfg := &nodecfg.DefaultConfig
		utils.SetNodeConfigCobra(cmd, cfg)
		ethConfig := &ethconfig.Defaults

		spec, err := chainspec.ChainSpecByName(chain)
		if err != nil {
			utils.Fatalf("unknown chain %s", chain)
		}
		ethConfig.Genesis = spec.Genesis
		erigoncli.ApplyFlagsForEthConfigCobra(cmd.Flags(), ethConfig)

		var readFromDomain string
		var addrs [][]byte
		for i := 0; i < len(args); i++ {
			if i == 0 {
				switch s := strings.ToLower(args[i]); s {
				case "account", "storage", "code", "commitment":
					readFromDomain = s
				default:
					logger.Error("invalid domain to read from", "arg", args[i])
					return
				}
				continue
			}
			addr, err := hex.DecodeString(strings.TrimPrefix(args[i], "0x"))
			if err != nil {
				logger.Warn("invalid address passed", "str", args[i], "at position", i, "err", err)
				continue
			}
			addrs = append(addrs, addr)
		}

		dirs := datadir.New(datadirCli)
		chainDb, err := openDB(dbCfg(kv.ChainDB, dirs.Chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer chainDb.Close()

		stateDb, err := mdbx.New(kv.ChainDB, log.New()).Path(filepath.Join(dirs.DataDir, "statedb")).WriteMap(true).Open(ctx)
		if err != nil {
			return
		}
		defer stateDb.Close()

		if err := requestDomains(chainDb, stateDb, ctx, readFromDomain, addrs, logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

var compactDomains = &cobra.Command{
	Use:     "compact_domains",
	Aliases: []string{"purify_domains"},
	Short:   `Regenerate kv files without repeating keys.`,
	Example: "go run ./cmd/integration compact_domains --datadir=... --verbosity=3",
	Args:    cobra.ArbitraryArgs,
	Run: func(cmd *cobra.Command, args []string) {
		ctx, _ := common.RootContext()
		dirs := datadir.New(datadirCli)
		logger := debug.SetupCobra(cmd, "integration")
		if minSkipRatio <= 0.0 {
			panic("--min-skip-ratio must be > 0")
		}

		if !replaceInDatadir && doIndexBuild {
			panic("can't build index when replace-in-datadir=false (consider removing --build-idx)")
		}

		chainDb, err := openDB(dbCfg(kv.ChainDB, dirs.Chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer chainDb.Close()

		tx, err := chainDb.BeginTemporalRo(ctx)
		if err != nil {
			logger.Error("Opening temporal DB", "error", err)
			return
		}
		defer tx.Rollback()
		defer dbstate.AggTx(tx).MadvNormal().DisableReadAhead()

		// Iterate over all the files in  dirs.SnapDomain and print them
		domainDir := dirs.SnapDomain

		// make a temporary dir
		tmpDir, err := os.MkdirTemp(dirs.Tmp, "compactTemp") // make a temporary dir to store the keys
		if err != nil {
			logger.Error("Error creating temporary directory", "error", err)
			return
		}
		defer dir.RemoveAll(tmpDir)
		// make a temporary DB to store the keys

		compactionDB := mdbx.MustOpen(tmpDir)
		defer compactionDB.Close()

		domainsStr := strings.Split(domain, ",")
		if len(domainsStr) == 0 {
			logger.Error("No domains specified")
			return
		}
		supportedDomain := []kv.Domain{kv.CommitmentDomain, kv.AccountsDomain, kv.StorageDomain, kv.CommitmentDomain}
		var compactionDomains []kv.Domain

		for _, domain := range domainsStr {
			found := false
			for _, supportedDomain := range supportedDomain {
				if strings.ToLower(domain) == strings.ToLower(supportedDomain.String()) {
					found = true
					compactionDomains = append(compactionDomains, supportedDomain)
					break
				}
			}
			if !found {
				logger.Error("Domain not supported", "domain", domain)
				return
			}
		}

		for _, domain := range compactionDomains {
			filesToProcess := tx.Debug().DomainFiles(domain).Fullpaths()
			if err := makeCompactableIndexDB(ctx, compactionDB, filesToProcess, dirs, log.New(), domain); err != nil {
				logger.Error("Error making compactable index DB", "error", err)
				return
			}
		}
		somethingCompacted := false
		for _, domain := range compactionDomains {
			filesToProcess := tx.Debug().DomainFiles(domain).Fullpaths()
			something, err := makeCompactDomains(ctx, compactionDB, filesToProcess, dirs, log.New(), domain)
			if err != nil {
				logger.Error("Error making compact domains", "error", err)
				return
			}
			somethingCompacted = somethingCompacted || something
		}
		if replaceInDatadir && doIndexBuild && somethingCompacted {
			logger.Info("building index for the compacted files...")
			if err := chainDb.Debug().ReloadFiles(); err != nil {
				logger.Error("Error re-opening folder after compaction", "error", err)
				return
			}

			if err := chainDb.Debug().BuildMissedAccessors(ctx, estimate.IndexSnapshot.Workers()); err != nil {
				logger.Error("Error rebuilding missed accessors", "error", err)
				return
			}
		}
		if err != nil {
			logger.Error("error walking the path", "domainDir", domainDir, "error", err)
		}
	},
}

func makeCompactableIndexDB(ctx context.Context, db kv.RwDB, files []string, dirs datadir.Dirs, logger log.Logger, domain kv.Domain) error {
	var tbl string
	switch domain {
	case kv.AccountsDomain:
		tbl = kv.MaxTxNum
	case kv.StorageDomain:
		tbl = kv.HeaderNumber
	case kv.CodeDomain:
		tbl = kv.HeaderCanonical
	case kv.CommitmentDomain:
		tbl = kv.HeaderTD
	case kv.ReceiptDomain:
		tbl = kv.BadHeaderNumber
	default:
		return fmt.Errorf("invalid domain %s", domain)
	}
	// Iterate over all the files in  dirs.SnapDomain and print them
	fileInfos := []downloadertype.FileInfo{}
	for _, f := range files {
		dirPart, fileName := filepath.Split(f)
		res, ok, _ := downloadertype.ParseFileName(dirPart, fileName)
		if !ok {
			panic("invalid file name")
		}
		if res.From < fromStepCompaction || res.To > toStepCompaction {
			continue
		}
		fileInfos = append(fileInfos, res)
	}
	// sort the files by name
	sort.Slice(fileInfos, func(i, j int) bool {
		return fileInfos[i].CompareTo(fileInfos[j]) <= 0
	})

	collector := etl.NewCollectorWithAllocator("Compaction", dirs.Tmp, etl.LargeSortableBuffers, logger)
	defer collector.Close()
	collector.LogLvl(log.LvlDebug)
	collector.SortAndFlushInBackground(true)

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	// now start the file indexing
	for i, fileInfo := range fileInfos {
		if i == 0 {
			continue // we can skip first layer as all the keys are already mapped to 0.
		}
		baseFileName := fileInfo.Base()
		layerBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(layerBytes, uint32(i))
		count := 0

		dec, err := seg.NewDecompressor(fileInfo.Path)
		if err != nil {
			return fmt.Errorf("failed to create decompressor: %w", err)
		}
		defer dec.Close()
		getter := dec.MakeGetter()
		logger.Info("Indexing", "file", baseFileName)
		var buf []byte
		for getter.HasNext() {
			buf, _ = getter.Next(buf[:0])

			if err := collector.Collect(buf, layerBytes); err != nil {
				return err
			}
			count++
			//fmt.Println("count: ", count, "keyLength: ", len(buf))
			if count%10_000_000 == 0 {
				logger.Info(fmt.Sprintf("[compaction] Indexed %dM keys in file %s", count/1_000_000, baseFileName))
			}
			// skip values
			getter.Skip()
		}
		logger.Info(fmt.Sprintf("Indexed %dM keys in file %s", count/1_000_000, baseFileName))
	}
	logger.Info("Loading the keys to DB")
	if err := collector.Load(tx, tbl, etl.IdentityLoadFunc, etl.TransformArgs{Quit: ctx.Done()}); err != nil {
		return fmt.Errorf("failed to load: %w", err)
	}

	return tx.Commit()
}

func makeCompactDomains(ctx context.Context, db kv.RwDB, files []string, dirs datadir.Dirs, logger log.Logger, domain kv.Domain) (somethingCompacted bool, err error) {
	compressionType := statecfg.Schema.GetDomainCfg(domain).Compression
	compressCfg := statecfg.Schema.GetDomainCfg(domain).CompressCfg
	compressCfg.Workers = runtime.NumCPU()
	var tbl string
	switch domain {
	case kv.AccountsDomain:
		tbl = kv.MaxTxNum
	case kv.StorageDomain:
		tbl = kv.HeaderNumber
	case kv.CodeDomain:
		tbl = kv.HeaderCanonical
	case kv.CommitmentDomain:
		tbl = kv.HeaderTD
	case kv.ReceiptDomain:
		tbl = kv.BadHeaderNumber
	case kv.RCacheDomain:
		tbl = kv.BlockBody
	default:
		return false, fmt.Errorf("invalid domainName %s", domain.String())
	}
	// Iterate over all the files in  dirs.SnapDomain and print them
	fileInfos := []downloadertype.FileInfo{}
	for _, f := range files {
		dirPart, fileName := filepath.Split(f)
		res, ok, _ := downloadertype.ParseFileName(dirPart, fileName)
		if !ok {
			panic("invalid file name")
		}
		if res.From < fromStepCompaction || res.To > toStepCompaction {
			continue
		}
		fileInfos = append(fileInfos, res)
	}
	// sort the files by name
	sort.Slice(fileInfos, func(i, j int) bool {
		return fileInfos[i].CompareTo(fileInfos[j]) <= 0
	})

	tx, err := db.BeginRo(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()
	outD := datadir.New(outDatadir)

	// now start the file indexing
	for currentLayer, fileInfo := range fileInfos {
		baseFileName := fileInfo.Base()
		outputFilePath := filepath.Join(outD.SnapDomain, baseFileName)
		count := 0
		skipped := 0

		dec, err := seg.NewDecompressor(fileInfo.Path)
		if err != nil {
			return false, fmt.Errorf("failed to create decompressor: %w", err)
		}
		defer dec.Close()
		getter := dec.MakeGetter()

		valuesComp, err := seg.NewCompressor(ctx, "Compaction", outputFilePath, dirs.Tmp, compressCfg, log.LvlTrace, log.New())
		if err != nil {
			return false, fmt.Errorf("create %s values compressor: %w", outputFilePath, err)
		}
		defer valuesComp.Close()

		comp := seg.NewWriter(valuesComp, compressionType)
		defer comp.Close()

		logger.Info("Indexing", "file", baseFileName)
		var k, v []byte

		var layer uint32
		for getter.HasNext() {
			// get the key and value for the current entry
			k, _ = getter.Next(k[:0])
			v, _ = getter.Next(v[:0])

			layerBytes, err := tx.GetOne(tbl, k)
			if err != nil {
				return false, fmt.Errorf("failed to get key %x: %w", k, err)
			}
			// if the key is not found, then the layer is 0
			layer = 0
			if len(layerBytes) == 4 {
				layer = binary.BigEndian.Uint32(layerBytes)
			}
			if layer != uint32(currentLayer) {
				skipped++
				continue
			}
			if _, err := comp.Write(k); err != nil {
				return false, fmt.Errorf("failed to add key %x: %w", k, err)
			}
			if _, err := comp.Write(v); err != nil {
				return false, fmt.Errorf("failed to add val %x: %w", v, err)
			}
			count++
			if count%10_000_000 == 0 {
				skipRatio := float64(skipped) / float64(count)
				logger.Info(fmt.Sprintf("Indexed %dM keys, skipped %dM, in file %s. skip ratio: %.2f", count/1_000_000, skipped/1_000_000, baseFileName, skipRatio))
			}
		}

		skipRatio := float64(skipped) / float64(count)
		if skipRatio < minSkipRatio {
			logger.Info(fmt.Sprintf("Skip ratio %.2f is less than min-skip-ratio %.2f, skipping %s", skipRatio, minSkipRatio, baseFileName))
			continue
		}
		logger.Info(fmt.Sprintf("Loaded %dM keys in file %s. now compressing...", count/1_000_000, baseFileName))
		if err := comp.Compress(); err != nil {
			return false, fmt.Errorf("failed to compress: %w", err)
		}
		logger.Info(fmt.Sprintf("Compressed %dM keys in file %s", count/1_000_000, baseFileName))
		comp.Close()
		if replaceInDatadir {
			logger.Info(fmt.Sprintf("Replacing the file %s in datadir", baseFileName))
			if err := os.Rename(outputFilePath, fileInfo.Path); err != nil {
				return false, fmt.Errorf("failed to replace the file %s: %w", baseFileName, err)
			}
			kveiFile := strings.ReplaceAll(baseFileName, ".kv", ".kvei")
			btFile := strings.ReplaceAll(baseFileName, ".kv", ".bt")
			kviFile := strings.ReplaceAll(baseFileName, ".kv", ".kvi")
			removeManyIgnoreError(
				filepath.Join(dirs.SnapDomain, baseFileName+".torrent"),
				filepath.Join(dirs.SnapDomain, btFile),
				filepath.Join(dirs.SnapDomain, btFile+".torrent"),
				filepath.Join(dirs.SnapDomain, kveiFile),
				filepath.Join(dirs.SnapDomain, kveiFile+".torrent"),
				filepath.Join(dirs.SnapDomain, kviFile),
				filepath.Join(dirs.SnapDomain, kviFile+".torrent"),
			)
			logger.Info(fmt.Sprintf("Removed the files %s and %s", kveiFile, btFile))
		}
		somethingCompacted = true
	}

	return somethingCompacted, nil
}

func requestDomains(chainDb, stateDb kv.RwDB, ctx context.Context, readDomain string, addrs [][]byte, logger log.Logger) error {
	stateTx, err := stateDb.BeginRw(ctx)
	must(err)
	defer stateTx.Rollback()
	temporalTx, ok := stateTx.(kv.TemporalTx)
	if !ok {
		return errors.New("stateDb transaction is not a temporal transaction")
	}
	domains, err := dbstate.NewSharedDomains(temporalTx, logger)
	if err != nil {
		return err
	}

	r := state.NewReaderV3(domains.AsGetter(temporalTx))
	if startTxNum != 0 {
		return fmt.Errorf("failed to seek commitment to txn %d: %w", startTxNum, err)
	}
	latestTx := domains.TxNum()
	if latestTx < startTxNum {
		return fmt.Errorf("latest available txn to start is  %d and its less than start txn %d", latestTx, startTxNum)
	}
	logger.Info("seek commitment", "block", domains.BlockNum(), "tx", latestTx)

	switch readDomain {
	case kv.AccountsDomain.String():
		for _, addr := range addrs {

			acc, err := r.ReadAccountData(common.BytesToAddress(addr))
			if err != nil {
				logger.Error("failed to read account", "addr", addr, "err", err)
				continue
			}
			logger.Info(fmt.Sprintf("%x: nonce=%d balance=%d code=%x root=%x", addr, acc.Nonce, acc.Balance.Uint64(), acc.CodeHash, acc.Root))
		}
	case kv.StorageDomain.String():
		for _, addr := range addrs {
			a, s := common.BytesToAddress(addr[:length.Addr]), common.BytesToHash(addr[length.Addr:])
			st, _, err := r.ReadAccountStorage(a, s)
			if err != nil {
				logger.Error("failed to read storage", "addr", a.String(), "key", s.String(), "err", err)
				continue
			}
			logger.Info(fmt.Sprintf("%s %s -> %x", a.String(), s.String(), st))
		}
	case kv.CodeDomain.String():
		for _, addr := range addrs {
			code, err := r.ReadAccountCode(common.BytesToAddress(addr))
			if err != nil {
				logger.Error("failed to read code", "addr", addr, "err", err)
				continue
			}
			logger.Info(fmt.Sprintf("%s: %x", addr, code))
		}
	}
	return nil
}

func removeManyIgnoreError(filePaths ...string) {
	for _, filePath := range filePaths {
		dir.RemoveFile(filePath)
	}
}
