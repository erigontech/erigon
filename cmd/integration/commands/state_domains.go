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
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/length"
	downloadertype "github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/mdbx"
	kv2 "github.com/erigontech/erigon-lib/kv/mdbx"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/seg"
	state3 "github.com/erigontech/erigon-lib/state"
	statelib "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/node/nodecfg"
	erigoncli "github.com/erigontech/erigon/turbo/cli"
	"github.com/erigontech/erigon/turbo/debug"
)

func init() {
	withDataDir(readDomains)
	withChain(readDomains)
	withHeimdall(readDomains)
	withWorkers(readDomains)
	withStartTx(readDomains)

	rootCmd.AddCommand(readDomains)

	withDataDir(purifyDomains)
	purifyDomains.Flags().StringVar(&outDatadir, "out", "out-purified", "")
	purifyDomains.Flags().BoolVar(&purifyOnlyCommitment, "only-commitment", true, "purify only commitment domain")
	purifyDomains.Flags().BoolVar(&replaceInDatadir, "replace-in-datadir", false, "replace the purified domains directly in datadir (will remove .kvei and .bt too)")
	purifyDomains.Flags().Float64Var(&minSkipRatioL0, "min-skip-ratio-l0", 0.1, "minimum ratio of keys to skip in L0")
	purifyDomains.Flags().Float64Var(&minSkipRatio, "min-skip-ratio", 0.1, "minimum ratio of keys to skip - otherwise keep file unchanged")
	purifyDomains.Flags().Uint64Var(&fromStepPurification, "from", 0, "step from which domains would be purified")
	purifyDomains.Flags().Uint64Var(&toStepPurification, "to", 1e18, "step to which domains would be purified")
	rootCmd.AddCommand(purifyDomains)
}

// if trie variant is not hex, we could not have another rootHash with to verify it
var (
	stepSize                     uint64
	lastStep                     uint64
	minSkipRatioL0, minSkipRatio float64
	outDatadir                   string
	purifyOnlyCommitment         bool
	replaceInDatadir             bool
	fromStepPurification         uint64
	toStepPurification           uint64
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
		ethConfig.Genesis = core.GenesisBlockByChainName(chain)
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

		stateDb, err := kv2.New(kv.ChainDB, log.New()).Path(filepath.Join(dirs.DataDir, "statedb")).WriteMap(true).Open(ctx)
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

var purifyDomains = &cobra.Command{
	Use:     "purify_domains",
	Short:   `Regenerate kv files without repeating keys.`,
	Example: "go run ./cmd/integration purify_domains --datadir=... --verbosity=3",
	Args:    cobra.ArbitraryArgs,
	Run: func(cmd *cobra.Command, args []string) {
		if minSkipRatioL0 <= 0.0 {
			panic("--min-skip-ratio-l0 must be > 0")
		}
		if minSkipRatio <= 0.0 {
			panic("--min-skip-ratio must be > 0")
		}
		dirs := datadir.New(datadirCli)
		// Iterate over all the files in  dirs.SnapDomain and print them
		domainDir := dirs.SnapDomain

		// make a temporary dir
		tmpDir, err := os.MkdirTemp(dirs.Tmp, "purifyTemp") // make a temporary dir to store the keys
		if err != nil {
			fmt.Println("Error creating temporary directory: ", err)
			return
		}
		// make a temporary DB to store the keys

		purifyDB := mdbx.MustOpen(tmpDir)
		defer purifyDB.Close()
		var purificationDomains []kv.Domain
		if purifyOnlyCommitment {
			purificationDomains = []kv.Domain{kv.CommitmentDomain}
		} else {
			purificationDomains = []kv.Domain{kv.AccountsDomain, kv.StorageDomain /*"code",*/, kv.CommitmentDomain}
		}
		//purificationDomains := []string{"commitment"}
		for _, domain := range purificationDomains {
			if err := makePurifiableIndexDB(purifyDB, dirs, log.New(), domain); err != nil {
				fmt.Println("Error making purifiable index DB: ", err)
				return
			}
		}
		for _, domain := range purificationDomains {
			if err := makePurifiedDomains(purifyDB, dirs, log.New(), domain); err != nil {
				fmt.Println("Error making purifiable index DB: ", err)
				return
			}
		}
		if err != nil {
			fmt.Printf("error walking the path %q: %v\n", domainDir, err)
		}
	},
}

func makePurifiableIndexDB(db kv.RwDB, dirs datadir.Dirs, logger log.Logger, domain kv.Domain) error {
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
	filesNamesToIndex := []string{}
	if err := filepath.Walk(dirs.SnapDomain, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Skip directories
		if info.IsDir() {
			return nil
		}
		if !strings.Contains(info.Name(), domain.String()) {
			return nil
		}
		// Here you can decide if you only want to process certain file extensions
		// e.g., .kv files
		if filepath.Ext(path) != ".kv" {
			// Skip non-kv files if that's your domain’s format
			return nil
		}

		res, ok, _ := downloadertype.ParseFileName("", info.Name())
		if !ok {
			panic("invalid file name")
		}

		if res.From < fromStepPurification || res.To > toStepPurification {
			return nil
		}

		fmt.Printf("Add file to indexing of %s: %s\n", domain, path)

		filesNamesToIndex = append(filesNamesToIndex, info.Name())
		return nil
	}); err != nil {
		return fmt.Errorf("failed to walk through the domainDir %s: %w", domain, err)
	}

	collector := etl.NewCollector("Purification", dirs.Tmp, etl.NewSortableBuffer(etl.BufferOptimalSize), logger)
	defer collector.Close()
	// sort the files by name
	sort.Slice(filesNamesToIndex, func(i, j int) bool {
		res, ok, _ := downloadertype.ParseFileName(dirs.SnapDomain, filesNamesToIndex[i])
		if !ok {
			panic("invalid file name")
		}
		res2, ok, _ := downloadertype.ParseFileName(dirs.SnapDomain, filesNamesToIndex[j])
		if !ok {
			panic("invalid file name")
		}
		return res.From < res2.From
	})
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()

	// now start the file indexing
	for i, fileName := range filesNamesToIndex {
		if i == 0 {
			continue // we can skip first layer as all the keys are already mapped to 0.
		}
		layerBytes := make([]byte, 4)
		binary.BigEndian.PutUint32(layerBytes, uint32(i))
		count := 0

		dec, err := seg.NewDecompressor(path.Join(dirs.SnapDomain, fileName))
		if err != nil {
			return fmt.Errorf("failed to create decompressor: %w", err)
		}
		defer dec.Close()
		getter := dec.MakeGetter()
		fmt.Printf("Indexing file %s\n", fileName)
		var buf []byte
		for getter.HasNext() {
			buf, _ = getter.Next(buf[:0])

			if err := collector.Collect(buf, layerBytes); err != nil {
				return err
			}
			count++
			//fmt.Println("count: ", count, "keyLength: ", len(buf))
			if count%1_000_000 == 0 {
				fmt.Printf("Indexed %d keys in file %s\n", count, fileName)
			}
			// skip values
			getter.Skip()
		}
		fmt.Printf("Indexed %d keys in file %s\n", count, fileName)
	}
	fmt.Println("Loading the keys to DB")
	if err := collector.Load(tx, tbl, etl.IdentityLoadFunc, etl.TransformArgs{}); err != nil {
		return fmt.Errorf("failed to load: %w", err)
	}

	return tx.Commit()
}

func makePurifiedDomains(db kv.RwDB, dirs datadir.Dirs, logger log.Logger, domain kv.Domain) error {
	compressionType := statelib.Schema.GetDomainCfg(domain).Compression
	compressCfg := statelib.Schema.GetDomainCfg(domain).CompressCfg
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
		return fmt.Errorf("invalid domainName %s", domain.String())
	}
	// Iterate over all the files in  dirs.SnapDomain and print them
	filesNamesToPurify := []string{}
	if err := filepath.Walk(dirs.SnapDomain, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Skip directories
		if info.IsDir() {
			return nil
		}
		if !strings.Contains(info.Name(), domain.String()) {
			return nil
		}
		// Here you can decide if you only want to process certain file extensions
		// e.g., .kv files
		if filepath.Ext(path) != ".kv" {
			// Skip non-kv files if that's your domainName’s format
			return nil
		}

		fmt.Printf("Add file to purification of %s: %s\n", domain.String(), path)

		res, ok, _ := downloadertype.ParseFileName("", info.Name())
		if !ok {
			panic("invalid file name")
		}
		if res.From < fromStepPurification || res.To > toStepPurification {
			return nil
		}

		filesNamesToPurify = append(filesNamesToPurify, info.Name())
		return nil
	}); err != nil {
		return fmt.Errorf("failed to walk through the domainDir %s: %w", domain.String(), err)
	}
	// sort the files by name
	sort.Slice(filesNamesToPurify, func(i, j int) bool {
		res, ok, _ := downloadertype.ParseFileName(dirs.SnapDomain, filesNamesToPurify[i])
		if !ok {
			panic("invalid file name")
		}
		res2, ok, _ := downloadertype.ParseFileName(dirs.SnapDomain, filesNamesToPurify[j])
		if !ok {
			panic("invalid file name")
		}
		return res.From < res2.From
	})

	tx, err := db.BeginRo(context.Background())
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback()
	outD := datadir.New(outDatadir)

	// now start the file indexing
	for currentLayer, fileName := range filesNamesToPurify {
		count := 0
		skipped := 0

		dec, err := seg.NewDecompressor(filepath.Join(dirs.SnapDomain, fileName))
		if err != nil {
			return fmt.Errorf("failed to create decompressor: %w", err)
		}
		defer dec.Close()
		getter := dec.MakeGetter()

		valuesComp, err := seg.NewCompressor(context.Background(), "Purification", filepath.Join(outD.SnapDomain, fileName), dirs.Tmp, compressCfg, log.LvlTrace, log.New())
		if err != nil {
			return fmt.Errorf("create %s values compressor: %w", filepath.Join(outD.SnapDomain, fileName), err)
		}
		defer valuesComp.Close()

		comp := seg.NewWriter(valuesComp, compressionType)
		defer comp.Close()

		fmt.Printf("Indexing file %s\n", fileName)
		var k, v []byte

		var layer uint32
		for getter.HasNext() {
			// get the key and value for the current entry
			k, _ = getter.Next(k[:0])
			v, _ = getter.Next(v[:0])

			layerBytes, err := tx.GetOne(tbl, k)
			if err != nil {
				return fmt.Errorf("failed to get key %x: %w", k, err)
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
				return fmt.Errorf("failed to add key %x: %w", k, err)
			}
			if _, err := comp.Write(v); err != nil {
				return fmt.Errorf("failed to add val %x: %w", v, err)
			}
			count++
			if count%10_000_000 == 0 {
				skipRatio := float64(skipped) / float64(count)
				fmt.Printf("Indexed %d keys, skipped %d, in file %s. skip ratio: %.2f\n", count, skipped, fileName, skipRatio)
			}
		}

		skipRatio := float64(skipped) / float64(count)
		if skipRatio < minSkipRatioL0 && currentLayer == 0 {
			fmt.Printf("Skip ratio %.2f is less than min-skip-ratio-l0 %.2f, skipping domain %s\n", skipRatio, minSkipRatioL0, domain)
			return nil
		}
		if skipRatio < minSkipRatio {
			fmt.Printf("Skip ratio %.2f is less than min-skip-ratio %.2f, skipping %s\n", skipRatio, minSkipRatioL0, fileName)
			continue
		}
		fmt.Printf("Loaded %d keys in file %s. now compressing...\n", count, fileName)
		if err := comp.Compress(); err != nil {
			return fmt.Errorf("failed to compress: %w", err)
		}
		fmt.Printf("Compressed %d keys in file %s\n", count, fileName)
		comp.Close()
		if replaceInDatadir {
			fmt.Printf("Replacing the file %s in datadir\n", fileName)
			if err := os.Rename(filepath.Join(outD.SnapDomain, fileName), filepath.Join(dirs.SnapDomain, fileName)); err != nil {
				return fmt.Errorf("failed to replace the file %s: %w", fileName, err)
			}
			kveiFile := strings.ReplaceAll(fileName, ".kv", ".kvei")
			btFile := strings.ReplaceAll(fileName, ".kv", ".bt")
			kviFile := strings.ReplaceAll(fileName, ".kv", ".kvi")
			removeManyIgnoreError(
				filepath.Join(dirs.SnapDomain, fileName+".torrent"),
				filepath.Join(dirs.SnapDomain, btFile),
				filepath.Join(dirs.SnapDomain, btFile+".torrent"),
				filepath.Join(dirs.SnapDomain, kveiFile),
				filepath.Join(dirs.SnapDomain, kveiFile+".torrent"),
				filepath.Join(dirs.SnapDomain, kviFile),
				filepath.Join(dirs.SnapDomain, kviFile+".torrent"),
			)
			fmt.Printf("Removed the files %s and %s\n", kveiFile, btFile)
		}
	}
	return nil
}

func requestDomains(chainDb, stateDb kv.RwDB, ctx context.Context, readDomain string, addrs [][]byte, logger log.Logger) error {
	stateTx, err := stateDb.BeginRw(ctx)
	must(err)
	defer stateTx.Rollback()
	temporalTx, ok := stateTx.(kv.TemporalTx)
	if !ok {
		return errors.New("stateDb transaction is not a temporal transaction")
	}
	domains, err := state3.NewSharedDomains(temporalTx, logger)
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
	case "account":
		for _, addr := range addrs {

			acc, err := r.ReadAccountData(common.BytesToAddress(addr))
			if err != nil {
				logger.Error("failed to read account", "addr", addr, "err", err)
				continue
			}
			fmt.Printf("%x: nonce=%d balance=%d code=%x root=%x\n", addr, acc.Nonce, acc.Balance.Uint64(), acc.CodeHash, acc.Root)
		}
	case "storage":
		for _, addr := range addrs {
			a, s := common.BytesToAddress(addr[:length.Addr]), common.BytesToHash(addr[length.Addr:])
			st, ok, err := r.ReadAccountStorage(a, s)
			if err != nil {
				logger.Error("failed to read storage", "addr", a.String(), "key", s.String(), "err", err)
				continue
			}
			if !ok {
				logger.Error("storage not found", "addr", a.String(), "key", s.String())
				continue
			}
			fmt.Printf("%s %s -> %x\n", a.String(), s.String(), st)
		}
	case "code":
		for _, addr := range addrs {
			code, err := r.ReadAccountCode(common.BytesToAddress(addr))
			if err != nil {
				logger.Error("failed to read code", "addr", addr, "err", err)
				continue
			}
			fmt.Printf("%s: %x\n", addr, code)
		}
	}
	return nil
}

func removeMany(filePaths ...string) error {
	for _, filePath := range filePaths {
		if err := os.Remove(filePath); err != nil {
			_, fileName := filepath.Split(filePath)
			return fmt.Errorf("failed to remove the file: %s, %w", fileName, err)
		}
	}
	return nil
}

func removeManyIgnoreError(filePaths ...string) {
	for _, filePath := range filePaths {
		os.Remove(filePath)
	}
}
