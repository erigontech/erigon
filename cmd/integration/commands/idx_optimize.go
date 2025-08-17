package commands

import (
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/turbo/debug"
)

// TODO: this utility can be safely deleted after PR https://github.com/erigontech/erigon/pull/12907/ is rolled out in production
func parseEFFilename(fileName string) (*efFileInfo, error) {
	partsByDot := strings.Split(fileName, ".")
	partsByDash := strings.Split(fileName, "-")
	stepParts := strings.Split(partsByDot[1], "-")
	startStep, err := strconv.ParseUint(stepParts[0], 10, 64)
	if err != nil {
		return nil, err
	}
	endStep, err := strconv.ParseUint(stepParts[1], 10, 64)
	if err != nil {
		return nil, err
	}

	return &efFileInfo{
		prefix:    partsByDash[0],
		stepSize:  endStep - startStep,
		startStep: startStep,
		endStep:   endStep,
	}, nil
}

type efFileInfo struct {
	prefix    string
	stepSize  uint64
	startStep uint64
	endStep   uint64
}

var b []byte

func doConvert(baseTxNum uint64, v []byte) ([]byte, error) {
	ef, _ := eliasfano32.ReadEliasFano(v)

	seqBuilder := multiencseq.NewBuilder(baseTxNum, ef.Count(), ef.Max())
	for it := ef.Iterator(); it.HasNext(); {
		n, err := it.Next()
		if err != nil {
			return nil, err
		}
		seqBuilder.AddOffset(n)
	}
	seqBuilder.Build()

	b = seqBuilder.AppendBytes(b[:0])
	return b, nil
}

var idxOptimize = &cobra.Command{
	Use:   "idx_optimize",
	Short: "Scan .ef files, backup them up, reencode and optimize them, rebuild .efi files",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, _ := common.RootContext()
		logger := debug.SetupCobra(cmd, "integration")
		dirs := datadir.New(datadirCli)

		if err := CheckSaltFilesExist(dirs); err != nil {
			logger.Error("Failed to check salt files", "error", err)
			return
		}

		// accessorDir := filepath.Join(datadirCli, "snapshots", "accessor")
		idxPath := dirs.SnapIdx
		idxDir := os.DirFS(idxPath)

		files, err := fs.ReadDir(idxDir, ".")
		if err != nil {
			logger.Error("Failed to read directory contents", "error", err)
			return
		}

		logger.Info("Sumarizing idx files...")
		cEF := 0
		for _, file := range files {
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".ef") {
				continue
			}
			cEF++
		}

		logger.Info("Optimizing idx files...")
		cOpt := 0
		for _, file := range files {
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".ef") {
				continue
			}

			efInfo, err := parseEFFilename(file.Name())
			if err != nil {
				logger.Error("Failed to parse file info: ", err)
			}
			logger.Info("Optimizing...", "file", file.Name(), "n", cOpt, "total", cEF)

			cOpt++
			baseTxNum := efInfo.startStep * config3.DefaultStepSize

			tmpDir := dirs.Tmp

			idxInput, err := seg.NewDecompressor(dirs.SnapIdx + file.Name())
			if err != nil {
				logger.Error("Failed to open decompressor", "error", err)
				return
			}
			defer idxInput.Close()

			idxOutput, err := seg.NewCompressor(ctx, "optimizoor", dirs.SnapIdx+file.Name()+".new", tmpDir, seg.DefaultCfg, log.LvlInfo, logger)
			if err != nil {
				logger.Error("Failed to open compressor", "error", err)
				return
			}
			defer idxOutput.Close()

			// Summarize 1 idx file
			g := idxInput.MakeGetter()
			reader := seg.NewReader(g, seg.CompressNone)
			reader.Reset(0)

			writer := seg.NewWriter(idxOutput, seg.CompressNone)
			ps := background.NewProgressSet()

			for reader.HasNext() {
				k, _ := reader.Next(nil)
				if !reader.HasNext() {
					logger.Error("reader doesn't have next!")
					return
				}
				if err := writer.AddWord(k); err != nil {
					logger.Error("error while writing key", "error", err)
				}

				v, _ := reader.Next(nil)
				v, err := doConvert(baseTxNum, v)
				if err != nil {
					logger.Error("error while optimizing value", "error", err)
					return
				}
				if err := writer.AddWord(v); err != nil {
					logger.Error("error while writing value", "error", err)
					return
				}

				select {
				case <-ctx.Done():
					return
				default:
				}
			}
			if err := writer.Compress(); err != nil {
				logger.Error("error while writing optimized file", "error", err)
				return
			}
			idxInput.Close()
			writer.Close()
			idxOutput.Close()

			// rebuid .efi; COPIED FROM InvertedIndex.buildMapAccessor
			salt, err := state.GetStateIndicesSalt(dirs, false, logger)
			if err != nil {
				logger.Error("Failed to build accessor", "error", err)
				return
			}
			idxPath := dirs.SnapAccessors + file.Name() + "i.new"
			cfg := recsplit.RecSplitArgs{
				Version:            1,
				Enums:              true,
				LessFalsePositives: true,

				BucketSize: recsplit.DefaultBucketSize,
				LeafSize:   recsplit.DefaultLeafSize,
				TmpDir:     tmpDir,
				IndexFile:  idxPath,
				Salt:       salt,
				NoFsync:    false,
			}
			data, err := seg.NewDecompressor(dirs.SnapIdx + file.Name() + ".new")
			if err != nil {
				logger.Error("Failed to build accessor", "error", err)
				return
			}
			if err := state.BuildHashMapAccessor(ctx, seg.NewReader(data.MakeGetter(), seg.CompressNone), idxPath, false, cfg, ps, logger); err != nil {
				logger.Error("Failed to build accessor", "error", err)
				return
			}
		}

		logger.Info(fmt.Sprintf("Optimized %d of %d files!!!", cOpt, cEF))
	},
}

func init() {
	withDataDir(idxOptimize)
	rootCmd.AddCommand(idxOptimize)
}
