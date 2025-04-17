package commands

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/spf13/cobra"
)

var traceCompare = &cobra.Command{
	Use:   "trace_compare",
	Short: "Deep compare .ef files of 2 E3 instances",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, _ := common.RootContext()
		logger := debug.SetupCobra(cmd, "integration")
		sourceIdxPath := filepath.Join(sourceDirCli, "snapshots", "idx")
		sourceIdxDir := os.DirFS(sourceIdxPath)

		files, err := fs.ReadDir(sourceIdxDir, ".")
		if err != nil {
			logger.Error("Failed to read directory contents", "error", err)
			return
		}

		logger.Info("Comparing idx files:")
		for _, file := range files {
			if file.IsDir() || !strings.HasPrefix(file.Name(), "v1-traces") || !strings.HasSuffix(file.Name(), ".ef") {
				continue
			}

			compareFiles(ctx, logger, file.Name())
		}
	},
}

func compareFiles(ctx context.Context, logger log.Logger, filename string) {
	// original .ef file
	sourceFilename := sourceDirCli + "/snapshots/idx/" + filename
	sourceIdx, err := seg.NewDecompressor(sourceFilename)
	if err != nil {
		logger.Error("Failed to open decompressor", "error", err)
		return
	}
	defer sourceIdx.Close()

	// target .ef file
	targetFilename := targetDirCli + "/snapshots/idx/" + filename
	targetIdx, err := seg.NewDecompressor(targetFilename)
	if err != nil {
		logger.Error("Failed to open decompressor", "error", err)
		return
	}
	defer targetIdx.Close()

	logger.Info("Deep checking files...", "source", sourceFilename, "target", targetFilename)

	sourceReader := seg.NewReader(sourceIdx.MakeGetter(), seg.CompressNone)
	sourceReader.Reset(0)

	targetReader := seg.NewReader(targetIdx.MakeGetter(), seg.CompressNone)
	targetReader.Reset(0)

	for sourceReader.HasNext() {
		if !targetReader.HasNext() {
			logger.Warn("target reader doesn't have next!")
			logger.Info("skipping to next file...")
			return
		}

		sourceK, _ := sourceReader.Next(nil)
		targetK, _ := targetReader.Next(nil)
		if !bytes.Equal(sourceK, targetK) {
			// keys don't match; there is a misalignment of keys between source/target files;
			// logs diffs and advance until keys match again or EOF
			comp := bytes.Compare(sourceK, targetK)
			if comp < 0 {
				logger.Info("diff", "sourceKey", hexutil.Encode(sourceK))
			} else {
				logger.Info("diff", "targetKey", hexutil.Encode(targetK))
			}
		G:
			for comp != 0 {
				for comp < 0 && sourceReader.HasNext() {
					v, _ := sourceReader.Next(nil) // V
					ef, _ := eliasfano32.ReadEliasFano(v)
					logger.Info("source", "min", ef.Min(), "max", ef.Max(), "count", ef.Count())
					for it := ef.Iterator(); it.HasNext(); {
						n, err := it.Next()
						if err != nil {
							logger.Error("Failed to iterate", "error", err)
							return
						}
						logger.Info(fmt.Sprintf("> %d", n))
					}

					if !sourceReader.HasNext() {
						logger.Warn("source reader doesn't have next!")
						logger.Info("skipping to next file...")
						return
					}

					sourceK, _ = sourceReader.Next(nil)
					comp = bytes.Compare(sourceK, targetK)
					if comp == 0 {
						break G
					}
					if !sourceReader.HasNext() {
						logger.Info("skipping to next file...")
						return
					}
					if comp < 0 {
						logger.Info(">", "sourceKey", hexutil.Encode(sourceK))
					} else {
						logger.Info("<", "targetKey", hexutil.Encode(targetK))
					}
				}
				for comp > 0 && targetReader.HasNext() {
					v, _ := targetReader.Next(nil) // V
					ef, _ := eliasfano32.ReadEliasFano(v)
					logger.Info("target", "min", ef.Min(), "max", ef.Max(), "count", ef.Count())
					for it := ef.Iterator(); it.HasNext(); {
						n, err := it.Next()
						if err != nil {
							logger.Error("Failed to iterate", "error", err)
							return
						}
						logger.Info(fmt.Sprintf("< %d", n))
					}

					if !targetReader.HasNext() {
						logger.Warn("target reader doesn't have next!")
						logger.Info("skipping to next file...")
						return
					}

					targetK, _ = targetReader.Next(nil)
					comp = bytes.Compare(sourceK, targetK)
					if comp == 0 {
						break G
					}
					if !targetReader.HasNext() {
						logger.Info("skipping to next file...")
						return
					}
					if comp < 0 {
						logger.Info("diff", "sourceKey", hexutil.Encode(sourceK))
					} else {
						logger.Info("diff", "targetKey", hexutil.Encode(targetK))
					}
				}
			}

			if !sourceReader.HasNext() || !targetReader.HasNext() {
				return
			}
		}

		// keys match, compare values
		if !sourceReader.HasNext() {
			logger.Warn("source reader doesn't have next!")
			logger.Info("skipping to next file...")
			return
		}
		if !targetReader.HasNext() {
			logger.Warn("target reader doesn't have next!")
			logger.Info("skipping to next file...")
			return
		}

		// source/target semantic value comparison
		sourceV, _ := sourceReader.Next(nil)
		targetV, _ := targetReader.Next(nil)
		if !compareSequences(logger, sourceK, sourceV, targetV) {
			continue // next KEY, not FILE
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

func compareSequences(logger log.Logger, sourceK, sourceV, targetV []byte) bool {
	sourceEf, _ := eliasfano32.ReadEliasFano(sourceV)
	targetEf, _ := eliasfano32.ReadEliasFano(targetV)

	sourceIt := sourceEf.Iterator()
	targetIt := targetEf.Iterator()
	var sourceN, targetN uint64
	var err error
	loggedDiff := false

	for sourceIt.HasNext() && targetIt.HasNext() {
		sourceN, err = sourceIt.Next()
		if err != nil {
			logger.Error("Failed to read next", "error", err)
			return false
		}
		targetN, err = targetIt.Next()
		if err != nil {
			logger.Error("Failed to read next", "error", err)
			return false
		}

		if sourceN == targetN {
			continue
		}

		if !loggedDiff {
			logger.Info("diff", "key", hexutil.Encode(sourceK))
			logger.Info("source", "min", sourceEf.Min(), "max", sourceEf.Max(), "count", sourceEf.Count())
			logger.Info("target", "min", targetEf.Min(), "max", targetEf.Max(), "count", targetEf.Count())
		}
		loggedDiff = true
		for sourceN != targetN && sourceIt.HasNext() && targetIt.HasNext() {
			if sourceN < targetN {
				for sourceN < targetN {
					logger.Info(fmt.Sprintf("> %d", sourceN))
					if !sourceIt.HasNext() {
						break
					}

					sourceN, err = sourceIt.Next()
					if err != nil {
						logger.Error("Failed to read next", "error", err)
						return false
					}
				}
			} else {
				for sourceN > targetN {
					logger.Info(fmt.Sprintf("< %d", targetN))
					if !targetIt.HasNext() {
						break
					}

					targetN, err = targetIt.Next()
					if err != nil {
						logger.Error("Failed to read next", "error", err)
						return false
					}
				}
			}
		}
	}

	if (sourceIt.HasNext() || targetIt.HasNext()) && !loggedDiff {
		logger.Info("diff", "key", hexutil.Encode(sourceK), "delta", targetEf.Count()-sourceEf.Count())
		logger.Info("source", "min", sourceEf.Min(), "max", sourceEf.Max(), "count", sourceEf.Count())
		logger.Info("target", "min", targetEf.Min(), "max", targetEf.Max(), "count", targetEf.Count())
	}

	// Drain remaining elems
	if sourceIt.HasNext() && !targetIt.HasNext() {
		for sourceIt.HasNext() {
			sourceN, err = sourceIt.Next()
			if err != nil {
				logger.Error("Failed to read next", "error", err)
				return false
			}
			logger.Info(fmt.Sprintf("> %d", sourceN))
		}
	}
	if !sourceIt.HasNext() && targetIt.HasNext() {
		for targetIt.HasNext() {
			targetN, err = targetIt.Next()
			if err != nil {
				logger.Error("Failed to read next", "error", err)
				return false
			}
			logger.Info(fmt.Sprintf("< %d", targetN))
		}
	}

	return !loggedDiff
}

func init() {
	traceCompare.Flags().StringVar(&sourceDirCli, "sourcedir", "", "data directory of source E3 instance")
	must(traceCompare.MarkFlagRequired("sourcedir"))
	must(traceCompare.MarkFlagDirname("sourcedir"))

	traceCompare.Flags().StringVar(&targetDirCli, "targetdir", "", "data directory of target E3 instance")
	must(traceCompare.MarkFlagRequired("targetdir"))
	must(traceCompare.MarkFlagDirname("targetdir"))

	rootCmd.AddCommand(traceCompare)
}

var sourceDirCli, targetDirCli string
