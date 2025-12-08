package commands

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/recsplit/multiencseq"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/turbo/debug"
)

// TODO: this utility can be safely deleted after PR https://github.com/erigontech/erigon/pull/12907/ is rolled out in production
var idxVerify = &cobra.Command{
	Use:   "idx_verify",
	Short: "After a genesis sync + snapshot regen, deep compare original and optimized .ef files of 2 E3 instances",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, _ := common.RootContext()
		logger := debug.SetupCobra(cmd, "integration")
		dirs := datadir.New(sourceDirCli)

		if err := CheckSaltFilesExist(dirs); err != nil {
			logger.Error("Failed to check salt files", "error", err)
			return
		}

		sourceIdxDir := os.DirFS(dirs.SnapIdx)

		files, err := fs.ReadDir(sourceIdxDir, ".")
		if err != nil {
			logger.Error("Failed to read directory contents", "error", err)
			return
		}

		logger.Info("Comparing idx files:")
	F:
		for _, file := range files {
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".ef") {
				continue
			}

			efInfo, err := parseEFFilename(file.Name())
			if err != nil {
				logger.Error("Failed to parse file info", "error", err)
				return
			}
			baseTxNum := efInfo.startStep * config3.DefaultStepSize
			targetEFFileName := strings.Replace(file.Name(), "v1.0-", "v2.0-", 1)
			targetEFIFileName := strings.Replace(file.Name(), "v1.0-", "v1.1-", 1)

			targetIndexFilename := targetDirCli + "/snapshots/accessor/" + targetEFIFileName + "i"
			if manuallyOptimized {
				targetIndexFilename = targetDirCli + "/snapshots/accessor/" + targetEFIFileName + "i.new"
			}
			targetEfi, err := recsplit.OpenIndex(targetIndexFilename)
			if err != nil {
				logger.Error("Failed to open index", "error", err)
				return
			}
			defer targetEfi.Close()

			targetEfiReader := targetEfi.GetReaderFromPool()
			defer targetEfiReader.Close()

			// original .ef file
			sourceFilename := sourceDirCli + "/snapshots/idx/" + file.Name()
			sourceIdx, err := seg.NewDecompressor(sourceFilename)
			if err != nil {
				logger.Error("Failed to open decompressor", "error", err)
				return
			}
			defer sourceIdx.Close()

			// reencoded optimized .ef file
			targetFilename := targetDirCli + "/snapshots/idx/" + targetEFFileName
			if manuallyOptimized {
				targetFilename = targetDirCli + "/snapshots/idx/" + targetEFFileName + ".new"
			}
			targetIdx, err := seg.NewDecompressor(targetFilename)
			if err != nil {
				logger.Error("Failed to open decompressor", "error", err)
				return
			}
			defer targetIdx.Close()

			logger.Info("Deep checking files...", "source", sourceFilename, "target", targetFilename, "targetIndex", targetIndexFilename)

			g := sourceIdx.MakeGetter()
			sourceReader := seg.NewReader(g, seg.CompressNone)
			sourceReader.Reset(0)

			g = targetIdx.MakeGetter()
			targetReader := seg.NewReader(g, seg.CompressNone)
			targetReader.Reset(0)

			prevKeyOffset := uint64(0)
			shouldSkipEfiCheck := false
			for sourceReader.HasNext() {
				if !targetReader.HasNext() {
					logger.Warn("target reader doesn't have next!")
					logger.Info("skipping to next file...")
					continue F
				}

				sourceK, _ := sourceReader.Next(nil)
				targetK, _ := targetReader.Next(nil)
				if !bytes.Equal(sourceK, targetK) {
					logger.Warn("key mismatch!")
					logger.Info("skipping to next file...")
					continue F
				}

				if !sourceReader.HasNext() {
					logger.Warn("source reader doesn't have next!")
					logger.Info("skipping to next file...")
					continue F
				}
				if !targetReader.HasNext() {
					logger.Warn("target reader doesn't have next!")
					logger.Info("skipping to next file...")
					continue F
				}

				// source/target semantic value comparison
				sourceV, _ := sourceReader.Next(nil)
				targetV, nextKeyOffset := targetReader.Next(nil)
				if !compareSequences(logger, sourceK, sourceV, targetV, baseTxNum) {
					logger.Warn("value mismatch!")
					logger.Info("skipping to next key...")
					logger.Info("skipping .efi offset checks for this file...")
					shouldSkipEfiCheck = true
					continue // next KEY, not FILE
				}

				// checks new efi lookup points to the same value
				if !shouldSkipEfiCheck {
					offset, found := targetEfiReader.TwoLayerLookup(targetK)
					if !found {
						logger.Warn("key not found in efi", "k", hexutil.Encode(targetK))
						logger.Info("skipping to next file...")
						continue F
					}
					if offset != prevKeyOffset {
						logger.Warn("offset mismatch", "offset", offset, "prevKeyOffset", prevKeyOffset)
						logger.Info("skipping to next file...")
						continue F
					}
					prevKeyOffset = nextKeyOffset
				}

				select {
				case <-ctx.Done():
					return
				default:
				}
			}
			sourceIdx.Close()
			targetIdx.Close()
			targetEfiReader.Close()
			targetEfi.Close()
		}
	},
}

func compareSequences(logger log.Logger, sourceK, sourceV, targetV []byte, baseTxNum uint64) bool {
	// log.Printf("k=%s sv=%s tv=%s baseTxNum=%d", hexutility.Encode(sourceK), hexutility.Encode(sourceV), hexutility.Encode(targetV), baseTxNum)
	sourceEf, _ := eliasfano32.ReadEliasFano(sourceV)
	targetSeq := multiencseq.ReadMultiEncSeq(baseTxNum, targetV)

	if targetSeq.EncodingType() == multiencseq.PlainEliasFano {
		logger.Warn("target encoding type can't be PlainEliasFano")
		return false
	}
	// if targetSeq.Count() > sourceEf.Count() {
	// 	log.Print("Optimized eliasfano is longer")
	// 	log.Printf("key=%s", hexutil.Encode(sourceK))
	// 	log.Printf("source min=%d max=%d count=%d", sourceEf.Min(), sourceEf.Max(), sourceEf.Count())
	// 	log.Printf("target min=%d max=%d count=%d", targetSeq.Min(), targetSeq.Max(), targetSeq.Count())
	// 	// return false
	// }
	// if sourceEf.Count() > targetSeq.Count() {
	// 	log.Print("Optimized eliasfano is shorter")
	// 	log.Printf("key=%s", hexutil.Encode(sourceK))
	// 	log.Printf("source min=%d max=%d count=%d", sourceEf.Min(), sourceEf.Max(), sourceEf.Count())
	// 	log.Printf("target min=%d max=%d count=%d", targetSeq.Min(), targetSeq.Max(), targetSeq.Count())
	// 	// return false
	// }

	sourceIt := sourceEf.Iterator()
	targetIt := targetSeq.Iterator(0)
	var sourceN, targetN uint64
	var err error
	diff := false
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

		if !diff {
			logger.Info("!diff", "key", hexutil.Encode(sourceK))
			logger.Info("source", "min", sourceEf.Min(), "max", sourceEf.Max(), "count", sourceEf.Count())
			logger.Info("target", "min", targetSeq.Min(), "max", targetSeq.Max(), "count", targetSeq.Count())
		}
		diff = true
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

		// if sourceN != targetN {
		// 	log.Printf("values mismatch: source=%d target=%d", sourceN, targetN)
		// 	log.Printf("key=%s", hexutil.Encode(sourceK))
		// 	log.Printf("source min=%d max=%d count=%d", sourceEf.Min(), sourceEf.Max(), sourceEf.Count())
		// 	log.Printf("target min=%d max=%d count=%d", targetSeq.Min(), targetSeq.Max(), targetSeq.Count())
		// 	return false
		// }
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

	return !diff
}

func init() {
	idxVerify.Flags().StringVar(&sourceDirCli, "sourcedir", "", "data directory of original E3 instance")
	must(idxVerify.MarkFlagRequired("sourcedir"))
	must(idxVerify.MarkFlagDirname("sourcedir"))

	idxVerify.Flags().StringVar(&targetDirCli, "targetdir", "", "data directory of optimized E3 instance")
	must(idxVerify.MarkFlagRequired("targetdir"))
	must(idxVerify.MarkFlagDirname("targetdir"))

	idxVerify.Flags().BoolVar(&manuallyOptimized, "manuallyOptimized", false, "set this parameter if you have manually optimized the .ef files ith idx_optimize; set sourcedir/targetdir to the same")

	rootCmd.AddCommand(idxVerify)
}

var sourceDirCli, targetDirCli string
var manuallyOptimized bool
