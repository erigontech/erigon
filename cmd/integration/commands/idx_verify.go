package commands

import (
	"bytes"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
	"github.com/erigontech/erigon-lib/recsplit/multiencseq"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/spf13/cobra"
)

// TODO: this utility can be safely deleted after PR https://github.com/erigontech/erigon/pull/12907/ is rolled out in production
var idxVerify = &cobra.Command{
	Use:   "idx_verify",
	Short: "After a genesis sync + snapshot regen, deep compare original and optimized .ef files of 2 E3 instances",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, _ := common.RootContext()

		sourceIdxPath := filepath.Join(sourceDirCli, "snapshots", "idx")
		sourceIdxDir := os.DirFS(sourceIdxPath)

		files, err := fs.ReadDir(sourceIdxDir, ".")
		if err != nil {
			log.Fatalf("Failed to read directory contents: %v", err)
		}

		log.Println("Comparing idx files:")
	F:
		for _, file := range files {
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".ef") {
				continue
			}

			efInfo, err := parseEFFilename(file.Name())
			if err != nil {
				log.Fatalf("Failed to parse file info: %v", err)
			}
			baseTxNum := efInfo.startStep * config3.DefaultStepSize

			targetIndexFilename := targetDirCli + "/snapshots/accessor/" + file.Name() + "i"
			if manuallyOptimized {
				targetIndexFilename = targetDirCli + "/snapshots/accessor/" + file.Name() + "i.new"
			}
			targetEfi, err := recsplit.OpenIndex(targetIndexFilename)
			if err != nil {
				log.Fatalf("Failed to open index: %v", err)
			}
			defer targetEfi.Close()

			targetEfiReader := targetEfi.GetReaderFromPool()
			defer targetEfiReader.Close()

			// original .ef file
			sourceFilename := sourceDirCli + "/snapshots/idx/" + file.Name()
			sourceIdx, err := seg.NewDecompressor(sourceFilename)
			if err != nil {
				log.Fatalf("Failed to open decompressor: %v", err)
			}
			defer sourceIdx.Close()

			// reencoded optimized .ef file
			targetFilename := targetDirCli + "/snapshots/idx/" + file.Name()
			if manuallyOptimized {
				targetFilename = targetDirCli + "/snapshots/idx/" + file.Name() + ".new"
			}
			targetIdx, err := seg.NewDecompressor(targetFilename)
			if err != nil {
				log.Fatalf("Failed to open decompressor: %v", err)
			}
			defer targetIdx.Close()

			log.Printf("Deep checking files %s -> %s, %s...", sourceFilename, targetFilename, targetIndexFilename)

			g := sourceIdx.MakeGetter()
			sourceReader := seg.NewReader(g, seg.CompressNone)
			sourceReader.Reset(0)

			g = targetIdx.MakeGetter()
			targetReader := seg.NewReader(g, seg.CompressNone)
			targetReader.Reset(0)

			prevKeyOffset := uint64(0)
			for sourceReader.HasNext() {
				if !targetReader.HasNext() {
					log.Printf("target reader doesn't have next!")
					log.Println("skipping to next file...")
					continue F
				}

				sourceK, _ := sourceReader.Next(nil)
				targetK, _ := targetReader.Next(nil)
				if !bytes.Equal(sourceK, targetK) {
					log.Printf("key mismatch!")
					log.Println("skipping to next file...")
					continue F
				}

				if !sourceReader.HasNext() {
					log.Println("source reader doesn't have next!")
					log.Println("skipping to next file...")
					continue F
				}
				if !targetReader.HasNext() {
					log.Println("target reader doesn't have next!")
					log.Println("skipping to next file...")
					continue F
				}

				// source/target semantic value comparison
				sourceV, _ := sourceReader.Next(nil)
				targetV, nextKeyOffset := targetReader.Next(nil)
				if !compareSequences(sourceK, sourceV, targetV, baseTxNum) {
					log.Println("value mismatch!")
					log.Println("skipping to next file...")
					continue F
				}

				// checks new efi lookup points to the same value
				offset, found := targetEfiReader.TwoLayerLookup(targetK)
				if !found {
					log.Printf("key %v not found in efi", hexutil.Encode(targetK))
					log.Println("skipping to next file...")
					continue F
				}
				if offset != prevKeyOffset {
					log.Printf("offset mismatch: %d != %d", offset, prevKeyOffset)
					log.Println("skipping to next file...")
					continue F
				}
				prevKeyOffset = nextKeyOffset

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

func compareSequences(sourceK, sourceV, targetV []byte, baseTxNum uint64) bool {
	// log.Printf("k=%s sv=%s tv=%s baseTxNum=%d", hexutility.Encode(sourceK), hexutility.Encode(sourceV), hexutility.Encode(targetV), baseTxNum)
	sourceEf, _ := eliasfano32.ReadEliasFano(sourceV)
	targetSeq := multiencseq.ReadMultiEncSeq(baseTxNum, targetV)

	if targetSeq.EncodingType() == multiencseq.PlainEliasFano {
		log.Printf("target encoding type can't be PlainEliasFano")
		return false
	}
	if targetSeq.Count() > sourceEf.Count() {
		log.Print("Optimized eliasfano is longer")
		log.Printf("key=%s", hexutil.Encode(sourceK))
		log.Printf("source min=%d max=%d count=%d", sourceEf.Min(), sourceEf.Max(), sourceEf.Count())
		log.Printf("target min=%d max=%d count=%d", targetSeq.Min(), targetSeq.Max(), targetSeq.Count())
		return false
	}
	if sourceEf.Count() > targetSeq.Count() {
		log.Print("Optimized eliasfano is shorter")
		log.Printf("key=%s", hexutil.Encode(sourceK))
		log.Printf("source min=%d max=%d count=%d", sourceEf.Min(), sourceEf.Max(), sourceEf.Count())
		log.Printf("target min=%d max=%d count=%d", targetSeq.Min(), targetSeq.Max(), targetSeq.Count())
		return false
	}

	sourceIt := sourceEf.Iterator()
	targetIt := targetSeq.Iterator(0)
	for sourceIt.HasNext() {
		sourceN, err := sourceIt.Next()
		if err != nil {
			log.Fatalf("Failed to read next: %v", err)
		}
		targetN, err := targetIt.Next()
		if err != nil {
			log.Fatalf("Failed to read next: %v", err)
		}
		if sourceN != targetN {
			log.Printf("values mismatch: source=%d target=%d", sourceN, targetN)
			log.Printf("key=%s", hexutil.Encode(sourceK))
			log.Printf("source min=%d max=%d count=%d", sourceEf.Min(), sourceEf.Max(), sourceEf.Count())
			log.Printf("target min=%d max=%d count=%d", targetSeq.Min(), targetSeq.Max(), targetSeq.Count())
			return false
		}
	}

	return true
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
