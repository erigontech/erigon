package commands

import (
	"bufio"
	"encoding/csv"
	"io/fs"
	"log"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon/turbo/cli"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/spf13/cobra"
)

const SIM_COUNT = 9

var simulations = [SIM_COUNT]simFunc{
	simOpt1,
	simOpt2,
	simOpt3,
	simOpt4,
	simOpt5,
	simOpt6,
	simOpt7,
	simOpt8,
	simOpt9,
}

type simFunc func(v []byte, ef *eliasfano32.EliasFano, baseTxNum uint64) int

type IdxSummaryEntry struct {
	records        uint64
	totalBytesK    int
	totalBytesV    int
	totalOptBytesV [SIM_COUNT]int
	min            uint64
	max            uint64
}

type efFileInfo struct {
	prefix    string
	stepSize  uint64
	startStep uint64
	endStep   uint64
}

func parseEFFilename(fileName string) (*efFileInfo, error) {
	parts := strings.Split(fileName, ".")
	stepParts := strings.Split(parts[1], "-")
	startStep, err := strconv.ParseUint(stepParts[0], 10, 64)
	if err != nil {
		return nil, err
	}
	endStep, err := strconv.ParseUint(stepParts[1], 10, 64)
	if err != nil {
		return nil, err
	}

	return &efFileInfo{
		prefix:    parts[0],
		stepSize:  endStep - startStep,
		startStep: startStep,
		endStep:   endStep,
	}, nil
}

// Naive optimization: for ef sequences < 16 elems; concatenate all
// raw uint64, hence size == count * sizeof(uint64) + 1 byte for encoding type
func simOpt1(v []byte, ef *eliasfano32.EliasFano, _ uint64) int {
	count := ef.Count()
	if count < 16 {
		return int(count)*8 + 1 //  number of txNums * size of uint64 + 1 byte to signal encoding type
	}

	return len(v) // DO NOT OPTIMIZE; plain elias fano
}

// Delta encoding starting from 2nd elem; only for ef sequences < 16 elems
//
// Encode 1st uint64 elem raw; next elems are deltas of the 1st; they can fit into uint32
// because max delta is bounded by 64 * stepSize == 100M
// hence size == sizeof(uint64) + (count - 1) * sizeof(uint32) + 1 byte for encoding type
func simOpt2(v []byte, ef *eliasfano32.EliasFano, _ uint64) int {
	count := ef.Count()
	if count < 16 {
		if ef.Max()-ef.Min()+1 < uint64(0xffffffff) {
			return 8 + int(count-1)*4 + 1
		}
	}

	return len(v) // DO NOT OPTIMIZE; plain elias fano
}

func simOpt3(v []byte, ef *eliasfano32.EliasFano, _ uint64) int {
	count := ef.Count()
	if count < 32 {
		if ef.Max()-ef.Min()+1 < uint64(0xff) {
			return 8 + int(count-1) + 1
		} else if ef.Max()-ef.Min()+1 < uint64(0xffff) {
			return 8 + int(count-1)*2 + 1
		} else if ef.Max()-ef.Min()+1 < uint64(0xffffff) {
			return 8 + int(count-1)*3 + 1
		} else if ef.Max()-ef.Min()+1 < uint64(0xffffffff) {
			return 8 + int(count-1)*4 + 1
		}
	}

	return len(v) // DO NOT OPTIMIZE; plain elias fano
}

// Delta encoding starting from 1st elem; only for ef sequences < 16 elems
//
// Encode all elems as deltas from baseTxId; they can fit into uint32
// because max delta is bounded by 64 * stepSize == 100M
// hence size == count * sizeof(uint32) + 1 byte for encoding type
func simOpt4(v []byte, ef *eliasfano32.EliasFano, _ uint64) int {
	count := ef.Count()
	if count < 16 {
		if ef.Max()-ef.Min()+1 < uint64(0xffffffff) {
			return int(count)*4 + 1
		}
	}

	return len(v) // DO NOT OPTIMIZE; plain elias fano
}

// Same as simOpt4, but applies to sequences up to 99 elems
func simOpt5(v []byte, ef *eliasfano32.EliasFano, _ uint64) int {
	count := ef.Count()
	if count < 100 {
		if ef.Max()-ef.Min()+1 < uint64(0xffffffff) {
			return int(count)*4 + 1
		}
	}

	return len(v) // DO NOT OPTIMIZE; plain elias fano
}

// Same as simOpt4, but:
//
// - Drops 4 bytes from ef.Count() (uint64) (bc bounded by 64 steps; fits in uint32)
// - Drops 4 bytes from ef.Max() (uint64) (bc bounded by 64 steps; fits in uint32)
// - Rebase ef sequence to baseTxNum
func simOpt6(v []byte, ef *eliasfano32.EliasFano, baseTxNum uint64) int {
	count := ef.Count()
	if count < 16 {
		if ef.Max()-ef.Min()+1 < uint64(0xffffffff) {
			return int(count)*4 + 1
		}
	}

	optEf := eliasfano32.NewEliasFano(ef.Count(), ef.Max()-baseTxNum)
	for it := ef.Iterator(); it.HasNext(); {
		n, err := it.Next()
		if err != nil {
			log.Fatalf("Failed to read next: %v", err)
		}
		optEf.AddOffset(n - baseTxNum)
	}
	optEf.Build()
	var b []byte
	b = optEf.AppendBytes(b[:0])

	return len(b) - 8 // rebased ef - 4 bytes (count) - 4 bytes (max)
}

// Same as simOpt6, but:
//
// - NOT: Drops 4 bytes from ef.Count() (uint64) (bc bounded by 64 steps; fits in uint32)
// - NOT: Drops 4 bytes from ef.Max() (uint64) (bc bounded by 64 steps; fits in uint32)
// - Rebase ef sequence to baseTxNum <- THIS
func simOpt7(v []byte, ef *eliasfano32.EliasFano, baseTxNum uint64) int {
	count := ef.Count()
	if count < 16 {
		if ef.Max()-ef.Min()+1 < uint64(0xffffffff) {
			return int(count)*4 + 1
		}
	}

	optEf := eliasfano32.NewEliasFano(ef.Count(), ef.Max()-baseTxNum)
	for it := ef.Iterator(); it.HasNext(); {
		n, err := it.Next()
		if err != nil {
			log.Fatalf("Failed to read next: %v", err)
		}
		optEf.AddOffset(n - baseTxNum)
	}
	optEf.Build()
	var b []byte
	b = optEf.AppendBytes(b[:0])

	return len(b) // rebased ef
}

// reencode everything using roaring bitmaps: ef -> bm
//
// size: 1 byte (encoding type) + len(bm)
func simOpt8(v []byte, ef *eliasfano32.EliasFano, _ uint64) int {
	bm := roaring64.NewBitmap()

	for it := ef.Iterator(); it.HasNext(); {
		n, err := it.Next()
		if err != nil {
			log.Fatalf("Failed to read next: %v", err)
		}
		bm.Add(n)
	}
	bm.RunOptimize()

	return 1 + int(bm.GetSerializedSizeInBytes()) // 1 byte encoding type + len(serialized bm)
}

// rebased roaring bitmaps: same as opt-8, but reduce each value
// by baseTxNum stored once somewhere in the file
//
// size: 1 byte (encoding type) + len(bm)
func simOpt9(v []byte, ef *eliasfano32.EliasFano, baseTxNum uint64) int {
	bm := roaring64.NewBitmap()

	for it := ef.Iterator(); it.HasNext(); {
		n, err := it.Next()
		if err != nil {
			log.Fatalf("Failed to read next: %v", err)
		}
		n -= baseTxNum
		bm.Add(n)
	}
	bm.RunOptimize()

	return 1 + int(bm.GetSerializedSizeInBytes()) // 1 byte encoding type + len(serialized bm)
}

var idxStat = &cobra.Command{
	Use:   "idx_stat",
	Short: "Scan .ef files, generates statistics about their contents and simulates optimizations",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, _ := common.RootContext()
		logger := debug.SetupCobra(cmd, "integration")

		// accessorDir := filepath.Join(datadirCli, "snapshots", "accessor")
		idxPath := filepath.Join(datadirCli, "snapshots", "idx")
		idxDir := os.DirFS(idxPath)

		files, err := fs.ReadDir(idxDir, ".")
		if err != nil {
			log.Fatalf("Failed to read directory contents: %v", err)
		}

		output, err := os.Create(csvOutput)
		if err != nil {
			log.Fatalf("Failed to create output csv: %v", err)
		}
		defer output.Close()
		bufWriter := bufio.NewWriter(output)
		csvWriter := csv.NewWriter(bufWriter)
		headers := []string{
			"filename",
			"type",
			"start step",
			"end step",
			"step size",
			"sequence number of elems",
			"record count",
			"cumulative key size",
			"cumulative value size",
		}
		for i := range SIM_COUNT {
			headers = append(headers, "cumulative value optimized size v"+strconv.Itoa(i+1))
		}
		csvWriter.Write(headers)

		log.Println("Reading idx files:")
		grandTotalRecords := 0
		grandTotalBytesK := 0
		grandTotalBytesV := 0

		var grandTotalOptBytesV [SIM_COUNT]int

		grandTotalFiles := 0
		grandIdxMap := make(map[uint64]*IdxSummaryEntry)
		for _, file := range files {
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".ef") {
				continue
			}

			info, err := file.Info()
			if err != nil {
				logger.Error("Failed to get file info: ", err)
			}
			efInfo, err := parseEFFilename(file.Name())
			if err != nil {
				logger.Error("Failed to parse file info: ", err)
			}
			baseTxNum := efInfo.startStep * config3.DefaultStepSize

			idx, err := seg.NewDecompressor(datadirCli + "/snapshots/idx/" + file.Name())
			if err != nil {
				log.Fatalf("Failed to open decompressor: %v", err)
			}
			defer idx.Close()

			// Summarize 1 idx file
			g := idx.MakeGetter()
			reader := seg.NewReader(g, seg.CompressNone)
			reader.Reset(0)

			idxMap := make(map[uint64]*IdxSummaryEntry)
			for reader.HasNext() {
				k, _ := reader.Next(nil)
				if !reader.HasNext() {
					log.Fatal("reader doesn't have next!")
				}

				v, _ := reader.Next(nil)
				ef, _ := eliasfano32.ReadEliasFano(v)

				select {
				case <-ctx.Done():
					return
				default:
				}

				// group all occurrences >= N
				bucket := ef.Count()
				if bucket >= 1_000_000 {
					bucket = 1_000_000
				} else if bucket >= 100_000 {
					bucket = 100_000
				} else if bucket >= 10_000 {
					bucket = 10_000
				} else if bucket >= 1_000 {
					bucket = 1_000
				} else if bucket >= 100 {
					bucket = 100
				} else if bucket >= 16 {
					bucket = 16
				}

				t, ok := idxMap[bucket]
				if !ok {
					t = &IdxSummaryEntry{
						min: math.MaxInt,
					}
					idxMap[bucket] = t
				}
				t.records++
				t.totalBytesK += len(k)
				t.totalBytesV += len(v)

				for i, sim := range simulations {
					t.totalOptBytesV[i] += sim(v, ef, baseTxNum)
				}
				t.min = min(t.min, ef.Min())
				t.max = max(t.max, ef.Max())
			}
			idx.Close()

			// Print idx file summary
			logger.Info("Summary for", "idx", file.Name())
			totalRecords := 0
			totalBytesK := 0
			totalBytesV := 0
			var totalOptBytesV [SIM_COUNT]int
			keys := make([]uint64, 0, len(idxMap))
			for b, t := range idxMap {
				keys = append(keys, b)

				totalRecords += int(t.records)
				totalBytesK += t.totalBytesK
				totalBytesV += t.totalBytesV
				for i := range totalOptBytesV {
					totalOptBytesV[i] += t.totalOptBytesV[i]
				}

				gt, ok := grandIdxMap[b]
				if !ok {
					gt = &IdxSummaryEntry{
						min: math.MaxInt,
					}
					grandIdxMap[b] = gt
				}
				gt.records += t.records
				gt.totalBytesK += t.totalBytesK
				gt.totalBytesV += t.totalBytesV
				for i := range gt.totalOptBytesV {
					gt.totalOptBytesV[i] += t.totalOptBytesV[i]
				}
			}

			slices.Sort(keys)
			for _, b := range keys {
				t := idxMap[b]
				var effec [SIM_COUNT]string
				for i := range effec {
					effec[i] = "yes"
					if t.totalOptBytesV[i] > t.totalBytesV {
						effec[i] = "NO"
					}
				}

				logger.Info(info.Name(),
					"bucket", b,
					"recs", t.records,
					"k", t.totalBytesK,
					"v", t.totalBytesV,
				)

				data := []string{
					info.Name(),
					efInfo.prefix,
					strconv.FormatUint(efInfo.startStep, 10),
					strconv.FormatUint(efInfo.endStep, 10),
					strconv.FormatUint(efInfo.stepSize, 10),
					strconv.FormatUint(b, 10),
					strconv.FormatUint(t.records, 10),
					strconv.Itoa(t.totalBytesK),
					strconv.Itoa(t.totalBytesV),
				}
				for _, v := range t.totalOptBytesV {
					data = append(data, strconv.Itoa(v))
				}
				csvWriter.Write(data)
				if err := csvWriter.Error(); err != nil {
					log.Fatalf("Error writing csv: %v", err)
				}
			}
			logger.Info("TOTALS",
				"totalBytes", totalBytesK+totalBytesV,
				"totalBytesFile", info.Size(),
				"%keys", 100*float64(totalBytesK)/float64(totalBytesK+totalBytesV),
				"%values", 100*float64(totalBytesV)/float64(totalBytesK+totalBytesV),
			)

			grandTotalRecords += totalRecords
			grandTotalBytesK += totalBytesK
			grandTotalBytesV += totalBytesV
			for i, v := range totalOptBytesV {
				grandTotalOptBytesV[i] += v
			}
			grandTotalFiles += int(info.Size())
		}

		// GRAND TOTALS
		keys := make([]uint64, 0, len(grandIdxMap))
		for b := range grandIdxMap {
			keys = append(keys, b)
		}
		slices.Sort(keys)
		for _, b := range keys {
			gt := grandIdxMap[b]
			logger.Info("ALL",
				"bucket", b,
				"recs", gt.records,
				"k", gt.totalBytesK,
				"v", gt.totalBytesV,
				"%value", 100*(float64(gt.totalBytesV)/float64(grandTotalBytesV)),
			)
		}

		logger.Info("GRAND TOTALS",
			"totalBytes", grandTotalBytesK+grandTotalBytesV,
			"totalBytesFile", grandTotalFiles,
			"%keys", 100*float64(grandTotalBytesK)/float64(grandTotalBytesK+grandTotalBytesV),
			"%values", 100*float64(grandTotalBytesV)/float64(grandTotalBytesK+grandTotalBytesV),
		)

		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			log.Fatalf("Error writing csv: %v", err)
		}
	},
}

func init() {
	withDataDir(idxStat)
	idxStat.Flags().StringVar(&csvOutput, "csv", cli.CsvOutput.Value, cli.CsvOutput.Usage)
	rootCmd.AddCommand(idxStat)
}
