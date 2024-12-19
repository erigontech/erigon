package commands

import (
	"encoding/binary"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/config3"
	lllog "github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
	"github.com/erigontech/erigon-lib/state"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/spf13/cobra"
)

var MAGIC_KEY_BASE_TX_NUM = hexutility.MustDecodeHex("0x8453FFFFFFFFFFFFFFFFFFFF")

// Delta encoding starting from 1st elem; only for ef sequences < 16 elems
//
// Encode all elems as deltas from baseTxId; they can fit into uint32
// because max delta is bounded by 64 * stepSize == 100M
// hence size == count * sizeof(uint32) + 1 byte for encoding type
func doOpt4(baseTxNum uint64, v []byte) ([]byte, error) {
	ef, _ := eliasfano32.ReadEliasFano(v)
	count := ef.Count()
	if count < 16 {
		if ef.Max()-ef.Min()+1 < uint64(0xffffffff) {
			return convertEF(baseTxNum, ef)
		}
	}

	return v, nil // DO NOT OPTIMIZE; plain elias fano
}

func convertEF(baseTxNum uint64, ef *eliasfano32.EliasFano) ([]byte, error) {
	b := make([]byte, 0, 1+ef.Count()*4)
	b = append(b, 0b10000000)
	for it := ef.Iterator(); it.HasNext(); {
		n, err := it.Next()
		if err != nil {
			return nil, err
		}
		n -= baseTxNum

		bn := make([]byte, 4)
		binary.BigEndian.PutUint32(bn, uint32(n))
		b = append(b, bn...)
	}
	return b, nil
}

var idxOptimize = &cobra.Command{
	Use:   "idx_optimize",
	Short: "Scan .ef files, backup them up, reencode and optimize them, rebuild .efi files",
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

		log.Println("Sumarizing idx files...")
		cEF := 0
		for _, file := range files {
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".ef") {
				continue
			}
			cEF++
		}

		log.Println("Optimizing idx files...")
		cOpt := 0
		for _, file := range files {
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".ef") {
				continue
			}

			efInfo, err := parseEFFilename(file.Name())
			if err != nil {
				logger.Error("Failed to parse file info: ", err)
			}
			log.Printf("Optimizing file %s [%d/%d]...", file.Name(), cOpt, cEF)

			// only optimize frozen files for this experiment, because we are not
			// implementing collation, merge, etc. support now
			// if efInfo.stepSize < 64 {
			// 	log.Printf("Skipping file %s, step size %d < 64", file.Name(), efInfo.stepSize)
			// 	continue
			// }
			cOpt++
			baseTxNum := efInfo.startStep * config3.DefaultStepSize

			tmpDir := datadirCli + "/temp"

			idxInput, err := seg.NewDecompressor(datadirCli + "/snapshots/idx/" + file.Name())
			if err != nil {
				log.Fatalf("Failed to open decompressor: %v", err)
			}
			defer idxInput.Close()

			idxOutput, err := seg.NewCompressor(ctx, "optimizoor", datadirCli+"/snapshots/idx/"+file.Name()+".new", tmpDir, seg.DefaultCfg, lllog.LvlInfo, logger)
			if err != nil {
				log.Fatalf("Failed to open compressor: %v", err)
			}
			defer idxOutput.Close()

			// Summarize 1 idx file
			g := idxInput.MakeGetter()
			reader := seg.NewReader(g, seg.CompressNone)
			reader.Reset(0)

			writer := seg.NewWriter(idxOutput, seg.CompressNone)
			writer.AddWord(MAGIC_KEY_BASE_TX_NUM)
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, baseTxNum)
			writer.AddWord(b)

			for reader.HasNext() {
				k, _ := reader.Next(nil)
				if !reader.HasNext() {
					log.Fatal("reader doesn't have next!")
				}
				if err := writer.AddWord(k); err != nil {
					log.Fatalf("error while writing key %v", err)
				}

				v, _ := reader.Next(nil)
				v, err := doOpt4(baseTxNum, v)
				if err != nil {
					log.Fatalf("error while optimizing value %v", err)
				}
				if err := writer.AddWord(v); err != nil {
					log.Fatalf("error while writing value %v", err)
				}

				select {
				case <-ctx.Done():
					return
				default:
				}
			}
			if err := writer.Compress(); err != nil {
				log.Fatalf("error while writing optimized file %v", err)
			}
			idxInput.Close()
			writer.Close()
			idxOutput.Close()

			// rebuid .efi; COPIED FROM InvertedIndex.buildMapAccessor
			salt, err := state.GetStateIndicesSalt(datadirCli + "/snapshots/")
			if err != nil {
				log.Fatalf("Failed to build accessor: %v", err)
			}
			idxPath := datadirCli + "/snapshots/accessor/" + file.Name() + "i.new"
			cfg := recsplit.RecSplitArgs{
				Enums:              true,
				LessFalsePositives: true,

				BucketSize: 2000,
				LeafSize:   8,
				TmpDir:     tmpDir,
				IndexFile:  idxPath,
				Salt:       salt,
				NoFsync:    false,
			}
			data, err := seg.NewDecompressor(datadirCli + "/snapshots/idx/" + file.Name() + ".new")
			if err != nil {
				log.Fatalf("Failed to build accessor: %v", err)
			}
			ps := background.NewProgressSet()
			if err := state.BuildAccessor(ctx, data, seg.CompressNone, idxPath, false, cfg, ps, logger); err != nil {
				log.Fatalf("Failed to build accessor: %v", err)
			}
		}

		log.Printf("Optimized %d of %d files!!!", cOpt, cEF)
	},
}

func init() {
	withDataDir(idxOptimize)
	rootCmd.AddCommand(idxOptimize)
}
