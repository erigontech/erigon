package commands

import (
	"bytes"
	"encoding/binary"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/spf13/cobra"
)

func readEliasFanoOrOpt(v []byte, baseTxNum uint64) *eliasfano32.EliasFano {
	if v[0]&0b10000000 == 0 {
		ef, _ := eliasfano32.ReadEliasFano(v)
		return ef
	}

	// not eliasfano, decode
	count := (len(v) - 1) / 4
	max := uint64(binary.BigEndian.Uint32(v[len(v)-4:])) + baseTxNum
	ef := eliasfano32.NewEliasFano(uint64(count), max)
	for i := 1; i <= len(v)-4; i += 4 {
		n := uint64(binary.BigEndian.Uint32(v[i:i+4])) + baseTxNum
		ef.AddOffset(n)
	}
	ef.Build()
	return ef
}

func compareOpt4(vOrig, vOpt []byte, baseTxNum uint64) bool {
	efOrig, _ := eliasfano32.ReadEliasFano(vOrig)
	efOpt := readEliasFanoOrOpt(vOpt, baseTxNum)

	if efOpt.Count() > efOrig.Count() {
		log.Print("Optimized eliasfano is longer")
		return false
	}
	if efOrig.Count() > efOpt.Count() {
		log.Print("Optimized eliasfano is shorter")
		return false
	}

	itOrig := efOrig.Iterator()
	itOpt := efOpt.Iterator()
	for itOrig.HasNext() {
		nOrig, err := itOrig.Next()
		if err != nil {
			log.Fatalf("Failed to read next: %v", err)
		}
		nOpt, err := itOpt.Next()
		if err != nil {
			log.Fatalf("Failed to read next: %v", err)
		}
		if nOrig != nOpt {
			log.Printf("values mismatch: orig=%d new=%d", nOrig, nOpt)
			log.Printf("orig=%v new=%v", hexutility.Encode(vOrig), hexutility.Encode(vOpt))
			return false
		}
	}

	return true
}

var idxCompare = &cobra.Command{
	Use:   "idx_compare",
	Short: "After an idx_optimize execution, deep compare original and optimized .ef files",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, _ := common.RootContext()

		idxPath := filepath.Join(datadirCli, "snapshots", "idx")
		idxDir := os.DirFS(idxPath)

		files, err := fs.ReadDir(idxDir, ".")
		if err != nil {
			log.Fatalf("Failed to read directory contents: %v", err)
		}

		log.Println("Comparing idx files:")
		for _, file := range files {
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".ef") {
				continue
			}

			log.Printf("Checking file %s...", file.Name())

			efi, err := recsplit.OpenIndex(datadirCli + "/snapshots/accessor/" + file.Name() + "i.new")
			if err != nil {
				log.Fatalf("Failed to open index: %v", err)
			}
			defer efi.Close()

			reader := efi.GetReaderFromPool()
			defer reader.Close()

			// original .ef file
			idxOrig, err := seg.NewDecompressor(datadirCli + "/snapshots/idx/" + file.Name())
			if err != nil {
				log.Fatalf("Failed to open decompressor: %v", err)
			}
			defer idxOrig.Close()

			// reencoded optimized .ef.new file
			idxOpt, err := seg.NewDecompressor(datadirCli + "/snapshots/idx/" + file.Name() + ".new")
			if err != nil {
				log.Fatalf("Failed to open decompressor: %v", err)
			}
			defer idxOpt.Close()

			g := idxOrig.MakeGetter()
			readerOrig := seg.NewReader(g, seg.CompressNone)
			readerOrig.Reset(0)

			g = idxOpt.MakeGetter()
			readerOpt := seg.NewReader(g, seg.CompressNone)
			readerOpt.Reset(0)

			// .ef.new MUST have a magic kv pair with baseTxNum
			if !readerOpt.HasNext() {
				log.Fatalf("reader doesn't have magic kv!")
			}
			k, _ := readerOpt.Next(nil)
			if !bytes.Equal(k, MAGIC_KEY_BASE_TX_NUM) {
				log.Fatalf("magic k is incorrect: %v", hexutility.Encode(k))
			}
			if !readerOpt.HasNext() {
				log.Fatalf("reader doesn't have magic number!")
			}
			v, prevKeyOffset := readerOpt.Next(nil)
			if len(v) != 8 {
				log.Fatalf("baseTxNum is not a uint64: %v", hexutility.Encode(v))
			}
			baseTxNum := binary.BigEndian.Uint64(v)

			for readerOrig.HasNext() {
				if !readerOpt.HasNext() {
					log.Fatal("opt reader doesn't have next!")
				}

				kOrig, _ := readerOrig.Next(nil)
				kOpt, _ := readerOpt.Next(nil)
				if !bytes.Equal(kOrig, kOpt) {
					log.Fatalf("key mismatch!")
				}

				if !readerOrig.HasNext() {
					log.Fatal("orig reader doesn't have next!")
				}
				if !readerOpt.HasNext() {
					log.Fatal("opt reader doesn't have next!")
				}

				// orig/opt value comparison
				vOrig, _ := readerOrig.Next(nil)
				vOpt, nextKeyOffset := readerOpt.Next(nil)
				if !compareOpt4(vOrig, vOpt, baseTxNum) {
					log.Fatalf("value mismatch!")
				}

				// checks new efi lookup points to the same value
				offset, found := reader.TwoLayerLookup(kOpt)
				if !found {
					log.Fatalf("key %v not found in efi", hexutility.Encode(kOpt))
				}
				if offset != prevKeyOffset {
					log.Fatalf("offset mismatch: %d != %d", offset, prevKeyOffset)
				}
				prevKeyOffset = nextKeyOffset

				select {
				case <-ctx.Done():
					return
				default:
				}
			}
			idxOrig.Close()
			idxOpt.Close()
			reader.Close()
			efi.Close()
		}
	},
}

func init() {
	withDataDir(idxCompare)
	rootCmd.AddCommand(idxCompare)
}
