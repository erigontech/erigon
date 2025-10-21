package app

import (
	"errors"
	"path/filepath"
	"slices"
	"time"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/node/debug"
	"github.com/urfave/cli/v2"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

func segInfo(cliCtx *cli.Context) error {
	logger, _, _, _, err := debug.Setup(cliCtx, true /* root logger */)
	if err != nil {
		return err
	}

	// Compression settings
	compress := cliCtx.String("compress")
	if compress != "all" && compress != "none" && compress != "keys" && compress != "values" {
		return errors.New("invalid compression type: " + compress)
	}

	// Opens datadir/file
	file := cliCtx.String("file")
	if file == "" {
		return errors.New("file is required")
	}

	dirs := datadir.Open(cliCtx.String("datadir"))
	fullFilepath := filepath.Join(dirs.Snap, file)
	logger.Info("Opening file...", "file", fullFilepath)

	seg, err := seg.NewDecompressor(fullFilepath)
	if err != nil {
		return err
	}
	defer seg.Close()

	// Scan entire file and collect statistics
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	logger.Info("Scanning file...")
	g := seg.MakeGetter()
	sizes := make([]int, 0, seg.Count())
	i := 0
	var w []byte
	compressedKeys := compress == "all" || compress == "keys"
	compressedValues := compress == "all" || compress == "values"
	for g.HasNext() {
		if (i%2 == 0 && compressedKeys) || (i%2 == 1 && compressedValues) {
			w, _ = g.Next(w[:0])
		} else {
			w, _ = g.NextUncompressed()
		}

		i++
		sizes = append(sizes, len(w))

		select {
		case <-ticker.C:
			logger.Info("Still scanning file...", "i", i, "total", seg.Count())
		default:
		}
	}
	if len(sizes) != seg.Count() {
		logger.Warn("Full scan of words doesn't match word count in file header", "header", seg.Count(), "scanned", len(sizes))
	}
	slices.Sort(sizes)
	minWordLen := sizes[0]
	maxWordLen := sizes[len(sizes)-1]

	// Calculate length statistics
	l := -1
	uniqueLengths := 0
	rawWordLen := 0
	for _, v := range sizes {
		if v != l {
			l = v
			uniqueLengths++
		}
		rawWordLen += v
	}

	// Print stats
	p := message.NewPrinter(language.English)
	p.Printf("\nFile statistics:\n\n")
	p.Printf("File size: %d byte(s)\n", seg.Size())
	p.Printf("Serialized dict size: %d byte(s)\n", seg.SerializedDictSize()) // word 3
	p.Printf("Dict words: %d\n", seg.DictWords())
	p.Printf("Serialized len size: %d byte(s)\n", seg.SerializedLenSize()) // word 4
	p.Printf("Dict lens: %d\n", seg.DictLens())
	p.Printf("Unique lengths: %d\n", uniqueLengths)
	p.Printf("Data length: %d byte(s)\n", g.DataLen())
	p.Printf("Total raw words length: %d byte(s)\n", rawWordLen)

	p.Printf("\nWord count: %d\n", seg.Count())                // word 1
	p.Printf("Empty words count: %d\n", seg.EmptyWordsCount()) // word 2
	p.Printf("\nMin word length: %d byte(s)\n", minWordLen)
	p.Printf("Max word length: %d byte(s)\n", maxWordLen)
	p.Printf("Median word length: %d byte(s)\n", sizes[len(sizes)/2])
	p.Printf("P90 word length: %d byte(s)\n", sizes[len(sizes)*90/100])
	p.Printf("P95 word length: %d byte(s)\n", sizes[len(sizes)*95/100])
	p.Printf("P99 word length: %d byte(s)\n", sizes[len(sizes)*99/100])

	return nil
}
