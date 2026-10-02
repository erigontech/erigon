package app

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"time"

	"github.com/urfave/cli/v3"
	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/seg"
)

func segInfo(ctx context.Context, cliCtx *cli.Command) error {
	logger := log.Root()

	// Compression settings
	var compression seg.FileCompression
	if err := compression.FromString(cliCtx.String("compress")); err != nil {
		return err
	}

	// Opens datadir/file
	file := cliCtx.String("file")
	if file == "" {
		return errors.New("file is required")
	}

	dirs := datadir.Open(cliCtx.String("datadir"))
	fullFilepath := filepath.Join(dirs.Snap, file)
	logger.Info("Opening file...", "file", fullFilepath)

	d, err := seg.NewDecompressor(fullFilepath)
	if err != nil {
		return err
	}
	defer d.Close()

	// Scan entire file and collect statistics
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	logger.Info("Scanning file...")
	r := seg.NewReader(d.MakeGetter(), compression)
	sizes := make([]int, 0, d.Count())
	i := 0
	for r.HasNext() {
		_, wordLen := r.Skip()
		i++
		sizes = append(sizes, wordLen)

		select {
		case <-ticker.C:
			logger.Info("Still scanning file...", "i", i, "total", d.Count())
		default:
		}
	}
	if len(sizes) != d.Count() {
		logger.Warn("Full scan of words doesn't match word count in file header", "header", d.Count(), "scanned", len(sizes))
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
	p.Printf("File size: %d byte(s)\n", d.Size())
	p.Printf("Serialized dict size: %d byte(s)\n", d.SerializedDictSize()) // word 3
	p.Printf("Dict words: %d\n", d.DictWords())
	p.Printf("Serialized len size: %d byte(s)\n", d.SerializedLenSize()) // word 4
	p.Printf("Dict lens: %d\n", d.DictLens())
	p.Printf("Unique lengths: %d\n", uniqueLengths)
	p.Printf("Data length: %d byte(s)\n", r.DataLen())
	p.Printf("Total raw words length: %d byte(s)\n", rawWordLen)

	p.Printf("\nWord count: %d\n", d.Count())                // word 1
	p.Printf("Empty words count: %d\n", d.EmptyWordsCount()) // word 2
	p.Printf("\nMin word length: %d byte(s)\n", minWordLen)
	p.Printf("Max word length: %d byte(s)\n", maxWordLen)
	p.Printf("Median word length: %d byte(s)\n", sizes[len(sizes)/2])
	p.Printf("P90 word length: %d byte(s)\n", sizes[len(sizes)*90/100])
	p.Printf("P95 word length: %d byte(s)\n", sizes[len(sizes)*95/100])
	p.Printf("P99 word length: %d byte(s)\n", sizes[len(sizes)*99/100])

	return nil
}
