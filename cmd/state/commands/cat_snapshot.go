package commands

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/spf13/cobra"
)

func init() {
	withFpath(catSnapshot)
	withCompressed(catSnapshot)
	withPick(catSnapshot)
	rootCmd.AddCommand(catSnapshot)
}

var (
	fpath      string
	compressed bool
	pick       string // print value only for keys with such prefix
)

func withFpath(cmd *cobra.Command) {
	cmd.Flags().StringVar(&fpath, "path", "", "path to .kv/.v file")
	// must(cmd.MarkFlagFilename("statsfile", "csv"))
}

func withCompressed(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&compressed, "compressed", false, "compressed")
}

func withPick(cmd *cobra.Command) {
	cmd.Flags().StringVar(&pick, "pick", "", "print value only for keys with such prefix")
}

var catSnapshot = &cobra.Command{
	Use:   "cat_snapshot",
	Short: "priint kv pairs from snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		if fpath == "" {
			return errors.New("fpath is required")
		}
		d, err := compress.NewDecompressor(fpath)
		if err != nil {
			return err
		}
		defer d.Close()

		fmt.Printf("File %s modtime %s (%s ago) size %v pairs %d \n", fpath, d.ModTime(), time.Since(d.ModTime()), (datasize.B * datasize.ByteSize(d.Size())).HR(), d.Count()/2)

		rd := state.NewArchiveGetter(d.MakeGetter(), compressed)

		pbytes := []byte{}
		if pick != "" {
			fmt.Printf("Picking prefix '%s'\n", pick)
			pbytes, _ = hex.DecodeString(pick)
		}

		count, dupCount := 0, 0

		uniq := make(map[string]struct{})
		for rd.HasNext() {
			k, _ := rd.Next(nil)
			v, _ := rd.Next(nil)

			if len(pbytes) != 0 && !bytes.HasPrefix(k, pbytes) {
				continue
			}
			if _, ok := uniq[string(k)]; ok {
				fmt.Printf("'%x' -> '%x' (duplicate)\n", k, v)
				dupCount++
			}
			uniq[string(k)] = struct{}{}
			count++
			fmt.Printf("'%x' -> '%x'\n", k, v)
		}
		if len(pbytes) != 0 {
			fmt.Printf("Picked %d pairs\n", count)
		}
		fmt.Printf("Found Duplicates %d\n", dupCount)

		return nil
	},
}
