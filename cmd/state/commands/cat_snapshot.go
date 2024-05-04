package commands

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/spf13/cobra"

	"github.com/ledgerwatch/erigon-lib/seg"
	"github.com/ledgerwatch/erigon-lib/state"
)

func init() {
	withFpath(catSnapshot)
	withCompressed(catSnapshot)
	withPick(catSnapshot)
	rootCmd.AddCommand(catSnapshot)
}

var (
	fpath      string
	compressed string
	pick       string // print value only for keys with such prefix
)

func withFpath(cmd *cobra.Command) {
	cmd.Flags().StringVar(&fpath, "path", "", "path to .kv/.v file")
	// must(cmd.MarkFlagFilename("statsfile", "csv"))
}

func withCompressed(cmd *cobra.Command) {
	cmd.Flags().StringVar(&compressed, "compress", "", "hint if we need to decompress keys or values or both (k|v|kv). Empty argument means no compression used")
}

func withPick(cmd *cobra.Command) {
	cmd.Flags().StringVar(&pick, "pick", "", "print value only for keys with such prefix")
}

var catSnapshot = &cobra.Command{
	Use:   "cat_snapshot",
	Short: "print kv pairs from snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		if fpath == "" {
			return errors.New("fpath is required")
		}
		d, err := seg.NewDecompressor(fpath)
		if err != nil {
			return err
		}
		defer d.Close()

		fmt.Printf("File %s modtime %s (%s ago) size %v pairs %d \n", fpath, d.ModTime(), time.Since(d.ModTime()), (datasize.B * datasize.ByteSize(d.Size())).HR(), d.Count()/2)

		compFlags := state.CompressNone
		switch strings.ToLower(compressed) {
		case "k":
			compFlags = state.CompressKeys
		case "v":
			compFlags = state.CompressVals
		case "kv":
			compFlags = state.CompressKeys | state.CompressVals
		case "":
			break
		default:
			return fmt.Errorf("unknown compression flags %s", compressed)
		}

		rd := state.NewArchiveGetter(d.MakeGetter(), compFlags)

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
