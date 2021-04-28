package commands

import (
"errors"
	"fmt"
	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"time"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(snapshotMetainfoCmd)
}

func PrintMetaInfoHash(path string) error {
	t := time.Now()
	mi := metainfo.MetaInfo{}
	info, err := snapshotsync.BuildInfoBytesForSnapshot(path, snapshotsync.MdbxFilename)
	if err != nil {
		return err
	}
	mi.InfoBytes, err = bencode.Marshal(info)
	if err != nil {
		return err
	}

	fmt.Println("infohash:",mi.HashInfoBytes().String())
	fmt.Println("infobytes:", common.Bytes2Hex(mi.InfoBytes))
	fmt.Println("It took", time.Since(t))
	return nil
}
	var snapshotMetainfoCmd = &cobra.Command{
	Use:   "snapshotMetainfo",
	Short: "Calculate snapshot metainfo",
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return errors.New("empty path")
		}
		return PrintMetaInfoHash(args[0])
	},
}