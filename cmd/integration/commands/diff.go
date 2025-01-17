package commands

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"reflect"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/snaptype"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

var cmdDiffHeaderSeg = &cobra.Command{
	Use:   "diff_header_seg",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		err := diffHeaderSeg(logger)
		if err != nil {
			logger.Error(err.Error())
		} else {
			logger.Info("No diffs found")
		}
	},
}

func diffHeaderSeg(logger log.Logger) error {
	if datadirCli == "" {
		panic("--datadir is required")
	}
	if datadirCli2 == "" {
		panic("--datadir2 is required")
	}
	if from == 0 && to == 0 {
		panic("--from and --to must be specified")
	}
	if from > to {
		panic("--from must be greater than --to")
	}

	dirs1 := datadir.New(datadirCli)
	dirs2 := datadir.New(datadirCli2)
	chainConfig := params.ChainConfigByChainName(chain)
	snapCfg := ethconfig.NewSnapCfg(true, true, true, chainConfig.ChainName)
	roSnapshots1 := freezeblocks.NewRoSnapshots(snapCfg, dirs1.Snap, 0, logger)
	roSnapshots2 := freezeblocks.NewRoSnapshots(snapCfg, dirs2.Snap, 0, logger)
	blockReader1 := freezeblocks.NewBlockReader(roSnapshots1, nil, nil, nil)
	blockReader2 := freezeblocks.NewBlockReader(roSnapshots2, nil, nil, nil)

	err := roSnapshots1.OpenFolder()
	if err != nil {
		return err
	}

	err = roSnapshots2.OpenFolder()
	if err != nil {
		return err
	}

	roSnapshots1.LogStat("roSnapshots1: " + datadirCli)
	roSnapshots2.LogStat("roSnapshots2: " + datadirCli2)
	for blockNum := from; blockNum < to; blockNum++ {
		logger.Info("diffing", "blockNum", blockNum)

		visibleSeg1, ok, _ := roSnapshots1.ViewSingleFile(snaptype.Headers, blockNum)
		if !ok {
			panic(fmt.Sprintf("failed to open seg for block %d in dir %s", blockNum, datadirCli))
		}

		visibleSeg2, ok, _ := roSnapshots2.ViewSingleFile(snaptype.Headers, blockNum)
		if !ok {
			return fmt.Errorf("failed to open seg for block %d in dir %s", blockNum, datadirCli2)
		}

		var buf1, buf2 []byte
		var header1, header2 *types.Header
		header1, buf1, err = blockReader1.HeaderFromSnapshot(blockNum, visibleSeg1, buf1)
		if err != nil {
			return err
		}
		if header1 == nil {
			return fmt.Errorf("nil header for block %d in dir %s", blockNum, datadirCli)
		}
		if len(buf1) == 0 {
			return fmt.Errorf("nil buf for block %d in dir %s", blockNum, datadirCli)
		}

		logger.Info("header1 full info", "info", fmt.Sprintf("%+v", header1))

		header2, buf2, err = blockReader2.HeaderFromSnapshot(blockNum, visibleSeg2, buf2)
		if err != nil {
			return err
		}
		if header2 == nil {
			return fmt.Errorf("nil header for block %d in dir %s", blockNum, datadirCli2)
		}
		if len(buf2) == 0 {
			return fmt.Errorf("nil buf for block %d in dir %s", blockNum, datadirCli2)
		}

		logger.Info("header2 full info", "info", fmt.Sprintf("%+v", header2))

		if !reflect.DeepEqual(header1, header2) {
			logger.Error("deep equal mismatch", "header1", header1, "header2", header2)
		}

		if !bytes.Equal(header1.Hash().Bytes(), header2.Hash().Bytes()) {
			return fmt.Errorf("header hashes differ for blockNum=%d", blockNum)
		}

		if !bytes.Equal(buf1, buf2) {
			return fmt.Errorf("header bufs differ for blockNum=%d", blockNum)
		}
	}
	for blockNum := from - 1000; blockNum < from; blockNum++ {
		logger.Info("checking for prev values", "blockNum", blockNum)

		_, ok, _ := roSnapshots1.ViewSingleFile(snaptype.Headers, blockNum)
		if ok {
			panic(fmt.Sprintf("unexpected header for block %d in dir %s", blockNum, datadirCli))
		}

		_, ok, _ = roSnapshots2.ViewSingleFile(snaptype.Headers, blockNum)
		if ok {
			panic(fmt.Sprintf("unexpected header for block %d in dir %s", blockNum, datadirCli2))
		}
	}

	for blockNum := to; blockNum < to+1000; blockNum++ {
		logger.Info("checking for subseq values", "blockNum", blockNum)

		_, ok, _ := roSnapshots1.ViewSingleFile(snaptype.Headers, blockNum)
		if ok {
			panic(fmt.Sprintf("unexpected header for block %d in dir %s", blockNum, datadirCli))
		}

		_, ok, _ = roSnapshots2.ViewSingleFile(snaptype.Headers, blockNum)
		if ok {
			panic(fmt.Sprintf("unexpected header for block %d in dir %s", blockNum, datadirCli2))
		}
	}

	{
		// indeed different bytes in files
		const fileName = "v1-006800-006900-headers.seg"
		b1, err := os.ReadFile(path.Join(dirs1.Snap, fileName))
		if err != nil {
			return err
		}
		b2, err := os.ReadFile(path.Join(dirs2.Snap, fileName))
		if err != nil {
			return err
		}
		if !bytes.Equal(b1, b2) {
			logger.Info("confirming file bytes are different")
		}
	}

	{
		// getter.Next to find byte diff
		const blockNum = 6_800_000
		logger.Info("checking for snapshot getter next", "blockNum", blockNum)

		visibleSeg1, ok, _ := roSnapshots1.ViewSingleFile(snaptype.Headers, blockNum)
		if !ok {
			panic(fmt.Sprintf("failed to open seg for block %d in dir %s", blockNum, datadirCli))
		}

		visibleSeg2, ok, _ := roSnapshots2.ViewSingleFile(snaptype.Headers, blockNum)
		if !ok {
			return fmt.Errorf("failed to open seg for block %d in dir %s", blockNum, datadirCli2)
		}

		var buf1, buf2 []byte
		var pos1, pos2 uint64
		var i int
		getter1 := visibleSeg1.Src().MakeGetter()
		getter2 := visibleSeg2.Src().MakeGetter()
		for {
			i++
			hasNext1, hasNext2 := getter1.HasNext(), getter2.HasNext()
			if hasNext1 != hasNext2 {
				return fmt.Errorf("diff in has next at i=%d with %v vs %v", i, hasNext1, hasNext2)
			}

			if !hasNext1 && !hasNext2 {
				break
			}

			buf1, pos1 = getter1.Next(buf1)
			buf2, pos2 = getter2.Next(buf2)

			if pos1 != pos2 && i < 4 {
				log.Info("diff in has next pos", "i", i, "pos1", pos1, "pos2", pos2)
				//return fmt.Errorf("diff in has next pos at i=%d with %v vs %v", i, pos1, pos2)
			}

			if !bytes.Equal(buf1, buf2) {
				return fmt.Errorf("diff in getter next bytes at i=%d with %v vs %v", i, buf1, buf2)
			}
		}
	}

	return nil
}

func init() {
	withChain(cmdDiffHeaderSeg)
	withDataDir(cmdDiffHeaderSeg)
	with2DataDirs(cmdDiffHeaderSeg)
	withFrom(cmdDiffHeaderSeg)
	withTo(cmdDiffHeaderSeg)
	rootCmd.AddCommand(cmdDiffHeaderSeg)
}
