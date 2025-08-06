package commands

import (
	"github.com/spf13/cobra"
)

var idxOptimize2 = &cobra.Command{
	Use:   "idx_optimize2",
	Short: "idx_optimize, but only for a specific file",
	Run: func(cmd *cobra.Command, args []string) {
		//TODO: re-enable
		/*
			ctx, _ := common.RootContext()
			logger := debug.SetupCobra(cmd, "integration")

			fullpath := file
			if !filepath.IsAbs(fullpath) {
				logger.Error("please give absolute path for file:", fullpath)
			}
			folder := filepath.Dir(fullpath)
			effile := filepath.Base(fullpath)
			dirs := datadir.New(datadirCli)
			idxPath := fullpath + "i.new"

			fmt.Printf("folder: %s, effile: %s fullpath:%s idxPath:%s\n", folder, effile, fullpath, idxPath)

			efInfo, err := parseEFFilename(effile)
			if err != nil {
				logger.Error("Failed to parse file info: ", "err", err)
				return
			}
			logger.Info("Optimizing...", "file", effile)

			baseTxNum := efInfo.startStep * config3.DefaultStepSize

			idxInput, err := seg.NewDecompressor(fullpath)
			if err != nil {
				logger.Error("Failed to open decompressor", "error", err)
				return
			}
			defer idxInput.Close()

			idxOutput, err := seg.NewCompressor(ctx, "optimizoor", fullpath+".new", dirs.Tmp, seg.DefaultWordLvlCfg, log.LvlInfo, logger)
			if err != nil {
				logger.Error("Failed to open compressor", "error", err)
				return
			}
			defer idxOutput.Close()

			// Summarize 1 idx file
			g := idxInput.MakeGetter()
			reader := seg.NewReader(g, seg.CompressNone)
			reader.Reset(0)

			writer := seg.NewWriter(idxOutput, seg.CompressNone)
			ps := background.NewProgressSet()

			for reader.HasNext() {
				k, _ := reader.Next(nil)
				if !reader.HasNext() {
					logger.Error("reader doesn't have next!")
					return
				}
				if _, err := writer.Write(k); err != nil {
					logger.Error("error while writing key", "error", err)
				}

				v, _ := reader.Next(nil)
				v, err := doConvert(baseTxNum, v)
				if err != nil {
					logger.Error("error while optimizing value", "error", err)
					return
				}
				if _, err := writer.Write(v); err != nil {
					logger.Error("error while writing value", "error", err)
					return
				}

				select {
				case <-ctx.Done():
					return
				default:
				}
			}
			if err := writer.Compress(); err != nil {
				logger.Error("error while writing optimized file", "error", err)
				return
			}
			idxInput.Close()
			writer.Close()
			idxOutput.Close()

			// rebuid .efi; COPIED FROM InvertedIndex.buildMapAccessor
			salt, err := state.GetStateIndicesSalt(dirs, false, logger)
			if err != nil {
				logger.Error("Failed to build accessor", "error", err)
				return
			}
			cfg := recsplit.RecSplitArgs{
				Version:            1,
				Enums:              true,
				LessFalsePositives: true,

				BucketSize: recsplit.DefaultBucketSize,
				LeafSize:   recsplit.DefaultLeafSize,
				TmpDir:     dirs.Tmp,
				IndexFile:  idxPath,
				Salt:       salt,
				NoFsync:    false,
			}
			data, err := seg.NewDecompressor(fullpath + ".new")
			if err != nil {
				logger.Error("Failed to build accessor", "error", err)
				return
			}
			logger.Info("building recsplit")
			if err := state.BuildHashMapAccessor(ctx, seg.NewReader(data.MakeGetter(), seg.CompressNone), idxPath, cfg, ps, logger); err != nil {
				logger.Error("Failed to build accessor", "error", err)
				return
			}

			logger.Info("Optimized file!!!")
		*/
	},
}

func init() {
	withFile(idxOptimize2)
	withDataDir(idxOptimize2)
	rootCmd.AddCommand(idxOptimize2)
}
