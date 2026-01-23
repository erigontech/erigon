// Copyright 2025 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package backtester

import (
	"errors"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/commitment"
)

func CompareRuns(runOutputDirs []string, outputDir string, logger log.Logger) error {
	minBlock := uint64(math.MaxUint64)
	maxBlock := uint64(0)
	runIds := make([]string, len(runOutputDirs))
	runOverviewPages := make([]string, len(runOutputDirs))
	for i, runOutputDir := range runOutputDirs {
		ri := extractRunId(path.Base(runOutputDir))
		runIds[i] = ri.String()
		runOverviewPages[i] = path.Join(runOutputDir, "overview.html")
		minBlock = min(minBlock, ri.fromBlock)
		maxBlock = max(maxBlock, ri.toBlock)
	}
	blockCount := maxBlock - minBlock + 1
	var blockNums []uint64
	processingTimes := make([][]time.Duration, len(runOutputDirs))
	for i, runOutputDir := range runOutputDirs {
		populateBlockNums := blockNums == nil
		if populateBlockNums {
			blockNums = make([]uint64, blockCount)
		}
		processingTimes[i] = make([]time.Duration, blockCount)
		for block := minBlock; block <= maxBlock; block++ {
			j := block - minBlock
			if populateBlockNums {
				blockNums[j] = block
			}
			blockOutputDir := deriveBlockOutputDir(runOutputDir, block)
			commitmentMetricsFilePrefix := deriveBlockMetricsFilePrefix(blockOutputDir)
			mVals, err := commitment.UnmarshallMetricValuesCsv(commitmentMetricsFilePrefix)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					// can happen if the runs we are comparing have block ranges with different start/end blocks
					// in this case we just leave the processing times as 0 so that we can still plot the graphs
					logger.Debug("skipping non existing metrics file", "prefix", commitmentMetricsFilePrefix, "err", err)
					continue
				}
				return err
			}
			if len(mVals) != 1 {
				return fmt.Errorf("expected metrics for 1 batch: got=%d, dir=%s", len(mVals), blockOutputDir)
			}
			processingTimes[i][j] = mVals[0].SpentProcessing
		}
	}
	err := os.MkdirAll(outputDir, 0755)
	if err != nil {
		return err
	}
	comparisonFilePath, err := renderComparisonPage(runIds, blockNums, processingTimes, runOverviewPages, outputDir)
	if err != nil {
		return err
	}
	logger.Info("comparison page rendered", "at", fmt.Sprintf("file://%s", comparisonFilePath))
	return nil
}
