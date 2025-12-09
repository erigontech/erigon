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
	"bufio"
	"os"
	"path"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/components"
	"github.com/go-echarts/go-echarts/v2/opts"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/commitment"
)

type MetricValues struct {
	commitment.MetricValues
	BatchId uint64
}

func renderChartsPage(mv []MetricValues, outputDir string) (string, error) {
	f, err := os.Create(path.Join(outputDir, "charts.html"))
	if err != nil {
		panic(err)
	}
	defer func() {
		err := f.Close()
		if err != nil {
			log.Error("failed to close charts file while rendering", "file", f.Name(), "err", err)
		}
	}()
	w := bufio.NewWriter(f)
	defer func() {
		err := w.Flush()
		if err != nil {
			log.Error("failed to flush charts file while rendering", "file", f.Name(), "err", err)
		}
	}()
	err = generateChartsPage(mv).Render(w)
	if err != nil {
		return "", err
	}
	return f.Name(), nil
}

func generateChartsPage(mv []MetricValues) *components.Page {
	page := components.NewPage()
	page.SetPageTitle("commitment backtest charts")
	sharedGlobalOpts := []charts.GlobalOpts{
		charts.WithInitializationOpts(opts.Initialization{Width: "100%"}),
		charts.WithLegendOpts(opts.Legend{Bottom: "5%", Left: "center"}),
		charts.WithGridOpts(opts.Grid{Bottom: "15%"}),
	}
	page.AddCharts(
		generateProcessingTimesChart(mv, sharedGlobalOpts...),
		generateTrieUpdatesChart(mv, sharedGlobalOpts...),
		generateDbRwChart(mv, sharedGlobalOpts...),
	)
	return page
}

func generateProcessingTimesChart(mv []MetricValues, globalOpts ...charts.GlobalOpts) *charts.Line {
	batchIds := make([]uint64, len(mv))
	totalProcessingTime := make([]opts.LineData, len(mv))
	unfoldingProcessingTime := make([]opts.LineData, len(mv))
	foldingProcessingTime := make([]opts.LineData, len(mv))
	for i := range mv {
		batchIds[i] = mv[i].BatchId
		totalProcessingTime[i] = opts.LineData{Value: mv[i].SpentProcessing.Milliseconds()}
		unfoldingProcessingTime[i] = opts.LineData{Value: mv[i].SpentUnfolding.Milliseconds()}
		foldingProcessingTime[i] = opts.LineData{Value: mv[i].SpentFolding.Milliseconds()}
	}
	chart := charts.NewLine()
	chart.SetGlobalOptions(charts.WithTitleOpts(opts.Title{Title: "processing times", Left: "center", Top: "5%"}))
	chart.SetGlobalOptions(globalOpts...)
	stackOpts := []charts.SeriesOpts{
		charts.WithLineChartOpts(opts.LineChart{Stack: "total"}),
		charts.WithAreaStyleOpts(opts.AreaStyle{Opacity: opts.Float(0.2)}),
	}
	chart.SetXAxis(batchIds).
		AddSeries("unfolding(ms)", unfoldingProcessingTime, stackOpts...).
		AddSeries("folding(ms)", foldingProcessingTime, stackOpts...).
		AddSeries("total(ms)", totalProcessingTime)
	return chart
}

func generateTrieUpdatesChart(mv []MetricValues, globalOpts ...charts.GlobalOpts) *charts.Line {
	batchIds := make([]uint64, len(mv))
	accountUpdates := make([]opts.LineData, len(mv))
	storageUpdates := make([]opts.LineData, len(mv))
	for i := range mv {
		batchIds[i] = mv[i].BatchId
		accountUpdates[i] = opts.LineData{Value: mv[i].AddressKeys}
		storageUpdates[i] = opts.LineData{Value: mv[i].StorageKeys}
	}
	chart := charts.NewLine()
	chart.SetGlobalOptions(charts.WithTitleOpts(opts.Title{Title: "trie updates", Left: "center", Top: "5%"}))
	chart.SetGlobalOptions(globalOpts...)
	chart.SetXAxis(batchIds).
		AddSeries("account", accountUpdates).
		AddSeries("storage", storageUpdates).
		SetSeriesOptions(
			charts.WithLineChartOpts(opts.LineChart{Stack: "total"}),
			charts.WithAreaStyleOpts(opts.AreaStyle{Opacity: opts.Float(0.2)}),
		)
	return chart
}

func generateDbRwChart(mv []MetricValues, globalOpts ...charts.GlobalOpts) *charts.Line {
	batchIds := make([]uint64, len(mv))
	accountReads := make([]opts.LineData, len(mv))
	storageReads := make([]opts.LineData, len(mv))
	branchReads := make([]opts.LineData, len(mv))
	branchWrites := make([]opts.LineData, len(mv))
	for i := range mv {
		batchIds[i] = mv[i].BatchId
		accountReads[i] = opts.LineData{Value: mv[i].LoadAccount}
		storageReads[i] = opts.LineData{Value: mv[i].LoadStorage}
		branchReads[i] = opts.LineData{Value: mv[i].LoadBranch}
		branchWrites[i] = opts.LineData{Value: mv[i].UpdateBranch}
	}
	chart := charts.NewLine()
	chart.SetGlobalOptions(charts.WithTitleOpts(opts.Title{Title: "db reads/writes", Left: "center", Top: "5%"}))
	chart.SetGlobalOptions(globalOpts...)
	chart.SetXAxis(batchIds).
		AddSeries("account_r", accountReads).
		AddSeries("storage_r", storageReads).
		AddSeries("branch_r", branchReads).
		AddSeries("branch_w", branchWrites).
		SetSeriesOptions(
			charts.WithLineChartOpts(opts.LineChart{Stack: "total"}),
			charts.WithAreaStyleOpts(opts.AreaStyle{Opacity: opts.Float(0.2)}),
		)
	return chart
}
