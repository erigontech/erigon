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
	"cmp"
	"encoding/hex"
	"os"
	"path"
	"slices"
	"strconv"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/components"
	"github.com/go-echarts/go-echarts/v2/opts"

	"github.com/erigontech/erigon/common/length"
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
	page.SetLayout(components.PageFlexLayout)
	top10Slowest := generateTop10SlowestCharts(mv)
	// place 3 in 1 row
	top10Slowest.times.SetGlobalOptions(widthOpts("30vw"))
	top10Slowest.updates.SetGlobalOptions(widthOpts("30vw"))
	top10Slowest.suGinis.SetGlobalOptions(widthOpts("30vw"))
	page.AddCharts(
		top10Slowest.times,
		top10Slowest.updates,
		top10Slowest.suGinis,
	)
	// place 1 per row
	processingTimes := generateProcessingTimesChart(mv)
	processingTimes.SetGlobalOptions(widthOpts("100vw"))
	trieUpdates := generateTrieUpdatesChart(mv)
	trieUpdates.SetGlobalOptions(widthOpts("100vw"))
	dbRw := generateDbRwChart(mv)
	dbRw.SetGlobalOptions(widthOpts("100vw"))
	page.AddCharts(
		processingTimes,
		trieUpdates,
		dbRw,
	)
	return page
}

type top10SlowestCharts struct {
	times   *charts.Line
	updates *charts.Bar
	suGinis *charts.Line
}

func generateTop10SlowestCharts(mv []MetricValues) top10SlowestCharts {
	mv = slices.SortedStableFunc(slices.Values(mv), func(v1 MetricValues, v2 MetricValues) int {
		return -cmp.Compare(v1.SpentProcessing.Milliseconds(), v2.SpentProcessing.Milliseconds())
	})
	if len(mv) > 10 {
		mv = mv[:10]
	}
	nums := make([]int, len(mv))
	times := make([]opts.LineData, len(mv))
	accs := make([]opts.BarData, len(mv))
	storage := make([]opts.BarData, len(mv))
	maxSuPerAcc := make([]opts.BarData, len(mv))
	suGinis := make([]opts.LineData, len(mv))
	for i := range mv {
		name := strconv.FormatUint(mv[i].BatchId, 10)
		nums[i] = i + 1
		times[i] = opts.LineData{Name: name, Value: mv[i].SpentProcessing.Milliseconds()}
		accs[i] = opts.BarData{Name: name, Value: mv[i].AddressKeys}
		storage[i] = opts.BarData{Name: name, Value: mv[i].StorageKeys}
		var accMaxSu uint64
		accSus := make([]uint64, 0, len(mv[i].Accounts))
		for k, v := range mv[i].Accounts {
			if len(k) == hex.EncodedLen(length.Addr) && v.LoadBranch == 0 {
				accMaxSu = max(accMaxSu, v.StorageUpates)
				accSus = append(accSus, v.StorageUpates)
			}
		}
		maxSuPerAcc[i] = opts.BarData{Name: name, Value: accMaxSu}
		suGinis[i] = opts.LineData{Name: name, Value: giniCoefficient(accSus)}
	}
	timesChart := charts.NewLine()
	timesChart.SetGlobalOptions(
		subTitleOpts("top 10 slowest", "processing times"),
		noLegendOpts(),
	)
	timesChart.SetXAxis(nums).
		AddSeries("times", times)
	updatesChart := charts.NewBar()
	updatesChart.SetGlobalOptions(
		subTitleOpts("top 10 slowest", "trie updates"),
		legendOpts(),
	)
	updatesChart.SetXAxis(nums).
		AddSeries("accs", accs).
		AddSeries("storage", storage).
		AddSeries("maxSuPerAcc", maxSuPerAcc)
	suGinisChart := charts.NewLine()
	suGinisChart.SetGlobalOptions(
		subTitleOpts("top 10 slowest", "storage updates gini coefficient"),
		noLegendOpts(),
	)
	suGinisChart.SetXAxis(nums).
		AddSeries("gini", suGinis)
	return top10SlowestCharts{times: timesChart, updates: updatesChart, suGinis: suGinisChart}
}

func generateProcessingTimesChart(mv []MetricValues) *charts.Line {
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
	chart.SetGlobalOptions(
		titleOpts("processing times"),
		legendOpts(),
		gridOpts(),
	)
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

func generateTrieUpdatesChart(mv []MetricValues) *charts.Line {
	batchIds := make([]uint64, len(mv))
	accountUpdates := make([]opts.LineData, len(mv))
	storageUpdates := make([]opts.LineData, len(mv))
	for i := range mv {
		batchIds[i] = mv[i].BatchId
		accountUpdates[i] = opts.LineData{Value: mv[i].AddressKeys}
		storageUpdates[i] = opts.LineData{Value: mv[i].StorageKeys}
	}
	chart := charts.NewLine()
	chart.SetGlobalOptions(
		titleOpts("trie updates"),
		legendOpts(),
		gridOpts(),
	)
	chart.SetXAxis(batchIds).
		AddSeries("account", accountUpdates).
		AddSeries("storage", storageUpdates).
		SetSeriesOptions(
			charts.WithLineChartOpts(opts.LineChart{Stack: "total"}),
			charts.WithAreaStyleOpts(opts.AreaStyle{Opacity: opts.Float(0.2)}),
		)
	return chart
}

func generateDbRwChart(mv []MetricValues) *charts.Line {
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
	chart.SetGlobalOptions(
		titleOpts("db reads/writes"),
		legendOpts(),
		gridOpts(),
	)
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

func titleOpts(title string) charts.GlobalOpts {
	return charts.WithTitleOpts(opts.Title{Title: title, Left: "center", Top: "5%"})
}

func subTitleOpts(title string, subtitle string) charts.GlobalOpts {
	return charts.WithTitleOpts(opts.Title{Title: title, Subtitle: subtitle, Left: "center", Top: "2%"})
}

func widthOpts(width string) charts.GlobalOpts {
	return charts.WithInitializationOpts(opts.Initialization{Width: width})
}

func legendOpts() charts.GlobalOpts {
	return charts.WithLegendOpts(opts.Legend{Bottom: "5%", Left: "center"})
}

func noLegendOpts() charts.GlobalOpts {
	return charts.WithLegendOpts(opts.Legend{Show: opts.Bool(false)})
}

func gridOpts() charts.GlobalOpts {
	return charts.WithGridOpts(opts.Grid{Bottom: "15%"})
}

func giniCoefficient(values []uint64) float64 {
	n := len(values)
	if n == 0 {
		return 0
	}
	sorted := make([]uint64, n)
	copy(sorted, values)
	slices.Sort(sorted)
	var sum, weightedSum float64
	for i, v := range sorted {
		sum += float64(v)
		weightedSum += (float64(i) + 1) * float64(v)
	}
	if sum == 0 {
		return 0
	}
	return (2*weightedSum)/(float64(n)*sum) - (float64(n)+1)/float64(n)
}
