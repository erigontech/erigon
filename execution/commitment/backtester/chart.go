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
	// place 1 per row
	top10Slowest.times.SetGlobalOptions(widthOpts("100vw"))
	page.AddCharts(top10Slowest.times)
	// place 2 per row
	top10Slowest.unfoldTimeGinis.SetGlobalOptions(widthOpts("45vw"))
	top10Slowest.foldTimeGinis.SetGlobalOptions(widthOpts("45vw"))
	page.AddCharts(top10Slowest.unfoldTimeGinis, top10Slowest.foldTimeGinis)
	// place 2 per row
	top10Slowest.updates.SetGlobalOptions(widthOpts("45vw"))
	top10Slowest.suGinis.SetGlobalOptions(widthOpts("45vw"))
	page.AddCharts(top10Slowest.updates, top10Slowest.suGinis)
	// place 1 per row
	processingTimes := generateProcessingTimesChart(mv)
	processingTimes.SetGlobalOptions(widthOpts("100vw"))
	unfoldTimeGinis := generateUnfoldTimeGinisChart(mv)
	unfoldTimeGinis.SetGlobalOptions(widthOpts("100vw"))
	foldTimeGinis := generateFoldTimeGinisChart(mv)
	foldTimeGinis.SetGlobalOptions(widthOpts("100vw"))
	trieUpdates := generateTrieUpdatesChart(mv)
	trieUpdates.SetGlobalOptions(widthOpts("100vw"))
	storageUpdateGinis := generateStorageUpdateGinisChart(mv)
	storageUpdateGinis.SetGlobalOptions(widthOpts("100vw"))
	dbRw := generateDbRwChart(mv)
	dbRw.SetGlobalOptions(widthOpts("100vw"))
	page.AddCharts(
		processingTimes,
		unfoldTimeGinis,
		foldTimeGinis,
		trieUpdates,
		storageUpdateGinis,
		dbRw,
	)
	return page
}

type top10SlowestCharts struct {
	times           *charts.Bar
	unfoldTimeGinis *charts.Line
	foldTimeGinis   *charts.Line
	updates         *charts.Bar
	suGinis         *charts.Line
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
	unfoldTimes := make([]opts.BarData, len(mv))
	maxUnfoldTimesPerAcc := make([]opts.BarData, len(mv))
	unfoldTimeGinis := make([]opts.LineData, len(mv))
	foldTimes := make([]opts.BarData, len(mv))
	maxFoldTimesPerAcc := make([]opts.BarData, len(mv))
	foldTimeGinis := make([]opts.LineData, len(mv))
	accs := make([]opts.BarData, len(mv))
	storage := make([]opts.BarData, len(mv))
	maxSuPerAcc := make([]opts.BarData, len(mv))
	suGinis := make([]opts.LineData, len(mv))
	for i := range mv {
		name := strconv.FormatUint(mv[i].BatchId, 10)
		nums[i] = i + 1
		times[i] = opts.LineData{Name: name, Value: mv[i].SpentProcessing.Milliseconds()}
		unfoldTimes[i] = opts.BarData{Name: name, Value: mv[i].SpentUnfolding.Milliseconds()}
		foldTimes[i] = opts.BarData{Name: name, Value: mv[i].SpentFolding.Milliseconds()}
		accs[i] = opts.BarData{Name: name, Value: mv[i].AddressKeys}
		storage[i] = opts.BarData{Name: name, Value: mv[i].StorageKeys}
		var accMaxSu, accMaxUnfoldTime, accMaxFoldTime uint64
		accSus := make([]uint64, 0, len(mv[i].Accounts))
		accUnfoldTimes := make([]uint64, 0, len(mv[i].Accounts))
		accFoldTimes := make([]uint64, 0, len(mv[i].Accounts))
		forEachAccStat(mv[i].Accounts, func(stat *commitment.AccountStats) {
			accMaxSu = max(accMaxSu, stat.StorageUpates)
			accSus = append(accSus, stat.StorageUpates)
			accMaxUnfoldTime = max(accMaxUnfoldTime, uint64(stat.SpentUnfolding.Milliseconds()))
			accUnfoldTimes = append(accUnfoldTimes, uint64(stat.SpentUnfolding.Milliseconds()))
			accMaxFoldTime = max(accMaxFoldTime, uint64(stat.SpentFolding.Milliseconds()))
			accFoldTimes = append(accFoldTimes, uint64(stat.SpentFolding.Milliseconds()))
		})
		maxSuPerAcc[i] = opts.BarData{Name: name, Value: accMaxSu}
		suGinis[i] = opts.LineData{Name: name, Value: giniCoefficient(accSus)}
		maxUnfoldTimesPerAcc[i] = opts.BarData{Name: name, Value: accMaxUnfoldTime}
		unfoldTimeGinis[i] = opts.LineData{Name: name, Value: giniCoefficient(accUnfoldTimes)}
		maxFoldTimesPerAcc[i] = opts.BarData{Name: name, Value: accMaxFoldTime}
		foldTimeGinis[i] = opts.LineData{Name: name, Value: giniCoefficient(accFoldTimes)}
	}
	timesChart := charts.NewBar()
	timesChart.SetGlobalOptions(
		subTitleOpts("top 10 slowest", "processing times (miliseconds)"),
		legendOpts(),
		gridOpts(),
	)
	timesChart.SetXAxis(nums).
		AddSeries("unfold", unfoldTimes, charts.WithLineChartOpts(opts.LineChart{Stack: "total"})).
		AddSeries("fold", foldTimes, charts.WithLineChartOpts(opts.LineChart{Stack: "total"})).
		AddSeries("maxUnfoldTimePerAcc", maxUnfoldTimesPerAcc, charts.WithLineChartOpts(opts.LineChart{Stack: "perAcc"})).
		AddSeries("maxFoldTimePerAcc", maxFoldTimesPerAcc, charts.WithLineChartOpts(opts.LineChart{Stack: "perAcc"}))
	timesLineChart := charts.NewLine()
	timesLineChart.SetXAxis("total").AddSeries("total", times)
	timesChart.Overlap(timesLineChart)
	unfoldTimeGinisChart := charts.NewLine()
	unfoldTimeGinisChart.SetGlobalOptions(
		subTitleOpts("top 10 slowest", "unfolding times gini coefficient"),
		legendOpts(),
		gridOpts(),
	)
	unfoldTimeGinisChart.SetXAxis(nums).
		AddSeries("gini", unfoldTimeGinis)
	foldTimeGinisChart := charts.NewLine()
	foldTimeGinisChart.SetGlobalOptions(
		subTitleOpts("top 10 slowest", "folding times gini coefficient"),
		legendOpts(),
		gridOpts(),
	)
	foldTimeGinisChart.SetXAxis(nums).
		AddSeries("gini", foldTimeGinis)
	updatesChart := charts.NewBar()
	updatesChart.SetGlobalOptions(
		subTitleOpts("top 10 slowest", "trie updates"),
		legendOpts(),
		gridOpts(),
	)
	updatesChart.SetXAxis(nums).
		AddSeries("accs", accs).
		AddSeries("storage", storage).
		AddSeries("maxSuPerAcc", maxSuPerAcc)
	suGinisChart := charts.NewLine()
	suGinisChart.SetGlobalOptions(
		subTitleOpts("top 10 slowest", "storage updates gini coefficient"),
		legendOpts(),
		gridOpts(),
	)
	suGinisChart.SetXAxis(nums).
		AddSeries("gini", suGinis)
	return top10SlowestCharts{
		times:           timesChart,
		unfoldTimeGinis: unfoldTimeGinisChart,
		foldTimeGinis:   foldTimeGinisChart,
		updates:         updatesChart,
		suGinis:         suGinisChart,
	}
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

func generateUnfoldTimeGinisChart(mv []MetricValues) *charts.Line {
	batchIds := make([]uint64, len(mv))
	unfoldTimeGinis := make([]opts.LineData, len(mv))
	for i := range mv {
		batchIds[i] = mv[i].BatchId
		unfoldTimes := make([]uint64, 0, len(mv[i].Accounts))
		forEachAccStat(mv[i].Accounts, func(stat *commitment.AccountStats) {
			unfoldTimes = append(unfoldTimes, uint64(stat.SpentUnfolding.Milliseconds()))
		})
		unfoldTimeGinis[i] = opts.LineData{Value: giniCoefficient(unfoldTimes)}
	}
	chart := charts.NewLine()
	chart.SetGlobalOptions(
		titleOpts("unfolding times gini coefficient"),
		legendOpts(),
		gridOpts(),
	)
	chart.SetXAxis(batchIds).
		AddSeries("gini", unfoldTimeGinis)
	return chart
}

func generateFoldTimeGinisChart(mv []MetricValues) *charts.Line {
	batchIds := make([]uint64, len(mv))
	foldTimeGinis := make([]opts.LineData, len(mv))
	for i := range mv {
		batchIds[i] = mv[i].BatchId
		foldTimes := make([]uint64, 0, len(mv[i].Accounts))
		forEachAccStat(mv[i].Accounts, func(stat *commitment.AccountStats) {
			foldTimes = append(foldTimes, uint64(stat.SpentFolding.Milliseconds()))
		})
		foldTimeGinis[i] = opts.LineData{Value: giniCoefficient(foldTimes)}
	}
	chart := charts.NewLine()
	chart.SetGlobalOptions(
		titleOpts("folding times gini coefficient"),
		legendOpts(),
		gridOpts(),
	)
	chart.SetXAxis(batchIds).
		AddSeries("gini", foldTimeGinis)
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

func generateStorageUpdateGinisChart(mv []MetricValues) *charts.Line {
	batchIds := make([]uint64, len(mv))
	suGinis := make([]opts.LineData, len(mv))
	for i := range mv {
		batchIds[i] = mv[i].BatchId
		sus := make([]uint64, 0, len(mv[i].Accounts))
		forEachAccStat(mv[i].Accounts, func(stat *commitment.AccountStats) {
			sus = append(sus, stat.StorageUpates)
		})
		suGinis[i] = opts.LineData{Value: giniCoefficient(sus)}
	}
	chart := charts.NewLine()
	chart.SetGlobalOptions(
		titleOpts("storage updates gini coefficient"),
		legendOpts(),
		gridOpts(),
	)
	chart.SetXAxis(batchIds).
		AddSeries("gini", suGinis)
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

func forEachAccStat(as map[string]*commitment.AccountStats, f func(stat *commitment.AccountStats)) {
	for addr, stat := range as {
		// note currently filtering on LoadBranch==0 as well because all rows with it are for branch keys
		// and not for accounts, and hence all their StorageUpdates are 0s messing up the real gini coefficients
		// in future may want to move the branch related metrics to a separate csv and MetricValues.Branches
		if len(addr) == hex.EncodedLen(length.Addr) && stat.LoadBranch == 0 {
			f(stat)
		}
	}
}
