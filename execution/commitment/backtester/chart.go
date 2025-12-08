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

func renderChartsPage(md []MetricValues, outputDir string) (string, error) {
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
	err = generateChartsPage(md).Render(bufio.NewWriter(f))
	if err != nil {
		return "", err
	}
	return f.Name(), nil
}

func generateChartsPage(md []MetricValues) *components.Page {
	page := components.NewPage()
	page.AddCharts(
		generateUpdatesBarChart(md),
	)
	return page
}

func generateUpdatesBarChart(md []MetricValues) *charts.Bar {
	bar := charts.NewBar()
	bar.SetGlobalOptions(charts.WithTitleOpts(opts.Title{Title: "trie updates"}))
	bar.SetXAxis(gatherBatchIds(md)).
		AddSeries("updates", gatherTotalUpdatesBarData(md)).
		AddSeries("account", gatherAccountUpdatesBarData(md)).
		AddSeries("storage", gatherStorageUpdatesBarData(md))
	return bar
}

func gatherBatchIds(md []MetricValues) []uint64 {
	data := make([]uint64, len(md))
	for i := range md {
		data[i] = md[i].BatchId
	}
	return data
}

func gatherTotalUpdatesBarData(md []MetricValues) []opts.BarData {
	data := make([]opts.BarData, len(md))
	for i := range md {
		data[i] = opts.BarData{Value: md[i].Updates}
	}
	return data
}

func gatherAccountUpdatesBarData(md []MetricValues) []opts.BarData {
	data := make([]opts.BarData, len(md))
	for i := range md {
		data[i] = opts.BarData{Value: md[i].Accounts}
	}
	return data
}

func gatherStorageUpdatesBarData(md []MetricValues) []opts.BarData {
	data := make([]opts.BarData, len(md))
	for i := range md {
		data[i] = opts.BarData{Value: md[i].StorageKeys}
	}
	return data
}
