package main

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"os"

	chart "github.com/wcharczuk/go-chart"
)

func statelessDoKVChart(filename string, right []int, chartFileName string, start int, startColor int) {
	file, err := os.Open(filename)
	check(err)
	defer file.Close()
	reader := csv.NewReader(bufio.NewReader(file))
	var blocks []float64
	var vals [22][]float64
	count := 0
	for records, _ := reader.Read(); len(records) == 16; records, _ = reader.Read() {
		count++
		if count < start {
			continue
		}
		blocks = append(blocks, parseFloat64(records[0])/1000000.0)
		for i := 0; i < 22; i++ {
			cProofs := 4.0*parseFloat64(records[2]) + 32.0*parseFloat64(records[3]) + parseFloat64(records[11]) + parseFloat64(records[12])
			proofs := 4.0*parseFloat64(records[7]) + 32.0*parseFloat64(records[8]) + parseFloat64(records[14]) + parseFloat64(records[15])
			switch i {
			case 1, 6:
				vals[i] = append(vals[i], 4.0*parseFloat64(records[i+1]))
			case 2, 7:
				vals[i] = append(vals[i], 32.0*parseFloat64(records[i+1]))
			case 15:
				vals[i] = append(vals[i], cProofs)
			case 16:
				vals[i] = append(vals[i], proofs)
			case 17:
				vals[i] = append(vals[i], cProofs+proofs+parseFloat64(records[13]))
			case 18:
				vals[i] = append(vals[i], 4.0*parseFloat64(records[2])+4.0*parseFloat64(records[7]))
			case 19:
				vals[i] = append(vals[i], 4.0*parseFloat64(records[2])+4.0*parseFloat64(records[7])+
					parseFloat64(records[3])+parseFloat64(records[4])+parseFloat64(records[10])+parseFloat64(records[11]))
			case 20:
				vals[i] = append(vals[i], 4.0*parseFloat64(records[2])+4.0*parseFloat64(records[7])+
					parseFloat64(records[4])+parseFloat64(records[5])+parseFloat64(records[10])+parseFloat64(records[11])+
					32.0*parseFloat64(records[3])+32.0*parseFloat64(records[8]))
			case 21:
				vals[i] = append(vals[i], 4.0*parseFloat64(records[2])+4.0*parseFloat64(records[7])+
					parseFloat64(records[4])+parseFloat64(records[5])+parseFloat64(records[10])+parseFloat64(records[11])+
					32.0*parseFloat64(records[3])+32.0*parseFloat64(records[8])+parseFloat64(records[13]))
			default:
				vals[i] = append(vals[i], parseFloat64(records[i+1]))
			}
		}
	}
	var windowSums [22]float64
	window := 1024
	var movingAvgs [22][]float64
	for i := 0; i < 22; i++ {
		movingAvgs[i] = make([]float64, len(blocks)-(window-1))
	}
	for j := 0; j < len(blocks); j++ {
		for i := 0; i < 22; i++ {
			windowSums[i] += vals[i][j]
		}
		if j >= window {
			for i := 0; i < 22; i++ {
				windowSums[i] -= vals[i][j-window]
			}
		}
		if j >= window-1 {
			for i := 0; i < 22; i++ {
				movingAvgs[i][j-window+1] = windowSums[i] / float64(window)
			}
		}
	}
	movingBlock := blocks[window-1:]
	seriesNames := [22]string{
		"Number of contracts",
		"Contract masks",
		"Contract hashes",
		"Number of contract leaf keys",
		"Number of contract leaf vals",
		"Number of contract codes",
		"Masks",
		"Hashes",
		"Number of leaf keys",
		"Number of leaf values",
		"Total size of contract leaf keys",
		"Total size of contract leaf vals",
		"Total size of codes",
		"Total size of leaf keys",
		"Total size of leaf vals",
		"Block proofs (contracts only)",
		"Block proofs (without contracts)",
		"Block proofs (total)",
		"Structure (total)",
		"Leaves (total)",
		"Hashes (total)",
		"Code (total)",
	}
	currentColor := startColor
	series := make([]chart.Series, len(right))
	for i, r := range right {
		s := &chart.ContinuousSeries{
			Name: seriesNames[r],
			Style: chart.Style{
				Show:        true,
				StrokeWidth: 0.0,
				StrokeColor: chartColors[currentColor],
				FillColor:   chartColors[currentColor],
				//FillColor:   chartColors[currentColor].WithAlpha(100),
			},
			XValues: movingBlock,
			YValues: movingAvgs[r],
		}
		currentColor++
		series[i] = s
	}

	graph1 := chart.Chart{
		Width:  1280,
		Height: 720,
		Background: chart.Style{
			Padding: chart.Box{
				Top: 50,
			},
		},
		YAxis: chart.YAxis{
			Name:      "kBytes",
			NameStyle: chart.StyleShow(),
			Style:     chart.StyleShow(),
			TickStyle: chart.Style{
				TextRotationDegrees: 45.0,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%d kB", int(v.(float64)/1024.0))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorBlack,
				StrokeWidth: 1.0,
			},
			//GridLines: days(),
		},
		/*
			YAxisSecondary: chart.YAxis{
				NameStyle: chart.StyleShow(),
				Style: chart.StyleShow(),
				TickStyle: chart.Style{
					TextRotationDegrees: 45.0,
				},
				ValueFormatter: func(v interface{}) string {
					return fmt.Sprintf("%d", int(v.(float64)))
				},
			},
		*/
		XAxis: chart.XAxis{
			Name: "Blocks, million",
			Style: chart.Style{
				Show: true,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorAlternateGray,
				StrokeWidth: 1.0,
			},
			//GridLines: blockMillions(),
		},
		Series: series,
	}

	graph1.Elements = []chart.Renderable{chart.LegendThin(&graph1)}

	buffer := bytes.NewBuffer([]byte{})
	err = graph1.Render(chart.PNG, buffer)
	check(err)
	err = ioutil.WriteFile(chartFileName, buffer.Bytes(), 0644)
	check(err)
}
