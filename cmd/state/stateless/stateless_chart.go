package stateless

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"

	chart "github.com/wcharczuk/go-chart"
)

func MakeChart(filename string, right []int, chartFileName string, start uint64, startColor int) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	columnNames, vals, err := readCsv(file, start)
	if err != nil {
		return err
	}

	window := 1024
	blockNr := 0

	movingAvgs := dataToMovingAvgs(vals[blockNr+1:], window)
	movingBlock := vals[blockNr][window-1:]

	graph1 := makeChart(movingBlock, movingAvgs, columnNames[blockNr+1:], startColor, right)

	buffer := bytes.NewBuffer([]byte{})
	if err = graph1.Render(chart.PNG, buffer); err != nil {
		return err
	}
	if err := ioutil.WriteFile(chartFileName, buffer.Bytes(), 0644); err != nil {
		return err
	}
	fmt.Printf("%s\n", chartFileName)
	return nil
}

func seriesToMovingAvgs(data []float64, window int) []float64 {
	if window < 1 {
		panic("window should be >= 1")
	}

	if len(data) < window {
		fmt.Fprintf(os.Stderr, "len of data is too short for sliding avgs: %d, will use raw data\n", len(data))
		window = 1
	}

	result := make([]float64, len(data)-(window-1))

	sum := float64(0)

	for i := 0; i < len(result); i++ {
		if i == 0 {
			for j := 0; j < window; j++ {
				sum += data[j]
			}
		} else {
			sum += data[window-1+i]
			sum -= data[i]
		}
		result[i] = sum / float64(window)
	}

	return result
}

func dataToMovingAvgs(data [][]float64, window int) [][]float64 {
	result := make([][]float64, len(data))

	for i := 0; i < len(data); i++ {
		result[i] = seriesToMovingAvgs(data[i], window)
	}

	return result
}

func readCsv(r io.Reader, from uint64) (header []string, data [][]float64, err error) {
	reader := csv.NewReader(bufio.NewReader(r))

	header, err = reader.Read()
	if err != nil {
		return
	}

	current := uint64(0)

	data = make([][]float64, len(header))

	for records, _ := reader.Read(); len(records) == len(header); records, _ = reader.Read() {
		current++
		if current < from {
			continue
		}
		for i := 0; i < len(records); i++ {
			var r float64
			if r, err = strconv.ParseFloat(records[i], 64); err != nil {
				return
			}

			if data[i] == nil {
				data[i] = make([]float64, 0)
			}

			data[i] = append(data[i], r)
		}
	}

	return header, data, err
}

func makeChart(xValues []float64, yValues [][]float64, columnNames []string, startColor int, filter []int) chart.Chart {
	currentColor := startColor

	if len(filter) == 0 {
		// include everything
		filter = make([]int, len(yValues))
		for i := 0; i < len(filter); i++ {
			filter[i] = i
		}
	}

	series := make([]chart.Series, len(filter))

	for i, r := range filter {
		if r < 0 || r >= len(yValues) {
			fmt.Printf("invalid filter value found: %d, ignoring\n", r)
			continue
		}

		s := &chart.ContinuousSeries{
			Name: columnNames[r],
			Style: chart.Style{
				Show:        true,
				StrokeWidth: 0.0,
				StrokeColor: chartColors[currentColor],
			},
			XValues: xValues,
			YValues: yValues[r],
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
		},
		XAxis: chart.XAxis{
			Name: "Blocks, million",
			Style: chart.Style{
				Show: true,
			},
			ValueFormatter: func(v interface{}) string {
				return fmt.Sprintf("%.3fm", v.(float64)/float64(1*1000*1000))
			},
			GridMajorStyle: chart.Style{
				Show:        true,
				StrokeColor: chart.ColorAlternateGray,
				StrokeWidth: 1.0,
			},
		},
		Series: series,
	}

	graph1.Elements = []chart.Renderable{chart.LegendThin(&graph1)}

	return graph1
}
