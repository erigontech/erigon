package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"time"

	"gonum.org/v1/gonum/stat"
)

func main() {
	pathVal := flag.String("path", "", "a path to a log file")
	flag.Parse()

	if pathVal == nil {
		log.Fatal("a path is required")
		return
	}
	path := *pathVal

	f, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(f)
	defer f.Close()

	var (
		prefix        = []byte("Imported new block headers")
		countPrefix   = []byte("count=")
		elapsedPrefix = []byte("elapsed=")
	)

	var r *row
	var totalTime time.Duration
	var totalCount int

	blocksPerMs := make([]float64, 0, 2048)

	for scanner.Scan() {
		r = newRow(scanner.Bytes(), prefix, countPrefix, elapsedPrefix)
		if r != nil {
			blocksPerMs = append(blocksPerMs, r.blocksPerMs)
			totalTime += r.elapsed
			totalCount += r.count
		}
	}

	sort.Float64s(blocksPerMs)

	fmt.Println("total time", totalTime)
	fmt.Println("blocks", totalCount)
	if len(blocksPerMs) > 0 {
		fmt.Printf("min %f blk/s\n", blocksPerMs[0])
		fmt.Printf("max %f blk/s\n", blocksPerMs[len(blocksPerMs)-1])
	}
	fmt.Printf("mean %f blk/s\n", float64(totalCount)/totalTime.Seconds())
	fmt.Printf("quantile blk/s:\n"+
		"\t0.10 = %f\n\t0.25 = %f\n\t0.50 = %f\n\t0.75 = %f\n\t0.90 = %f\n\t0.95 = %f\n\t0.99 = %f\n",
		stat.Quantile(0.10, stat.Empirical, blocksPerMs, nil),
		stat.Quantile(0.25, stat.Empirical, blocksPerMs, nil),
		stat.Quantile(0.50, stat.Empirical, blocksPerMs, nil),
		stat.Quantile(0.75, stat.Empirical, blocksPerMs, nil),
		stat.Quantile(0.90, stat.Empirical, blocksPerMs, nil),
		stat.Quantile(0.95, stat.Empirical, blocksPerMs, nil),
		stat.Quantile(0.99, stat.Empirical, blocksPerMs, nil))
}

type row struct {
	count       int
	elapsed     time.Duration
	blocksPerMs float64
}

func newRow(s, hasPrefix, count, elapsed []byte) *row {
	if !bytes.Contains(s, hasPrefix) {
		return nil
	}

	idxFrom := bytes.Index(s, elapsed)
	if idxFrom == -1 {
		return nil
	}
	idxFrom += len(elapsed)
	for _, c := range s[idxFrom:] {
		if c != ' ' {
			break
		}
		idxFrom++
	}
	idxTo := bytes.IndexByte(s[idxFrom:], ' ')
	if idxTo == -1 {
		return nil
	}
	duration, err := time.ParseDuration(string(s[idxFrom : idxFrom+idxTo]))
	if err != nil {
		fmt.Println("err", err)
		return nil
	}

	r := &row{}
	r.elapsed = duration

	idxFrom = bytes.Index(s, count)
	if idxFrom == -1 {
		return nil
	}
	idxFrom += len(count)
	for _, c := range s[idxFrom:] {
		if c != ' ' {
			break
		}
		idxFrom++
	}
	idxTo = bytes.IndexByte(s[idxFrom:], ' ')
	if idxTo == -1 {
		return nil
	}
	countValue, err := strconv.Atoi(string(s[idxFrom : idxFrom+idxTo]))
	if err != nil {
		fmt.Println("err", err)
		return nil
	}

	r.count = countValue
	r.blocksPerMs = float64(r.count) / r.elapsed.Seconds()

	return r
}
