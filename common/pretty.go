package common

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type number interface {
	int | int64 | uint | uint64 | float64
}

const (
	numK = 1_000
	numM = numK * numK
	numG = numM * numK
	numT = numG * numK
	numQ = numT * numK
)

// PrettyCounter print counter number in human readable format
func PrettyCounter[N number](num N) string {
	if num < 0 {
		// float64 is the only comparison that compiles against the unsigned members of the type set
		if f := float64(num); f > -1 {
			return fmt.Sprintf("%.2f", f)
		}
		return "-" + prettyCounter(uint64(-num))
	}
	if num < 1 && num > 0 {
		return fmt.Sprintf("%.2f", float64(num))
	}
	return prettyCounter(uint64(num))
}

const digitGroupSep = '.'

// PrettyExact prints num in full, grouping digits from five digits up: 4356854 -> "4.356.854".
// Use it for values an operator reads back out of the log - block numbers, key totals in a
// report. Use PrettyCounter when only the order of magnitude matters.
func PrettyExact(num uint64) string {
	d := strconv.FormatUint(num, 10)
	if len(d) < 5 {
		return d
	}
	var b strings.Builder
	b.Grow(len(d) + (len(d)-1)/3)
	for i := range len(d) {
		if i > 0 && (len(d)-i)%3 == 0 {
			b.WriteByte(digitGroupSep)
		}
		b.WriteByte(d[i])
	}
	return b.String()
}

func prettyCounter(num uint64) string {
	if num < numK {
		return strconv.FormatUint(num, 10)
	}
	if num < numM {
		// sequence %02d does not always print 2 first digits but prints whole value so we have to divide by expected /100th part
		return fmt.Sprintf("%d.%02dk", num/numK, (num%numK)/10)
	}
	if num < numG {
		return fmt.Sprintf("%d.%02dM", num/numM, (num%numM)/(numK*10))
	}
	if num < numT {
		return fmt.Sprintf("%d.%02dG", num/numG, (num%numG)/(numM*10))
	}
	if num < numQ {
		return fmt.Sprintf("%d.%02dT", num/numT, (num%numT)/(numG*10))
	}
	return fmt.Sprintf("%d.%02dQ", num/numQ, (num%numQ)/(numT*10))
}

var divs = []time.Duration{
	time.Duration(1), time.Duration(10), time.Duration(100), time.Duration(1000)}

func round(d time.Duration, digits int) time.Duration {
	switch {
	case d > time.Second:
		d = d.Round(time.Second / divs[digits])
	case d > time.Millisecond:
		d = d.Round(time.Millisecond / divs[digits])
	case d > time.Microsecond:
		d = d.Round(time.Microsecond / divs[digits])
	}
	return d
}

func Round(d time.Duration, digits int) time.Duration {
	return round(d, digits)
}
