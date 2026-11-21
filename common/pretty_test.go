package common

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
)

func TestPrettyCounterSigned(t *testing.T) {
	tests := []struct {
		num int64
		out string
	}{
		{num: math.MinInt64, out: "-9223.37Q"},
		{num: math.MinInt64 + 1, out: "-9223.37Q"},
		{num: -4356854, out: "-4.35M"},
		{num: -1500, out: "-1.50k"},
		{num: -1000, out: "-1.00k"},
		{num: -999, out: "-999"},
		{num: -5, out: "-5"},
		{num: -1, out: "-1"},
		{num: 0, out: "0"},
		{num: 1, out: "1"},
		{num: 999, out: "999"},
		{num: 1000, out: "1.00k"},
		{num: math.MaxInt64, out: "9223.37Q"},
	}

	for _, test := range tests {
		if got := PrettyCounter(test.num); got != test.out {
			t.Errorf("PrettyCounter(int64(%d)) = %s, want %s", test.num, got, test.out)
		}
	}
}

func TestPrettyCounterNegativeFloat(t *testing.T) {
	tests := []struct {
		num float64
		out string
	}{
		{num: -4356854, out: "-4.35M"},
		{num: -1000.5, out: "-1.00k"},
		{num: -2.7, out: "-2"},
		{num: -1, out: "-1"},
		{num: -0.99, out: "-0.99"},
		{num: -0.5, out: "-0.50"},
		{num: 0.5, out: "0.50"},
	}

	for _, test := range tests {
		if got := PrettyCounter(test.num); got != test.out {
			t.Errorf("PrettyCounter(float64(%v)) = %s, want %s", test.num, got, test.out)
		}
	}
}

func TestPrettyExact(t *testing.T) {
	tests := []struct {
		num uint64
		out string
	}{
		{num: 0, out: "0"},
		{num: 1, out: "1"},
		{num: 999, out: "999"},
		{num: 1000, out: "1000"},
		{num: 9999, out: "9999"},
		{num: 10000, out: "10.000"},
		{num: 100000, out: "100.000"},
		{num: 999999, out: "999.999"},
		{num: 1000000, out: "1.000.000"},
		{num: 1005000, out: "1.005.000"},
		{num: 4356854, out: "4.356.854"},
		{num: 14866176, out: "14.866.176"},
		{num: 23847119, out: "23.847.119"},
		{num: math.MaxUint64, out: "18.446.744.073.709.551.615"},
	}

	for _, test := range tests {
		if got := PrettyExact(test.num); got != test.out {
			t.Errorf("PrettyExact(%d) = %s, want %s", test.num, got, test.out)
		}
	}
}

// The point of the helper is that the operator can read the original value back out of the log.
func TestPrettyExactRoundTrips(t *testing.T) {
	nums := []uint64{0, 1, 999, 1000, 9999, 10000, math.MaxUint64}
	for i := range uint64(20000) {
		nums = append(nums, i, math.MaxUint64-i)
	}
	for _, n := range nums {
		got, err := strconv.ParseUint(strings.ReplaceAll(PrettyExact(n), ".", ""), 10, 64)
		if err != nil {
			t.Fatalf("PrettyExact(%d) = %q does not parse back: %v", n, PrettyExact(n), err)
		}
		if got != n {
			t.Fatalf("PrettyExact(%d) round-tripped to %d", n, got)
		}
	}
}

// prettyCounterBeforeSignFix is the implementation as it stood before negative inputs were
// handled, kept verbatim so the sign fix can be proven to leave non-negative output untouched.
func prettyCounterBeforeSignFix[N number](num N) string {
	if num < N(numK) {
		if num < 1 && num > 0 {
			return fmt.Sprintf("%.2f", float64(num))
		}
		return fmt.Sprintf("%d", uint64(num))
	}
	if num < N(numM) {
		return fmt.Sprintf("%d.%02dk", uint64(num)/numK, (uint64(num)%numK)/10)
	}
	if num < N(numG) {
		return fmt.Sprintf("%d.%02dM", uint64(num)/numM, (uint64(num)%numM)/(numK*10))
	}
	if num < N(numT) {
		return fmt.Sprintf("%d.%02dG", uint64(num)/numG, (uint64(num)%numG)/(numM*10))
	}
	if num < N(numQ) {
		return fmt.Sprintf("%d.%02dT", uint64(num)/numT, (uint64(num)%numT)/(numG*10))
	}
	return fmt.Sprintf("%d.%02dQ", uint64(num)/numQ, (uint64(num)%numQ)/(numT*10))
}

// The sign fix must be invisible to every existing call site. NaN and +Inf are excluded: both
// were already garbage before the fix and remain so, just differently shaped.
func TestPrettyCounterUnchangedForNonNegative(t *testing.T) {
	var uints []uint64
	for _, decade := range []uint64{0, numK, numM, numG, numT, numQ, math.MaxUint64} {
		for _, d := range []uint64{0, 1, 9, 99, 499, 500, 501, 999, 12345} {
			if decade >= d {
				uints = append(uints, decade-d)
			}
			if math.MaxUint64-decade >= d {
				uints = append(uints, decade+d)
			}
		}
	}
	for i := range uint64(5000) {
		uints = append(uints, i)
	}
	for _, n := range uints {
		if got, want := PrettyCounter(n), prettyCounterBeforeSignFix(n); got != want {
			t.Fatalf("PrettyCounter(uint64(%d)) = %s, was %s", n, got, want)
		}
	}

	for _, f := range []float64{0, 0.001, 0.5, 0.99, 1, 1.5, 999.99, 1000, 1000.5, 4356854, 1e12, 1e15, 1e18} {
		if got, want := PrettyCounter(f), prettyCounterBeforeSignFix(f); got != want {
			t.Fatalf("PrettyCounter(float64(%v)) = %s, was %s", f, got, want)
		}
	}
}

func TestPrettyCounter(t *testing.T) {
	tests := []struct {
		num uint64
		out string
	}{
		{num: 1, out: "1"},
		{num: 10, out: "10"},
		{num: 100, out: "100"},
		{num: 1000, out: "1.00k"},
		{num: 12000, out: "12.00k"},
		{num: 130400, out: "130.40k"},
		{num: 1000000, out: "1.00M"},
		{num: 10500000, out: "10.50M"},
		{num: 100000000, out: "100.00M"},
		{num: 1000000000, out: "1.00G"},
		{num: 10000000000, out: "10.00G"},
		{num: 100000000000, out: "100.00G"},
		{num: 1790000000000, out: "1.79T"},
		{num: 10000000000000, out: "10.00T"},
		{num: 100080000000000, out: "100.08T"},
		{num: 9000000000000000, out: "9.00Q"},
		{num: 12000000000000000, out: "12.00Q"},
		{num: 100020000000000000, out: "100.02Q"},
		{num: 1000240000000000000, out: "1000.24Q"},
	}

	for _, test := range tests {
		if got := PrettyCounter(test.num); got != test.out {
			t.Errorf("PrettyCounter(%d) = %s, want %s", test.num, got, test.out)
		}
	}
}
