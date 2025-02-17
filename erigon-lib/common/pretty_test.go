package common

import "testing"

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
		{num: 1000000000, out: "1.00B"},
		{num: 10000000000, out: "10.00B"},
		{num: 100000000000, out: "100.00B"},
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
