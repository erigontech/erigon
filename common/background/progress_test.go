// Copyright 2026 The Erigon Authors
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

package background

import "testing"

func TestProgressPercent(t *testing.T) {
	tests := []struct {
		name             string
		processed, total uint64
		want             int
	}{
		{name: "zero total", processed: 0, total: 0, want: 0},
		{name: "zero total with processed work", processed: 1, total: 0, want: 0},
		{name: "not started", processed: 0, total: 10, want: 0},
		{name: "partial", processed: 1, total: 4, want: 25},
		{name: "fractional", processed: 1, total: 3, want: 33},
		{name: "complete", processed: 10, total: 10, want: 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var p Progress
			p.Processed.Store(tt.processed)
			p.Total.Store(tt.total)
			if got := p.percent(); got != tt.want {
				t.Fatalf("percent() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestProgressSetZeroTotal(t *testing.T) {
	for _, processed := range []uint64{0, 1} {
		ps := NewProgressSet()
		p := ps.AddNew("job", 0)
		p.Processed.Store(processed)
		if got := ps.String(); got != "job=0%" {
			t.Errorf("processed=%d: String() = %q, want job=0%%", processed, got)
		}
		if got, ok := ps.DiagnosticsData()["job"]; !ok || got != 0 {
			t.Errorf("processed=%d: DiagnosticsData()[job] = %d, present = %t; want 0, true", processed, got, ok)
		}
	}
}
