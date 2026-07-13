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

package jsonrpc

import (
	"testing"

	"github.com/erigontech/erigon/common/hexutil"
)

func witness(state, codes, keys []hexutil.Bytes) *ExecutionWitnessResult {
	return &ExecutionWitnessResult{State: state, Codes: codes, Keys: keys}
}

func hb(vals ...byte) []hexutil.Bytes {
	out := make([]hexutil.Bytes, len(vals))
	for i, v := range vals {
		out[i] = hexutil.Bytes{v}
	}
	return out
}

func TestCompareWitnessNodeSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		erigon *ExecutionWitnessResult
		reth   *ExecutionWitnessResult
		want   WitnessParity
	}{
		{
			name:   "identical sets",
			erigon: witness(hb(1, 2, 3), hb(9), hb(4, 5)),
			reth:   witness(hb(1, 2, 3), hb(9), hb(4, 5)),
			want:   WitnessParity{ByteIdentical: true, ErigonOnly: 0, RethOnly: 0, CodesEqual: true, KeysEqual: true},
		},
		{
			name:   "identical sets, different order",
			erigon: witness(hb(3, 1, 2), hb(9), hb(5, 4)),
			reth:   witness(hb(1, 2, 3), hb(9), hb(4, 5)),
			want:   WitnessParity{ByteIdentical: true, ErigonOnly: 0, RethOnly: 0, CodesEqual: true, KeysEqual: true},
		},
		{
			name:   "erigon has an extra node (red flag)",
			erigon: witness(hb(1, 2, 3, 4), hb(9), hb(4, 5)),
			reth:   witness(hb(1, 2, 3), hb(9), hb(4, 5)),
			want:   WitnessParity{ByteIdentical: false, ErigonOnly: 1, RethOnly: 0, CodesEqual: true, KeysEqual: true},
		},
		{
			name:   "reth has an extra node (benign)",
			erigon: witness(hb(1, 2, 3), hb(9), hb(4, 5)),
			reth:   witness(hb(1, 2, 3, 7), hb(9), hb(4, 5)),
			want:   WitnessParity{ByteIdentical: false, ErigonOnly: 0, RethOnly: 1, CodesEqual: true, KeysEqual: true},
		},
		{
			name:   "disjoint sets on both sides",
			erigon: witness(hb(1, 2), hb(9), hb(4, 5)),
			reth:   witness(hb(3, 4), hb(9), hb(4, 5)),
			want:   WitnessParity{ByteIdentical: false, ErigonOnly: 2, RethOnly: 2, CodesEqual: true, KeysEqual: true},
		},
		{
			name:   "differing codes flagged",
			erigon: witness(hb(1, 2, 3), hb(9), hb(4, 5)),
			reth:   witness(hb(1, 2, 3), hb(8), hb(4, 5)),
			want:   WitnessParity{ByteIdentical: true, ErigonOnly: 0, RethOnly: 0, CodesEqual: false, KeysEqual: true},
		},
		{
			name:   "differing keys flagged",
			erigon: witness(hb(1, 2, 3), hb(9), hb(4, 5)),
			reth:   witness(hb(1, 2, 3), hb(9), hb(4, 6)),
			want:   WitnessParity{ByteIdentical: true, ErigonOnly: 0, RethOnly: 0, CodesEqual: true, KeysEqual: false},
		},
		{
			name:   "duplicate nodes collapse to a set",
			erigon: witness(hb(1, 1, 2), hb(9, 9), hb(4, 4)),
			reth:   witness(hb(1, 2), hb(9), hb(4)),
			want:   WitnessParity{ByteIdentical: true, ErigonOnly: 0, RethOnly: 0, CodesEqual: true, KeysEqual: true},
		},
		{
			name:   "both empty",
			erigon: witness(nil, nil, nil),
			reth:   witness(nil, nil, nil),
			want:   WitnessParity{ByteIdentical: true, ErigonOnly: 0, RethOnly: 0, CodesEqual: true, KeysEqual: true},
		},
		{
			name:   "both nil",
			erigon: nil,
			reth:   nil,
			want:   WitnessParity{ByteIdentical: true, ErigonOnly: 0, RethOnly: 0, CodesEqual: true, KeysEqual: true},
		},
		{
			name:   "nil erigon vs populated reth",
			erigon: nil,
			reth:   witness(hb(1, 2), hb(9), hb(4)),
			want:   WitnessParity{ByteIdentical: false, ErigonOnly: 0, RethOnly: 2, CodesEqual: false, KeysEqual: false},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := CompareWitnessNodeSets(tc.erigon, tc.reth)
			if got != tc.want {
				t.Fatalf("CompareWitnessNodeSets() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestCompareWitnessNodeSetsDistinguishesByFullBytes(t *testing.T) {
	t.Parallel()

	// Two nodes sharing a prefix but differing in a later byte must be distinct set
	// members — identity is the full RLP, not a prefix.
	erigon := witness([]hexutil.Bytes{{0x01, 0x02, 0x03}}, nil, nil)
	reth := witness([]hexutil.Bytes{{0x01, 0x02, 0x04}}, nil, nil)

	got := CompareWitnessNodeSets(erigon, reth)
	if got.ByteIdentical {
		t.Fatal("nodes differing only in a trailing byte must not compare byte-identical")
	}
	if got.ErigonOnly != 1 || got.RethOnly != 1 {
		t.Fatalf("expected 1 erigon-only and 1 reth-only, got %+v", got)
	}
}
