// Copyright 2016 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package types

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"

	"github.com/davecgh/go-spew/spew"
)

var unmarshalLogTests = map[string]struct {
	input     string
	want      *Log
	wantError error
}{
	"ok": {
		input: `{"address":"0xecf8f87f810ecf450940c9f60066b4a7a501d6a7","blockHash":"0x656c34545f90a730a19008c0e7a7cd4fb3895064b48d6d69761bd5abad681056","blockNumber":"0x1ecfa4","timestamp":"0x57a53d3a","data":"0x000000000000000000000000000000000000000000000001a055690d9db80000","logIndex":"0x2","topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef","0x00000000000000000000000080b2c9d7cbbf30a1b0fc8983c647d754c6525615"],"transactionHash":"0x3b198bfd5d2907285af009e9ae84a0ecd63677110d89d7e030251acb87f6487e","transactionIndex":"0x3"}`,
		want: &Log{
			Address:     common.HexToAddress("0xecf8f87f810ecf450940c9f60066b4a7a501d6a7"),
			BlockHash:   common.HexToHash("0x656c34545f90a730a19008c0e7a7cd4fb3895064b48d6d69761bd5abad681056"),
			BlockNumber: 2019236,
			Data:        hexutil.MustDecode("0x000000000000000000000000000000000000000000000001a055690d9db80000"),
			Index:       2,
			TxIndex:     3,
			TxHash:      common.HexToHash("0x3b198bfd5d2907285af009e9ae84a0ecd63677110d89d7e030251acb87f6487e"),
			Topics: []common.Hash{
				common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
				common.HexToHash("0x00000000000000000000000080b2c9d7cbbf30a1b0fc8983c647d754c6525615"),
			},
		},
	},

	"empty data": {
		input: `{"address":"0xecf8f87f810ecf450940c9f60066b4a7a501d6a7","blockHash":"0x656c34545f90a730a19008c0e7a7cd4fb3895064b48d6d69761bd5abad681056","blockNumber":"0x1ecfa4","timestamp":"0x57a53d3a","data":"0x","logIndex":"0x2","topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef","0x00000000000000000000000080b2c9d7cbbf30a1b0fc8983c647d754c6525615"],"transactionHash":"0x3b198bfd5d2907285af009e9ae84a0ecd63677110d89d7e030251acb87f6487e","transactionIndex":"0x3"}`,
		want: &Log{
			Address:     common.HexToAddress("0xecf8f87f810ecf450940c9f60066b4a7a501d6a7"),
			BlockHash:   common.HexToHash("0x656c34545f90a730a19008c0e7a7cd4fb3895064b48d6d69761bd5abad681056"),
			BlockNumber: 2019236,
			Data:        []byte{},
			Index:       2,
			TxIndex:     3,
			TxHash:      common.HexToHash("0x3b198bfd5d2907285af009e9ae84a0ecd63677110d89d7e030251acb87f6487e"),
			Topics: []common.Hash{
				common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
				common.HexToHash("0x00000000000000000000000080b2c9d7cbbf30a1b0fc8983c647d754c6525615"),
			},
		},
	},
	"missing block fields (pending logs)": {
		input: `{"address":"0xecf8f87f810ecf450940c9f60066b4a7a501d6a7","data":"0x","logIndex":"0x0","topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],"transactionHash":"0x3b198bfd5d2907285af009e9ae84a0ecd63677110d89d7e030251acb87f6487e","transactionIndex":"0x3"}`,
		want: &Log{
			Address:     common.HexToAddress("0xecf8f87f810ecf450940c9f60066b4a7a501d6a7"),
			BlockHash:   common.Hash{},
			BlockNumber: 0,

			Data:    []byte{},
			Index:   0,
			TxIndex: 3,
			TxHash:  common.HexToHash("0x3b198bfd5d2907285af009e9ae84a0ecd63677110d89d7e030251acb87f6487e"),
			Topics: []common.Hash{
				common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
			},
		},
	},
	"Removed: true": {
		input: `{"address":"0xecf8f87f810ecf450940c9f60066b4a7a501d6a7","blockHash":"0x656c34545f90a730a19008c0e7a7cd4fb3895064b48d6d69761bd5abad681056","blockNumber":"0x1ecfa4","timestamp":"0x57a53d3a","data":"0x","logIndex":"0x2","topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],"transactionHash":"0x3b198bfd5d2907285af009e9ae84a0ecd63677110d89d7e030251acb87f6487e","transactionIndex":"0x3","removed":true}`,
		want: &Log{
			Address:     common.HexToAddress("0xecf8f87f810ecf450940c9f60066b4a7a501d6a7"),
			BlockHash:   common.HexToHash("0x656c34545f90a730a19008c0e7a7cd4fb3895064b48d6d69761bd5abad681056"),
			BlockNumber: 2019236,

			Data:    []byte{},
			Index:   2,
			TxIndex: 3,
			TxHash:  common.HexToHash("0x3b198bfd5d2907285af009e9ae84a0ecd63677110d89d7e030251acb87f6487e"),
			Topics: []common.Hash{
				common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
			},
			Removed: true,
		},
	},
	"missing data": {
		input:     `{"address":"0xecf8f87f810ecf450940c9f60066b4a7a501d6a7","blockHash":"0x656c34545f90a730a19008c0e7a7cd4fb3895064b48d6d69761bd5abad681056","blockNumber":"0x1ecfa4","timestamp":"0x57a53d3a","logIndex":"0x2","topics":["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef","0x00000000000000000000000080b2c9d7cbbf30a1b0fc8983c647d754c6525615","0x000000000000000000000000f9dff387dcb5cc4cca5b91adb07a95f54e9f1bb6"],"transactionHash":"0x3b198bfd5d2907285af009e9ae84a0ecd63677110d89d7e030251acb87f6487e","transactionIndex":"0x3"}`,
		wantError: errors.New("missing required field 'data' for Log"),
	},
}

func TestUnmarshalLog(t *testing.T) {
	t.Parallel()
	dumper := spew.ConfigState{DisableMethods: true, Indent: "    "}
	for name, test := range unmarshalLogTests {
		var log *Log
		err := json.Unmarshal([]byte(test.input), &log)
		checkError(t, name, err, test.wantError)
		if test.wantError == nil && err == nil {
			if !reflect.DeepEqual(log, test.want) {
				t.Errorf("test %q:\nGOT %sWANT %s", name, dumper.Sdump(log), dumper.Sdump(test.want))
			}
		}
	}
}

func checkError(t *testing.T, testname string, got, want error) bool {
	t.Helper()

	if got == nil {
		if want != nil {
			t.Errorf("test %q: got no error, want %q", testname, want)
			return false
		}
		return true
	}
	if want == nil {
		t.Errorf("test %q: unexpected error %q", testname, got)
	} else if got.Error() != want.Error() {
		t.Errorf("test %q: got error %q, want %q", testname, got, want)
	}
	return false
}

func TestFilterLogsTopics(t *testing.T) {
	t.Parallel()
	// hashes and addresses to make test more readable
	var (
		A common.Hash = [32]byte{1}
		B common.Hash = [32]byte{2}
		C common.Hash = [32]byte{3}
		D common.Hash = [32]byte{4}
		E common.Hash = [32]byte{5}
		F common.Hash = [32]byte{6}

		a1 common.Address = [20]byte{1}
		a2 common.Address = [20]byte{2}
		a3 common.Address = [20]byte{3}
		a4 common.Address = [20]byte{4}
		a5 common.Address = [20]byte{5}
		a6 common.Address = [20]byte{6}
	)

	type filterLogTest struct {
		input  Logs             // logs, each with an address and slice of topics
		filter [][]common.Hash  // the topic filter we want to use
		want   []common.Address // slice of addresses that should pass the filter
	}
	var basicSet = Logs{
		{
			Address: a1,
			Topics:  []common.Hash{F, F, F, F, F, B, B},
		},
		{
			Address: a2,
			Topics:  []common.Hash{A, B, F, F, F, A, B},
		},
		{
			Address: a3,
			Topics:  []common.Hash{B, A},
		},
		{
			Address: a4,
			Topics:  []common.Hash{C, D, A, D, E},
		},
		{
			Address: a5,
			Topics:  []common.Hash{C, B, C, A, E},
		},
		{
			Address: a6,
			Topics:  []common.Hash{F, F, F, D},
		},
	}
	var filterLogTests = map[string]filterLogTest{
		"1. no topics, should return all topics": {
			input:  basicSet,
			filter: [][]common.Hash{},
			want:   []common.Address{a1, a2, a3, a4, a5, a6},
		},
		"2. three empty topics, should return all topics other than a3": {
			input:  basicSet,
			filter: [][]common.Hash{{}, {}, {}},
			want:   []common.Address{a1, a2, a4, a5, a6},
		},
		"3. filter for hash A in slot 0 should only be a2": {
			input:  basicSet,
			filter: [][]common.Hash{{A}},
			want:   []common.Address{a2},
		},
		"4. filter for hash B in slot 0 should be a2 and a5": {
			input:  basicSet,
			filter: [][]common.Hash{{}, {B}},
			want:   []common.Address{a2, a5},
		},
		"5. filter for hash C in slot 0 and hash B in slot 1 should be a2, a4, and a5": {
			input:  basicSet,
			filter: [][]common.Hash{{C}, {B}},
			want:   []common.Address{a5},
		},
		"6. {{A, B}, {C, D}} to match log entry {A, D}": {
			input:  Logs{{Address: a1, Topics: []common.Hash{A, D}}},
			filter: [][]common.Hash{{A, B}, {C, D}},
			want:   []common.Address{a1},
		},
		"7. filter for hashes [B,B,B...,A,B] in slot 3 and hashes [D, C] in slot 4 should be a4 ": {
			input:  basicSet,
			filter: [][]common.Hash{{}, {}, {B, B, B, B, B, B, B, B, B, B, B, A, B}, {D, C}},
			want:   []common.Address{a4},
		},
		`8. filter for hashes
		[F] in slot 0, [F, C] in slot 1, [F, B] in slot 2, [F, C, B] in slot3, [B] in slot 5, and [] in slot 6
		should be a1 only`: {
			input:  basicSet,
			filter: [][]common.Hash{{F}, {F, C}, {F, B}, {B, F, C}, {}, {B}, {}},
			want:   []common.Address{a1},
		},
	}
	for name, v := range filterLogTests {
		ares := testFLExtractAddress(v.input.Filter(map[common.Address]struct{}{}, v.filter, 0))
		if !reflect.DeepEqual(ares, v.want) {
			t.Errorf("Fail %s, got %v want %v", name, ares, v.want)
		}
		old_res := testFLExtractAddress(v.input.FilterOld(map[common.Address]struct{}{}, v.filter))
		if !reflect.DeepEqual(old_res, v.want) {
			t.Errorf("Fail Old %s, got %v want %v", name, old_res, v.want)
		}
	}
}

func testFLExtractAddress(xs Logs) (o []common.Address) {
	for _, v := range xs {
		o = append(o, v.Address)
	}
	return
}
