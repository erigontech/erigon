// Copyright 2017 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/crypto"
)

func bitvecToUint64Slice(b bitvec) []uint64 {
	n := (len(b) + 7) / 8
	padded := make([]byte, n*8)
	copy(padded, b)

	res := make([]uint64, n)
	for i := 0; i < n; i++ {
		res[i] = binary.LittleEndian.Uint64(padded[i*8:])
	}
	return res
}

func TestJumpDestAnalysis(t *testing.T) {
	tests := []struct {
		code  []byte
		exp   uint64
		which int
	}{
		{[]byte{byte(PUSH1), 0x01, 0x01, 0x01}, 0x02, 0},
		{[]byte{byte(PUSH1), byte(PUSH1), byte(PUSH1), byte(PUSH1)}, 0x0a, 0},
		{[]byte{byte(PUSH8), byte(PUSH8), byte(PUSH8), byte(PUSH8), byte(PUSH8), byte(PUSH8), byte(PUSH8), byte(PUSH8), 0x01, 0x01, 0x01}, 0x01fe, 0},
		{[]byte{byte(PUSH8), 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01}, 0x01fe, 0},
		{[]byte{0x01, 0x01, 0x01, 0x01, 0x01, byte(PUSH2), byte(PUSH2), byte(PUSH2), 0x01, 0x01, 0x01}, 0xc0, 0},
		{[]byte{0x01, 0x01, 0x01, 0x01, 0x01, byte(PUSH2), 0x01, 0x01, 0x01, 0x01, 0x01}, 0xc0, 0},
		{[]byte{byte(PUSH3), 0x01, 0x01, 0x01, byte(PUSH1), 0x01, 0x01, 0x01, 0x01, 0x01, 0x01}, 0x2e, 0},
		{[]byte{0x01, byte(PUSH8), 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01}, 0x03fc, 0},
		{[]byte{byte(PUSH16), 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01}, 0x01fffe, 0},
		{[]byte{byte(PUSH8), 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, byte(PUSH1), 0x01}, 0x05fe, 0},
		{[]byte{byte(PUSH32)}, 0x01fffffffe, 0},
	}
	for _, test := range tests {
		ret := codeBitmap(test.code)
		if ret[test.which] != test.exp {
			t.Fatalf("expected %x, got %02x", test.exp, ret[test.which])
		}
		retEof := bitvecToUint64Slice(eofCodeBitmap(test.code))
		if retEof[test.which] != test.exp {
			t.Fatalf("eof expected %x, got %02x", test.exp, retEof[test.which])
		}
	}
}

func TestEOFAnalysis(t *testing.T) {
	tests := []struct {
		code  []byte
		exp   byte
		which int
	}{
		{[]byte{byte(RJUMP), 0x01, 0x01, 0x01}, 0b0000_0110, 0},
		{[]byte{byte(RJUMPI), byte(RJUMP), byte(RJUMP), byte(RJUMPI)}, 0b0011_0110, 0},
		{[]byte{byte(RJUMPV), 0x02, byte(RJUMP), 0x00, byte(RJUMPI), 0x00}, 0b0011_1110, 0},
	}
	for i, test := range tests {
		ret := eofCodeBitmap(test.code)
		if ret[test.which] != test.exp {
			t.Fatalf("test %d: expected %x, got %02x", i, test.exp, ret[test.which])
		}
	}
}

func BenchmarkJumpdestAnalysisEmpty_1200k(bench *testing.B) {
	// 1.4 ms
	code := make([]byte, 1200000)
	bench.ResetTimer()
	for i := 0; i < bench.N; i++ {
		codeBitmap(code)
	}
	bench.StopTimer()
}

func BenchmarkJumpdestAnalysis_1200k(bench *testing.B) {
	code := common.Hex2Bytes("6060604052361561006c5760e060020a600035046308551a53811461007457806335a063b4146100865780633fa4f245146100a6578063590e1ae3146100af5780637150d8ae146100cf57806373fac6f0146100e1578063c19d93fb146100fe578063d696069714610112575b610131610002565b610133600154600160a060020a031681565b610131600154600160a060020a0390811633919091161461015057610002565b61014660005481565b610131600154600160a060020a039081163391909116146102d557610002565b610133600254600160a060020a031681565b610131600254600160a060020a0333811691161461023757610002565b61014660025460ff60a060020a9091041681565b61013160025460009060ff60a060020a9091041681146101cc57610002565b005b600160a060020a03166060908152602090f35b6060908152602090f35b60025460009060a060020a900460ff16811461016b57610002565b600154600160a060020a03908116908290301631606082818181858883f150506002805460a060020a60ff02191660a160020a179055506040517f72c874aeff0b183a56e2b79c71b46e1aed4dee5e09862134b8821ba2fddbf8bf9250a150565b80546002023414806101dd57610002565b6002805460a060020a60ff021973ffffffffffffffffffffffffffffffffffffffff1990911633171660a060020a1790557fd5d55c8a68912e9a110618df8d5e2e83b8d83211c57a8ddd1203df92885dc881826060a15050565b60025460019060a060020a900460ff16811461025257610002565b60025460008054600160a060020a0390921691606082818181858883f150508354604051600160a060020a0391821694503090911631915082818181858883f150506002805460a060020a60ff02191660a160020a179055506040517fe89152acd703c9d8c7d28829d443260b411454d45394e7995815140c8cbcbcf79250a150565b60025460019060a060020a900460ff1681146102f057610002565b6002805460008054600160a060020a0390921692909102606082818181858883f150508354604051600160a060020a0391821694503090911631915082818181858883f150506002805460a060020a60ff02191660a160020a179055506040517f8616bbbbad963e4e65b1366f1d75dfb63f9e9704bbbf91fb01bec70849906cf79250a15056")
	bench.ResetTimer()
	for i := 0; i < bench.N; i++ {
		codeBitmap(code)
	}
	bench.StopTimer()
}

func BenchmarkJumpdestHashing_1200k(bench *testing.B) {
	// 4 ms
	code := make([]byte, 1200000)
	bench.ResetTimer()
	for i := 0; i < bench.N; i++ {
		crypto.Keccak256Hash(code)
	}
	bench.StopTimer()
}

func BenchmarkJumpDest(b *testing.B) {
	code := common.Hex2Bytes("6060604052361561006c5760e060020a600035046308551a53811461007457806335a063b4146100865780633fa4f245146100a6578063590e1ae3146100af5780637150d8ae146100cf57806373fac6f0146100e1578063c19d93fb146100fe578063d696069714610112575b610131610002565b610133600154600160a060020a031681565b610131600154600160a060020a0390811633919091161461015057610002565b61014660005481565b610131600154600160a060020a039081163391909116146102d557610002565b610133600254600160a060020a031681565b610131600254600160a060020a0333811691161461023757610002565b61014660025460ff60a060020a9091041681565b61013160025460009060ff60a060020a9091041681146101cc57610002565b005b600160a060020a03166060908152602090f35b6060908152602090f35b60025460009060a060020a900460ff16811461016b57610002565b600154600160a060020a03908116908290301631606082818181858883f150506002805460a060020a60ff02191660a160020a179055506040517f72c874aeff0b183a56e2b79c71b46e1aed4dee5e09862134b8821ba2fddbf8bf9250a150565b80546002023414806101dd57610002565b6002805460a060020a60ff021973ffffffffffffffffffffffffffffffffffffffff1990911633171660a060020a1790557fd5d55c8a68912e9a110618df8d5e2e83b8d83211c57a8ddd1203df92885dc881826060a15050565b60025460019060a060020a900460ff16811461025257610002565b60025460008054600160a060020a0390921691606082818181858883f150508354604051600160a060020a0391821694503090911631915082818181858883f150506002805460a060020a60ff02191660a160020a179055506040517fe89152acd703c9d8c7d28829d443260b411454d45394e7995815140c8cbcbcf79250a150565b60025460019060a060020a900460ff1681146102f057610002565b6002805460008054600160a060020a0390921692909102606082818181858883f150508354604051600160a060020a0391821694503090911631915082818181858883f150506002805460a060020a60ff02191660a160020a179055506040517f8616bbbbad963e4e65b1366f1d75dfb63f9e9704bbbf91fb01bec70849906cf79250a15056")
	pc := new(uint256.Int)
	hash := common.Hash{1, 2, 3, 4, 5}

	contractRef := dummyContractRef{}

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		contract := NewContract(contractRef, contractRef, nil, 0, false /* skipAnalysis */)
		contract.Code = code
		contract.CodeHash = hash

		b.StartTimer()
		for i := range contract.Code {
			contract.validJumpdest(pc.SetUint64(uint64(i)))
		}
		b.StopTimer()
	}
}

func TestCodeAnalysis(t *testing.T) {
	for _, tc := range []string{
		"5e30303030",
	} {
		eofCodeBitmap(common.FromHex(tc))
		codeBitmap(common.FromHex(tc))
	}
}

func FuzzCodeAnalysis(f *testing.F) {
	f.Add(common.FromHex("5e30303030"))
	f.Fuzz(func(t *testing.T, data []byte) {
		eofCodeBitmap(data)
		codeBitmap(data)
	})
}
