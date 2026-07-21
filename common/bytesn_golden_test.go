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

package common

import (
	"database/sql/driver"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	mrand "math/rand"
	randv2 "math/rand/v2"
	"testing"
)

var fixedBytesGoldens = map[string]map[string]string{
	"Bytes4": {
		"String":               "0x01020304",
		"MarshalText":          "0x01020304",
		"Value":                "01020304",
		"%v":                   "0x01020304",
		"%s":                   "0x01020304",
		"%q":                   "\"0x01020304\"",
		"%x":                   "01020304",
		"%X":                   "01020304",
		"%#x":                  "0x01020304",
		"%#X":                  "0X01020304",
		"%d":                   "[1 2 3 4]",
		"%t":                   "%!t(hash=01020304)",
		"Hex":                  "0x01020304",
		"TerminalString":       "01020304",
		"SetBytesShort":        "00deadbe",
		"SetBytesLong":         "06070809",
		"UnmarshalTextErr":     "hex string has length 4, want 8 for Bytes4",
		"UnmarshalJSONErrType": "common.Bytes4",
		"Generate":             "010203dd",
	},
	"Bytes48": {
		"String":               "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f30",
		"MarshalText":          "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f30",
		"Value":                "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f30",
		"%v":                   "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f30",
		"%s":                   "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f30",
		"%q":                   "\"0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f30\"",
		"%x":                   "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f30",
		"%X":                   "0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F202122232425262728292A2B2C2D2E2F30",
		"%#x":                  "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f30",
		"%#X":                  "0X0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F202122232425262728292A2B2C2D2E2F30",
		"%d":                   "[1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48]",
		"%t":                   "%!t(hash=0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f30)",
		"Hex":                  "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f30",
		"TerminalString":       "010203…2e2f30",
		"SetBytesShort":        "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000deadbe",
		"SetBytesLong":         "060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435",
		"UnmarshalTextErr":     "hex string has length 4, want 96 for Bytes48",
		"UnmarshalJSONErrType": "common.Bytes48",
		"Generate":             "0102030405060708090a0b0c0d0e0ff6b16be9bf8ac39fba6a4f2665389c9b5ce0b0eee5f5a2f97870cc80e9897f5add",
	},
	"Bytes64": {
		"String":               "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40",
		"MarshalText":          "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40",
		"Value":                "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40",
		"%v":                   "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40",
		"%s":                   "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40",
		"%q":                   "\"0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40\"",
		"%x":                   "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40",
		"%X":                   "0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F202122232425262728292A2B2C2D2E2F303132333435363738393A3B3C3D3E3F40",
		"%#x":                  "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40",
		"%#X":                  "0X0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F202122232425262728292A2B2C2D2E2F303132333435363738393A3B3C3D3E3F40",
		"%d":                   "[1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64]",
		"%t":                   "%!t(hash=0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40)",
		"Hex":                  "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f40",
		"TerminalString":       "010203…3e3f40",
		"SetBytesShort":        "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000deadbe",
		"SetBytesLong":         "060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445",
		"UnmarshalTextErr":     "hex string has length 4, want 128 for Bytes64",
		"UnmarshalJSONErrType": "common.Bytes64",
	},
	"Bytes96": {
		"String":               "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f60",
		"MarshalText":          "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f60",
		"Value":                "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f60",
		"%v":                   "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f60",
		"%s":                   "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f60",
		"%q":                   "\"0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f60\"",
		"%x":                   "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f60",
		"%X":                   "0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F202122232425262728292A2B2C2D2E2F303132333435363738393A3B3C3D3E3F404142434445464748494A4B4C4D4E4F505152535455565758595A5B5C5D5E5F60",
		"%#x":                  "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f60",
		"%#X":                  "0X0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F202122232425262728292A2B2C2D2E2F303132333435363738393A3B3C3D3E3F404142434445464748494A4B4C4D4E4F505152535455565758595A5B5C5D5E5F60",
		"%d":                   "[1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96]",
		"%t":                   "%!t(hash=0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f60)",
		"Hex":                  "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f60",
		"TerminalString":       "010203…5e5f60",
		"SetBytesShort":        "000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000deadbe",
		"SetBytesLong":         "060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465",
		"UnmarshalTextErr":     "hex string has length 4, want 192 for BLSSignature",
		"UnmarshalJSONErrType": "common.Bytes96",
		"Generate":             "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3ff6b16be9bf8ac39fba6a4f2665389c9b5ce0b0eee5f5a2f97870cc80e9897f5add",
	},
	"Hash": {
		"String":               "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		"MarshalText":          "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		"Value":                "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		"%v":                   "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		"%s":                   "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		"%q":                   "\"0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20\"",
		"%x":                   "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		"%X":                   "0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F20",
		"%#x":                  "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		"%#X":                  "0X0102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F20",
		"%d":                   "[1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32]",
		"%t":                   "%!t(hash=0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20)",
		"Hex":                  "0x0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
		"TerminalString":       "010203…1e1f20",
		"SetBytesShort":        "0000000000000000000000000000000000000000000000000000000000deadbe",
		"SetBytesLong":         "060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425",
		"UnmarshalTextErr":     "hex string has length 4, want 64 for Hash",
		"UnmarshalJSONErrType": "common.Hash",
		"Generate":             "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f80",
	},
	"Address": {
		"String":               "0x0102030405060708090a0B0c0d0e0f1011121314",
		"MarshalText":          "0x0102030405060708090a0b0c0d0e0f1011121314",
		"Value":                "0102030405060708090a0b0c0d0e0f1011121314",
		"%v":                   "0x0102030405060708090a0B0c0d0e0f1011121314",
		"%s":                   "0x0102030405060708090a0B0c0d0e0f1011121314",
		"%q":                   "\"0x0102030405060708090a0B0c0d0e0f1011121314\"",
		"%x":                   "0102030405060708090a0b0c0d0e0f1011121314",
		"%X":                   "0102030405060708090A0B0C0D0E0F1011121314",
		"%#x":                  "0x0102030405060708090a0b0c0d0e0f1011121314",
		"%#X":                  "0X0102030405060708090A0B0C0D0E0F1011121314",
		"%d":                   "[1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20]",
		"%t":                   "%!t(address=0102030405060708090a0b0c0d0e0f1011121314)",
		"Hex":                  "0x0102030405060708090a0B0c0d0e0f1011121314",
		"HexEIP55":             "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed",
		"SetBytesShort":        "0000000000000000000000000000000000deadbe",
		"SetBytesLong":         "060708090a0b0c0d0e0f10111213141516171819",
		"UnmarshalTextErr":     "hex string has length 4, want 40 for Address",
		"UnmarshalJSONErrType": "common.Address",
	},
}

// TestFixedBytesGolden pins the exact formatting, marshalling, and SetBytes
// output of every fixed-size byte type.
func TestFixedBytesGolden(t *testing.T) {
	for _, tc := range fixedBytesGoldenCases(t) {
		t.Run(tc.name, func(t *testing.T) {
			want := fixedBytesGoldens[tc.name]
			if len(tc.rows) != len(want) {
				t.Errorf("have %d rows, want %d goldens", len(tc.rows), len(want))
			}
			for _, r := range tc.rows {
				w, ok := want[r.name]
				if !ok {
					t.Errorf("%s: no golden", r.name)
					continue
				}
				if r.got != w {
					t.Errorf("%s = %q, want %q", r.name, r.got, w)
				}
			}
		})
	}
}

type goldenRow struct {
	name string
	got  string
}

type goldenCase struct {
	name string
	rows []goldenRow
}

func fixedBytesGoldenCases(t *testing.T) []goldenCase {
	t.Helper()
	return []goldenCase{
		{"Bytes4", bytes4Goldens(t)},
		{"Bytes48", bytes48Goldens(t)},
		{"Bytes64", bytes64Goldens(t)},
		{"Bytes96", bytes96Goldens(t)},
		{"Hash", hashGoldens(t)},
		{"Address", addressGoldens(t)},
	}
}

func goldenSeq(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i + 1)
	}
	return b
}

type fixedBytesValue interface {
	fmt.Stringer
	encoding.TextMarshaler
	driver.Valuer
}

func formatGoldens(t *testing.T, v fixedBytesValue) []goldenRow {
	t.Helper()
	text, err := v.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText: %v", err)
	}
	value, err := v.Value()
	if err != nil {
		t.Fatalf("Value: %v", err)
	}
	valueBytes, ok := value.([]byte)
	if !ok {
		t.Fatalf("Value returned %T, want []byte", value)
	}
	rows := []goldenRow{
		{"String", v.String()},
		{"MarshalText", string(text)},
		{"Value", fmt.Sprintf("%x", valueBytes)},
	}
	for _, verb := range []string{"%v", "%s", "%q", "%x", "%X", "%#x", "%#X", "%d", "%t"} {
		rows = append(rows, goldenRow{verb, fmt.Sprintf(verb, v)})
	}
	return rows
}

func errString(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

func jsonErrType(t *testing.T, err error) string {
	t.Helper()
	var typeErr *json.UnmarshalTypeError
	if !errors.As(err, &typeErr) {
		t.Fatalf("expected *json.UnmarshalTypeError, got %v", err)
	}
	return typeErr.Type.String()
}

func bytes4Goldens(t *testing.T) []goldenRow {
	t.Helper()
	var b Bytes4
	b.SetBytes(goldenSeq(4))

	var short, long, u, j Bytes4
	short.SetBytes([]byte{0xde, 0xad, 0xbe})
	long.SetBytes(goldenSeq(9))
	gen := b.Generate(mrand.New(mrand.NewSource(7)), 10).Interface().(Bytes4)

	return append(formatGoldens(t, b),
		goldenRow{"Hex", b.Hex()},
		goldenRow{"TerminalString", b.TerminalString()},
		goldenRow{"SetBytesShort", fmt.Sprintf("%x", short)},
		goldenRow{"SetBytesLong", fmt.Sprintf("%x", long)},
		goldenRow{"UnmarshalTextErr", errString(u.UnmarshalText([]byte("0x1234")))},
		goldenRow{"UnmarshalJSONErrType", jsonErrType(t, j.UnmarshalJSON([]byte("5")))},
		goldenRow{"Generate", fmt.Sprintf("%x", gen)},
	)
}

func bytes48Goldens(t *testing.T) []goldenRow {
	t.Helper()
	var b Bytes48
	b.SetBytes(goldenSeq(48))

	var short, long, u, j Bytes48
	short.SetBytes([]byte{0xde, 0xad, 0xbe})
	long.SetBytes(goldenSeq(53))
	gen := b.Generate(mrand.New(mrand.NewSource(7)), 10).Interface().(Bytes48)

	return append(formatGoldens(t, b),
		goldenRow{"Hex", b.Hex()},
		goldenRow{"TerminalString", b.TerminalString()},
		goldenRow{"SetBytesShort", fmt.Sprintf("%x", short)},
		goldenRow{"SetBytesLong", fmt.Sprintf("%x", long)},
		goldenRow{"UnmarshalTextErr", errString(u.UnmarshalText([]byte("0x1234")))},
		goldenRow{"UnmarshalJSONErrType", jsonErrType(t, j.UnmarshalJSON([]byte("5")))},
		goldenRow{"Generate", fmt.Sprintf("%x", gen)},
	)
}

func bytes64Goldens(t *testing.T) []goldenRow {
	t.Helper()
	var b Bytes64
	b.SetBytes(goldenSeq(64))

	var short, long, u, j Bytes64
	short.SetBytes([]byte{0xde, 0xad, 0xbe})
	long.SetBytes(goldenSeq(69))

	return append(formatGoldens(t, b),
		goldenRow{"Hex", b.Hex()},
		goldenRow{"TerminalString", b.TerminalString()},
		goldenRow{"SetBytesShort", fmt.Sprintf("%x", short)},
		goldenRow{"SetBytesLong", fmt.Sprintf("%x", long)},
		goldenRow{"UnmarshalTextErr", errString(u.UnmarshalText([]byte("0x1234")))},
		goldenRow{"UnmarshalJSONErrType", jsonErrType(t, j.UnmarshalJSON([]byte("5")))},
	)
}

func bytes96Goldens(t *testing.T) []goldenRow {
	t.Helper()
	var b Bytes96
	b.SetBytes(goldenSeq(96))

	var short, long, u, j Bytes96
	short.SetBytes([]byte{0xde, 0xad, 0xbe})
	long.SetBytes(goldenSeq(101))
	gen := b.Generate(mrand.New(mrand.NewSource(7)), 10).Interface().(Bytes96)

	return append(formatGoldens(t, b),
		goldenRow{"Hex", b.Hex()},
		goldenRow{"TerminalString", b.TerminalString()},
		goldenRow{"SetBytesShort", fmt.Sprintf("%x", short)},
		goldenRow{"SetBytesLong", fmt.Sprintf("%x", long)},
		goldenRow{"UnmarshalTextErr", errString(u.UnmarshalText([]byte("0x1234")))},
		goldenRow{"UnmarshalJSONErrType", jsonErrType(t, j.UnmarshalJSON([]byte("5")))},
		goldenRow{"Generate", fmt.Sprintf("%x", gen)},
	)
}

func hashGoldens(t *testing.T) []goldenRow {
	t.Helper()
	var h Hash
	h.SetBytes(goldenSeq(32))

	var short, long, u, j Hash
	short.SetBytes([]byte{0xde, 0xad, 0xbe})
	long.SetBytes(goldenSeq(37))
	gen := h.Generate(randv2.New(randv2.NewPCG(7, 7)), 10).Interface().(Hash)

	return append(formatGoldens(t, h),
		goldenRow{"Hex", h.Hex()},
		goldenRow{"TerminalString", h.TerminalString()},
		goldenRow{"SetBytesShort", fmt.Sprintf("%x", short)},
		goldenRow{"SetBytesLong", fmt.Sprintf("%x", long)},
		goldenRow{"UnmarshalTextErr", errString(u.UnmarshalText([]byte("0x1234")))},
		goldenRow{"UnmarshalJSONErrType", jsonErrType(t, j.UnmarshalJSON([]byte("5")))},
		goldenRow{"Generate", fmt.Sprintf("%x", gen)},
	)
}

func addressGoldens(t *testing.T) []goldenRow {
	t.Helper()
	var a Address
	a.SetBytes(goldenSeq(20))

	var short, long, u, j Address
	short.SetBytes([]byte{0xde, 0xad, 0xbe})
	long.SetBytes(goldenSeq(25))

	return append(formatGoldens(t, a),
		goldenRow{"Hex", a.Hex()},
		goldenRow{"HexEIP55", HexToAddress("0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed").Hex()},
		goldenRow{"SetBytesShort", fmt.Sprintf("%x", short)},
		goldenRow{"SetBytesLong", fmt.Sprintf("%x", long)},
		goldenRow{"UnmarshalTextErr", errString(u.UnmarshalText([]byte("0x1234")))},
		goldenRow{"UnmarshalJSONErrType", jsonErrType(t, j.UnmarshalJSON([]byte("5")))},
	)
}
