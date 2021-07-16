/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package txpool

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"
)

var hashParseTests = []struct {
	payloadStr  string
	hashStr     string
	expectedErr bool
}{
	{payloadStr: "a0595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328", hashStr: "595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328", expectedErr: false},
}

func TestParseHash(t *testing.T) {
	for i, tt := range hashParseTests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			var payload []byte
			var err error
			var hashBuf [32]byte
			var parseEnd int
			if payload, err = hex.DecodeString(tt.payloadStr); err != nil {
				t.Fatal(err)
			}
			if _, parseEnd, err = ParseHash(payload, 0, hashBuf[:0]); err != nil {
				if !tt.expectedErr {
					t.Fatal(err)
				}
			} else if tt.expectedErr {
				t.Fatalf("expected error when parsing")
			}
			if parseEnd != len(payload) {
				t.Errorf("parsing ended at %d, expected %d", parseEnd, len(payload))
			}
			if tt.hashStr != "" {
				var hash []byte
				if hash, err = hex.DecodeString(tt.hashStr); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(hash, hashBuf[:]) {
					t.Errorf("hash expected %x, got %x", hash, hashBuf)
				}
			}
		})
	}
}

var hashEncodeTests = []struct {
	payloadStr  string
	hashesStr   string
	hashCount   int
	expectedErr bool
}{
	{payloadStr: "e1a0595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328",
		hashesStr: "595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328", hashCount: 1, expectedErr: false},
}

func TestEncodeHash(t *testing.T) {
	for i, tt := range hashEncodeTests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			var payload []byte
			var hashes []byte
			var err error
			var encodeBuf []byte
			if payload, err = hex.DecodeString(tt.payloadStr); err != nil {
				t.Fatal(err)
			}
			if hashes, err = hex.DecodeString(tt.hashesStr); err != nil {
				t.Fatal(err)
			}
			if encodeBuf, err = EncodeHashes(hashes, tt.hashCount, encodeBuf); err != nil {
				if !tt.expectedErr {
					t.Fatal(err)
				}
			} else if tt.expectedErr {
				t.Fatalf("expected error when encoding")
			}
			if !bytes.Equal(payload, encodeBuf) {
				t.Errorf("encoding expected %x, got %x", payload, encodeBuf)
			}
		})
	}
}

var gpt66EncodeTests = []struct {
	payloadStr  string
	hashesStr   string
	hashCount   int
	requestId   uint64
	expectedErr bool
}{
	{payloadStr: "e68306f854e1a0595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328",
		hashesStr: "595e27a835cd79729ff1eeacec3120eeb6ed1464a04ec727aaca734ead961328", hashCount: 1, requestId: 456788, expectedErr: false},
}

// TestEncodeGPT66 tests the encoding of GetPoolTransactions66 packet
func TestEncodeGPT66(t *testing.T) {
	for i, tt := range gpt66EncodeTests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			var payload []byte
			var hashes []byte
			var err error
			var encodeBuf []byte
			if payload, err = hex.DecodeString(tt.payloadStr); err != nil {
				t.Fatal(err)
			}
			if hashes, err = hex.DecodeString(tt.hashesStr); err != nil {
				t.Fatal(err)
			}
			if encodeBuf, err = EncodeGetPooledTransactions66(hashes, tt.hashCount, tt.requestId, encodeBuf); err != nil {
				if !tt.expectedErr {
					t.Fatal(err)
				}
			} else if tt.expectedErr {
				t.Fatalf("expected error when encoding")
			}
			if !bytes.Equal(payload, encodeBuf) {
				t.Errorf("encoding expected %x, got %x", payload, encodeBuf)
			}
		})
	}
}
