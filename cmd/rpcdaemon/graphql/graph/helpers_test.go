// Copyright 2024 The Erigon Authors
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

package graph

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/hexutil"
)

func TestConvertDataToStringP_Nil(t *testing.T) {
	m := map[string]any{"null": nil, "value": hexutil.Uint64(42)}

	if got := convertDataToStringP(m, "null"); got != nil {
		t.Errorf("nil value: expected nil, got %q", *got)
	}
	if got := convertDataToStringP(m, "missing"); got != nil {
		t.Errorf("missing key: expected nil, got %q", *got)
	}
	if got := convertDataToStringP(m, "value"); got == nil {
		t.Error("present value: expected non-nil, got nil")
	}
}

func TestConvertDataToIntP_Nil(t *testing.T) {
	m := map[string]any{"null": nil, "value": hexutil.Uint64(5)}

	if got := convertDataToIntP(m, "null"); got != nil {
		t.Errorf("nil value: expected nil, got %d", *got)
	}
	if got := convertDataToIntP(m, "missing"); got != nil {
		t.Errorf("missing key: expected nil, got %d", *got)
	}
	if got := convertDataToIntP(m, "value"); got == nil {
		t.Error("present value: expected non-nil, got nil")
	}
}

func TestConvertDataToUint64P_Nil(t *testing.T) {
	m := map[string]any{"null": nil, "value": hexutil.Uint64(7)}

	if got := convertDataToUint64P(m, "null"); got != nil {
		t.Errorf("nil value: expected nil, got %d", *got)
	}
	if got := convertDataToUint64P(m, "missing"); got != nil {
		t.Errorf("missing key: expected nil, got %d", *got)
	}
	if got := convertDataToUint64P(m, "value"); got == nil {
		t.Error("present value: expected non-nil, got nil")
	}
}

// RPCMarshalHeader emits number, difficulty and baseFeePerGas as *hexutil.U256.
// Without a case for it the type switch falls through to "unhandled" (strings)
// or 0 (uint64s), which GraphQL then serves as the block's number.
func TestConvertDataU256(t *testing.T) {
	m := map[string]any{
		"number":   (*hexutil.U256)(uint256.NewInt(0x1a2b)),
		"zero":     (*hexutil.U256)(uint256.NewInt(0)),
		"typedNil": (*hexutil.U256)(nil),
	}

	if got := convertDataToStringP(m, "number"); got == nil || *got != "0x1a2b" {
		t.Errorf("string of number: expected 0x1a2b, got %v", got)
	}
	if got := convertDataToStringP(m, "zero"); got == nil || *got != "0x0" {
		t.Errorf("string of zero: expected 0x0, got %v", got)
	}
	if got := convertDataToStringP(m, "typedNil"); got != nil {
		t.Errorf("string of typed nil: expected nil, got %q", *got)
	}

	if got := convertDataToUint64P(m, "number"); got == nil || *got != 0x1a2b {
		t.Errorf("uint64 of number: expected 6699, got %v", got)
	}
	if got := convertDataToUint64P(m, "zero"); got == nil || *got != 0 {
		t.Errorf("uint64 of zero: expected 0, got %v", got)
	}
	if got := convertDataToUint64P(m, "typedNil"); got != nil {
		t.Errorf("uint64 of typed nil: expected nil, got %d", *got)
	}
}
