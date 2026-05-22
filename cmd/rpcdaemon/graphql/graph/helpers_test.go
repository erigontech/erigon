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
