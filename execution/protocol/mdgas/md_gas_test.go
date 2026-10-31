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

package mdgas

import (
	"testing"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
)

func TestRefillReversesExecutionGasConsumption(t *testing.T) {
	initial := MdGas{Execution: 100, State: 30}
	remaining := initial
	var used MdGasUsage

	if !Consume(&remaining, &used, 40, ExecutionGas) {
		t.Fatal("execution gas consumption failed")
	}
	Refill(&remaining, &used, 40, ExecutionGas)

	if remaining != initial {
		t.Fatalf("remaining gas: got %+v, want %+v", remaining, initial)
	}
	if used != (MdGasUsage{}) {
		t.Fatalf("used gas: got %+v, want zero", used)
	}
}

func TestRefillReversesStateGasConsumption(t *testing.T) {
	initial := MdGas{Execution: 100, State: 30}
	remaining := initial
	var used MdGasUsage

	if !Consume(&remaining, &used, 50, StateGas) {
		t.Fatal("state gas consumption failed")
	}
	Refill(&remaining, &used, 50, StateGas)

	if remaining != initial {
		t.Fatalf("remaining gas: got %+v, want %+v", remaining, initial)
	}
	if used != (MdGasUsage{}) {
		t.Fatalf("used gas: got %+v, want zero", used)
	}
}

func TestRefillStateGasUsesSpillFirst(t *testing.T) {
	remaining := MdGas{Execution: 80}
	used := MdGasUsage{State: 50, StateSpill: 20}

	Refill(&remaining, &used, 10, StateGas)
	if remaining != (MdGas{Execution: 90}) {
		t.Fatalf("remaining gas after first refill: got %+v", remaining)
	}
	if used != (MdGasUsage{State: 40, StateSpill: 10}) {
		t.Fatalf("used gas after first refill: got %+v", used)
	}

	Refill(&remaining, &used, 20, StateGas)
	if remaining != (MdGas{Execution: 100, State: 10}) {
		t.Fatalf("remaining gas after second refill: got %+v", remaining)
	}
	if used != (MdGasUsage{State: 20}) {
		t.Fatalf("used gas after second refill: got %+v", used)
	}
}

func TestSplitTxnGasLimit(t *testing.T) {
	const intrinsic = 21_000
	gas := params.MaxTxnGasLimit + 1_000_000
	amsterdam := &chain.Rules{IsAmsterdam: true}
	for _, tc := range []struct {
		name      string
		rules     *chain.Rules
		skipSplit bool
		want      MdGas
	}{
		{"pre-Amsterdam", &chain.Rules{}, false, MdGas{Execution: gas - intrinsic}},
		{"pre-Amsterdam with skipSplit", &chain.Rules{}, true, MdGas{Execution: gas - intrinsic}},
		{"Amsterdam", amsterdam, false, MdGas{Execution: params.MaxTxnGasLimit - intrinsic, State: gas - params.MaxTxnGasLimit}},
		{"Amsterdam with skipSplit", amsterdam, true, MdGas{Execution: gas - intrinsic}},
	} {
		if got := SplitTxnGasLimit(gas, intrinsic, tc.rules, tc.skipSplit); got != tc.want {
			t.Errorf("%s: got %+v, want %+v", tc.name, got, tc.want)
		}
	}
}
