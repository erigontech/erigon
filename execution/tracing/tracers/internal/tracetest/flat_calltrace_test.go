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

package tracetest

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

type flatCallTrace struct {
	Action       flatCallTraceAction `json:"action"`
	Error        string              `json:"error,omitempty"`
	Result       flatCallTraceResult `json:"result"`
	Subtraces    int                 `json:"subtraces"`
	TraceAddress []int               `json:"traceAddress"`
	Type         string              `json:"type"`
}

type flatCallTraceAction struct {
	Author         common.Address `json:"author,omitempty"`
	RewardType     string         `json:"rewardType,omitempty"`
	SelfDestructed common.Address `json:"address,omitempty"`
	Balance        hexutil.Big    `json:"balance"`
	CallType       string         `json:"callType,omitempty"`
	CreationMethod string         `json:"creationMethod,omitempty"`
	From           common.Address `json:"from,omitempty"`
	Gas            hexutil.Uint64 `json:"gas,omitempty"`
	Init           hexutil.Bytes  `json:"init,omitempty"`
	Input          hexutil.Bytes  `json:"input,omitempty"`
	RefundAddress  common.Address `json:"refundAddress,omitempty"`
	To             common.Address `json:"to,omitempty"`
	Value          hexutil.Big    `json:"value"`
}

type flatCallTraceResult struct {
	Address common.Address `json:"address,omitempty"`
	Code    hexutil.Bytes  `json:"code,omitempty"`
	GasUsed hexutil.Uint64 `json:"gasUsed,omitempty"`
	Output  hexutil.Bytes  `json:"output,omitempty"`
}

func TestFlatCallTracerGethFixtures(t *testing.T) {
	files, err := os.ReadDir(filepath.Join("testdata", "call_tracer_flat"))
	require.NoError(t, err)
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		t.Run(camel(strings.TrimSuffix(file.Name(), ".json")), func(t *testing.T) {
			t.Parallel()
			blob, err := os.ReadFile(filepath.Join("testdata", "call_tracer_flat", file.Name()))
			require.NoError(t, err)
			test := new(testcase)
			require.NoError(t, json.Unmarshal(blob, test))
			cfg := test.TracerConfig
			if cfg == nil {
				cfg = json.RawMessage("{}")
			}
			_, res, _ := traceFixtureTx(t, "flatCallTracer", test.Genesis, test.Context, test.Input, cfg)
			var have, want []flatCallTrace
			require.NoError(t, json.Unmarshal(res, &have))
			wantBlob, err := json.Marshal(test.Result)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(wantBlob, &want))
			h, err := json.MarshalIndent(have, "", " ")
			require.NoError(t, err)
			w, err := json.MarshalIndent(want, "", " ")
			require.NoError(t, err)
			if !bytes.Equal(h, w) {
				t.Fatalf("trace mismatch\nhave %s\nwant %s", h, w)
			}
		})
	}
}
