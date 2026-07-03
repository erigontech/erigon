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

package vm

import (
	"testing"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
)

func TestCheckMaxCodeSize(t *testing.T) {
	t.Parallel()
	// Gnosis/Chiado disable EIP-170 via DisabledEIPs; the limit returns at Shanghai (EIP-3860).
	gnosis := func(shanghai bool) *chain.Rules {
		return &chain.Rules{IsSpuriousDragon: true, IsAura: true, IsShanghai: shanghai, DisabledEIPs: []int{170}}
	}
	tests := []struct {
		name    string
		rules   *chain.Rules
		size    int
		wantErr bool
	}{
		{"pre-SpuriousDragon ignores limit", &chain.Rules{}, params.MaxCodeSize + 1, false},
		{"SpuriousDragon enforces limit", &chain.Rules{IsSpuriousDragon: true}, params.MaxCodeSize + 1, true},
		{"SpuriousDragon allows at limit", &chain.Rules{IsSpuriousDragon: true}, params.MaxCodeSize, false},
		{"EIP-170-disabled chain ignores limit before Shanghai", gnosis(false), params.MaxCodeSize + 1, false},
		{"EIP-170-disabled chain enforces limit from Shanghai", gnosis(true), params.MaxCodeSize + 1, true},
		{"Amsterdam allows at the raised limit", &chain.Rules{IsSpuriousDragon: true, IsAmsterdam: true}, params.MaxCodeSizeAmsterdam, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if gotErr := CheckMaxCodeSize(tt.size, tt.rules) != nil; gotErr != tt.wantErr {
				t.Fatalf("CheckMaxCodeSize(%d) gotErr = %v, want %v", tt.size, gotErr, tt.wantErr)
			}
		})
	}
}
