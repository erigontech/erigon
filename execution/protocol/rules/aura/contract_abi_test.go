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

package aura

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// Regression test for #21066: getCertifier must fail closed (return nil)
// instead of panicking when the EVM syscall into the registrar returns an
// error or unexpected bytes. Prior to the fix this crashed the eth_call RPC
// handler on Gnosis with "invalid opcode: SHR" propagating up as a panic.
func TestGetCertifierFailsClosed(t *testing.T) {
	registrar := common.HexToAddress("0x6B53721D4f2Fb9514B85f5C49b197D857e36Cf03")

	tests := []struct {
		name    string
		syscall rules.SystemCall
	}{
		{
			name: "syscall returns error",
			syscall: func(_ accounts.Address, _ []byte) ([]byte, error) {
				return nil, errors.New("invalid opcode: SHR")
			},
		},
		{
			name: "syscall returns malformed bytes (unpack fails)",
			syscall: func(_ accounts.Address, _ []byte) ([]byte, error) {
				return []byte{0xde, 0xad, 0xbe, 0xef}, nil
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.NotPanics(t, func() {
				got := getCertifier(registrar, tc.syscall)
				assert.Nil(t, got, "expected nil certifier on syscall/unpack failure")
			})
		})
	}
}
