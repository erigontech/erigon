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

package commitment

import (
	"context"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
)

// TestPBinCodeSizeMissingIsRefused: the chunk gate reads code_size, which only a
// context configured for bin fills in. Under a context that leaves it at zero
// every code-bearing account would chunk to nothing, so the whole code zone
// would go missing behind a root that still verifies against itself.
func TestPBinCodeSizeMissingIsRefused(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(51)
	code := pbinTestCode(62)
	sizeless := new(pbinTestCorpus).accountWithCode(addr, 1, 10, keccak.Sum256(code), 0)

	pph, ms := pbinTestEngine(t)
	sizeless.applyTo(t, ms)
	ms.setCode(addr, code)

	upd := WrapKeyUpdates(t, ModeDirect, pbinKeyHasher(), sizeless.plainKeys, sizeless.updates)
	_, err := pph.Process(context.Background(), upd, "", nil, WarmupConfig{})
	require.ErrorIs(t, err, errPBinCodeSizeMissing)

	// The same account with its size filled in reaches the code zone, so the
	// refusal above is about the size and not about the account.
	sized := new(pbinTestCorpus).accountWithCodeBytes(addr, 1, 10, code)
	require.Equal(t, 2+2, sized.leafCount(t), "two header leaves plus ceil(62/31) chunks")
	_, root := sized.process(t)
	require.Equal(t, sized.oracleRoot(t), root)
}

// A codeless account is the one state that legitimately reports no code size.
func TestPBinCodelessAccountPassesTheCodeSizeGate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		codeHash common.Hash
	}{
		{name: "empty-bytecode hash", codeHash: empty.CodeHash},
		{name: "unset hash", codeHash: common.Hash{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			corpus := new(pbinTestCorpus).account(pbinOracleAddr(52), 1, 10, tc.codeHash)
			_, root := corpus.process(t)
			require.Equal(t, corpus.oracleRoot(t), root)
		})
	}
}

// A removed account's code fields are whatever the batch merge left behind, so
// the gate must read them as no code rather than as a missing size.
func TestPBinRemovedAccountPassesTheCodeSizeGate(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(53)
	deploy := new(pbinTestCorpus).accountWithCodeBytes(addr, 1, 10, pbinTestCode(62))
	remove := new(pbinTestCorpus).remove(addr)

	pph, ms := pbinTestEngine(t)
	deploy.applyTo(t, ms)
	pbinTestProcess(t, pph, deploy.plainKeys, deploy.updates)

	stale := Update{Flags: DeleteUpdate, CodeHash: keccak.Sum256(pbinTestCode(62))}
	upd := WrapKeyUpdates(t, ModeUpdate, pbinKeyHasher(), remove.plainKeys, []Update{stale})
	_, err := pph.Process(context.Background(), upd, "", nil, WarmupConfig{})
	require.NoError(t, err)
}
