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

package commitmentdb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
)

func pbinTestCode(n int) []byte {
	code := make([]byte, n)
	for i := range code {
		code[i] = byte(n + i)
	}
	return code
}

// TestPBinTrieContextCodeReadsCodeDomain pins the read the binary trie's code
// chunking rests on. Chunk leaves hold bytecode, which no other trie needs and
// no account read returns.
func TestPBinTrieContextCodeReadsCodeDomain(t *testing.T) {
	t.Parallel()

	code := pbinTestCode(100)
	addr := pbinCodeSizeAddr(6)
	ttx := pbinCodeSizeTrieContext(t, true, addr, pbinCodeSizeAccount(crypto.Keccak256Hash(code)), code)

	got, err := ttx.Code(addr)
	require.NoError(t, err)
	require.Equal(t, code, got)

	absent, err := ttx.Code(pbinCodeSizeAddr(7))
	require.NoError(t, err)
	require.Empty(t, absent)
}

// TestPBinSharedDomainsCommitsCodeBearingAccount is the wiring end to end: the
// engine chunks code it reads through the trie context, and cross-checks the
// chunk count against the code_size it hashes, so a context that cannot serve
// code fails the commit rather than committing a code-less tree. Chunk values
// themselves are pinned against the reference tree in the commitment package.
func TestPBinSharedDomainsCommitsCodeBearingAccount(t *testing.T) {
	t.Parallel()

	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie

	code := pbinTestCode(1000)
	addr := pbinCodeSizeAddr(8)
	acc := pbinCodeSizeAccount(crypto.Keccak256Hash(code))

	sd, tx := pbinCodeSizeSharedDomains(t, []execctx.SharedDomainOption{execctx.WithTrieConfig(cfg)}, addr, acc, code)
	require.IsType(t, &commitment.PBinPatriciaHashed{}, sd.GetCommitmentCtx().Trie())

	withCode, err := sd.ComputeCommitment(t.Context(), tx, false, 0, 0, "pbin-code", nil)
	require.NoError(t, err)

	// The same account with one byte of code roots differently: the chunk leaves
	// are part of what is committed, not a side table.
	short := pbinTestCode(1)
	shortSd, shortTx := pbinCodeSizeSharedDomains(t,
		[]execctx.SharedDomainOption{execctx.WithTrieConfig(cfg)}, addr, pbinCodeSizeAccount(crypto.Keccak256Hash(short)), short)
	withShortCode, err := shortSd.ComputeCommitment(t.Context(), shortTx, false, 0, 0, "pbin-code-short", nil)
	require.NoError(t, err)
	require.NotEqual(t, withShortCode, withCode)
}

// TestPBinSharedDomainsRefusesCodeBeyondHeader pins the M1 boundary at the
// domain layer: chunks past the account header belong in the code zone, and
// until that lands a contract needing them is refused rather than committed
// short.
func TestPBinSharedDomainsRefusesCodeBeyondHeader(t *testing.T) {
	t.Parallel()

	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie

	code := pbinTestCode(128*31 + 1)
	addr := pbinCodeSizeAddr(9)
	sd, tx := pbinCodeSizeSharedDomains(t,
		[]execctx.SharedDomainOption{execctx.WithTrieConfig(cfg)}, addr, pbinCodeSizeAccount(crypto.Keccak256Hash(code)), code)

	_, err := sd.ComputeCommitment(t.Context(), tx, false, 0, 0, "pbin-code-overflow", nil)
	require.ErrorIs(t, err, commitment.ErrPBinUnsupported)
}
