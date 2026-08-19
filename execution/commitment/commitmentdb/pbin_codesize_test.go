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

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func pbinCodeSizeAddr(i byte) []byte {
	a := make([]byte, length.Addr)
	a[0], a[length.Addr-1] = 0xc0, i
	return a
}

func pbinCodeSizeSharedDomains(t *testing.T, opts []execctx.SharedDomainOption, addr []byte, acc *accounts.Account, code []byte) (*execctx.SharedDomains, kv.TemporalTx) {
	t.Helper()
	db := pbinNewTestDb(t)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), opts...)
	require.NoError(t, err)
	t.Cleanup(sd.Close)

	require.NoError(t, sd.DomainPut(kv.AccountsDomain, tx, addr, accounts.SerialiseV3(acc), 0, nil))
	if code != nil {
		require.NoError(t, sd.DomainPut(kv.CodeDomain, tx, addr, code, 0, nil))
	}
	return sd, tx
}

func pbinCodeSizeTrieContext(t *testing.T, readCodeSize bool, addr []byte, acc *accounts.Account, code []byte) *commitmentdb.TrieContext {
	t.Helper()
	sd, tx := pbinCodeSizeSharedDomains(t, nil, addr, acc, code)
	ttx := commitmentdb.NewTrieContextRo(commitmentdb.NewLatestStateReader(tx, sd), sd.StepSize())
	ttx.SetReadCodeSize(readCodeSize)
	return ttx
}

func pbinCodeSizeAccount(codeHash common.Hash) *accounts.Account {
	return &accounts.Account{Nonce: 3, Balance: *uint256.NewInt(77), CodeHash: accounts.InternCodeHash(codeHash)}
}

// BASIC_DATA's code_size is the length of the account's code in the CodeDomain.
func TestPBinTrieContextAccountReadsCodeSize(t *testing.T) {
	t.Parallel()

	code := []byte{0x60, 0x00, 0x60, 0x00, 0xfd}
	addr := pbinCodeSizeAddr(1)
	ttx := pbinCodeSizeTrieContext(t, true, addr, pbinCodeSizeAccount(crypto.Keccak256Hash(code)), code)

	u, err := ttx.Account(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(len(code)), u.CodeSize)
	require.NotZero(t, u.Flags&commitment.CodeUpdate)
}

// The hex trie does not hash code_size, so it must not pay for the extra
// CodeDomain read.
func TestPBinTrieContextLeavesCodeSizeZeroForHex(t *testing.T) {
	t.Parallel()

	code := []byte{0x60, 0x00, 0x60, 0x00, 0xfd}
	addr := pbinCodeSizeAddr(2)
	ttx := pbinCodeSizeTrieContext(t, false, addr, pbinCodeSizeAccount(crypto.Keccak256Hash(code)), code)

	u, err := ttx.Account(addr)
	require.NoError(t, err)
	require.Zero(t, u.CodeSize)
}

// A cleared EIP-7702 delegation leaves code in the CodeDomain that no longer
// belongs to the account. code_size follows the account's own code hash, so the
// residue must not move the root.
func TestPBinTrieContextIgnoresClearedDelegationResidue(t *testing.T) {
	t.Parallel()

	residue := []byte{0xef, 0x01, 0x00}
	addr := pbinCodeSizeAddr(3)
	ttx := pbinCodeSizeTrieContext(t, true, addr, pbinCodeSizeAccount(empty.CodeHash), residue)

	u, err := ttx.Account(addr)
	require.NoError(t, err)
	require.Zero(t, u.CodeSize)
	require.Equal(t, empty.CodeHash, u.CodeHash)
}

// A code hash with no code behind it (an eth_simulateV1 overlay, a truncated
// datadir) would hash as code_size 0 and silently produce a wrong root.
func TestPBinTrieContextRefusesCodeBearingAccountWithoutCode(t *testing.T) {
	t.Parallel()

	addr := pbinCodeSizeAddr(4)
	ttx := pbinCodeSizeTrieContext(t, true, addr, pbinCodeSizeAccount(common.Hash{0xAB}), nil)

	_, err := ttx.Account(addr)
	require.ErrorContains(t, err, "code missing")
}

// Only a bin SharedDomains must insist the code is there.
func TestPBinSharedDomainsReadsCodeSizeUnderBin(t *testing.T) {
	t.Parallel()

	cfg := commitment.DefaultTrieConfig()
	cfg.Variant = commitment.VariantBinPatriciaTrie

	addr := pbinCodeSizeAddr(5)
	acc := pbinCodeSizeAccount(common.Hash{0xAB})
	sd, tx := pbinCodeSizeSharedDomains(t, []execctx.SharedDomainOption{execctx.WithTrieConfig(cfg)}, addr, acc, nil)
	require.IsType(t, &commitment.PBinPatriciaHashed{}, sd.GetCommitmentCtx().Trie())

	_, err := sd.ComputeCommitment(t.Context(), tx, false, 0, 0, "pbin-codesize", nil)
	require.ErrorContains(t, err, "code missing")

	hexSd, hexTx := pbinCodeSizeSharedDomains(t, nil, addr, acc, nil)
	_, err = hexSd.ComputeCommitment(t.Context(), hexTx, false, 0, 0, "hex-codesize", nil)
	require.NoError(t, err, "hex does not hash code_size and must not start requiring the code")
}
