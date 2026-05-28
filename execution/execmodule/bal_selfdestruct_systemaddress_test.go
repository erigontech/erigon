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

package execmodule_test

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

// pushAddrSelfdestruct returns init code that pushes `addr` onto the stack and
// runs SELFDESTRUCT: PUSH20 <addr> ; SELFDESTRUCT  (0x73 || 20 bytes || 0xff).
func pushAddrSelfdestruct(addr common.Address) []byte {
	out := make([]byte, 0, 22)
	out = append(out, 0x73)
	out = append(out, addr.Bytes()...)
	out = append(out, 0xff)
	return out
}

// TestBALIncludesSystemAddressOnSelfdestructToItWithZeroBalance asserts
// the EIP-7928 rule that a SELFDESTRUCT records the beneficiary as a
// state access independently of value transfer, combined with the
// SystemAddress carve-out exception ("MUST NOT be included unless it
// experiences state access itself"). A SELFDESTRUCT to SystemAddress
// is itself such an access, so the entry MUST appear in the BAL even
// when contract balance is zero and no value is transferred.
//
// Init code: PUSH20 SystemAddress; SELFDESTRUCT  (0x73 || 20 bytes || 0xff)
func TestBALIncludesSystemAddressOnSelfdestructToItWithZeroBalance(t *testing.T) {
	t.Parallel()

	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)

	genesis := &types.Genesis{
		Config: chain.AllProtocolChanges, // AmsterdamTime = 0, EIP-7928 active
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(privKey),
	)

	require.True(t, m.ChainConfig.IsAmsterdam(0), "Amsterdam must be active at genesis time for EIP-7928")

	initCode := pushAddrSelfdestruct(params.SystemAddress.Value())

	baseFee := m.Genesis.BaseFee().Uint64()
	gasPrice := baseFee * 2
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)

	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, gen *blockgen.BlockGen) {
		nonce := gen.TxNonce(senderAddr)
		tx, txErr := types.SignTx(
			types.NewContractCreation(nonce, uint256.NewInt(0), 1_000_000, uint256.NewInt(gasPrice), initCode),
			*signer,
			privKey,
		)
		require.NoError(t, txErr)
		gen.AddTx(tx)
	})
	require.NoError(t, err)

	// The CREATE tx must succeed: the init code runs SELFDESTRUCT before
	// returning any deploy bytes, so the receipt is a successful CREATE
	// with no deployed code at the contract address.
	require.Len(t, chainPack.Receipts[0], 1)
	receipt := chainPack.Receipts[0][0]
	require.Equal(t, types.ReceiptStatusSuccessful, receipt.Status,
		"SELFDESTRUCT-in-init-code CREATE tx should succeed")

	// Sanity: the block header carries the BAL hash now that Amsterdam is
	// active. The BlockAccessLists payload is the RLP encoding that hashes
	// to BlockAccessListHash.
	require.NotNil(t, chainPack.Headers[0].BlockAccessListHash,
		"Amsterdam block header must carry BlockAccessListHash")
	require.NotEmpty(t, chainPack.BlockAccessLists[0],
		"Amsterdam block must produce a non-empty BAL payload")

	bal, err := types.DecodeBlockAccessListBytes(chainPack.BlockAccessLists[0])
	require.NoError(t, err)

	sysAddr := params.SystemAddress
	var sysEntry *types.AccountChanges
	for _, ac := range bal {
		if ac.Address == sysAddr {
			sysEntry = ac
			break
		}
	}

	require.NotNil(t, sysEntry,
		"BAL must include a SystemAddress entry: EIP-7928 records SELFDESTRUCT as an access on the beneficiary even when no value is transferred, and the SystemAddress carve-out exception fires because the SELFDESTRUCT is the access itself.")

	// All changesets are empty: the SELFDESTRUCT is the only access on
	// SystemAddress in this block; no balance/nonce/code/storage state
	// actually changes on the beneficiary.
	require.Empty(t, sysEntry.StorageChanges, "SystemAddress entry should have no storage changes")
	require.Empty(t, sysEntry.StorageReads, "SystemAddress entry should have no storage reads")
	require.Empty(t, sysEntry.BalanceChanges, "SystemAddress entry should have no balance changes (zero transfer)")
	require.Empty(t, sysEntry.NonceChanges, "SystemAddress entry should have no nonce changes")
	require.Empty(t, sysEntry.CodeChanges, "SystemAddress entry should have no code changes")
}

// TestBALIncludesSystemAddressOnSelfdestructToItWithNonZeroBalance is a
// guard for the non-zero balance variant: when the destructing contract
// has a non-zero balance, the BAL records a balance change on the
// SystemAddress beneficiary in addition to the access already recorded
// by the SELFDESTRUCT itself.
func TestBALIncludesSystemAddressOnSelfdestructToItWithNonZeroBalance(t *testing.T) {
	t.Parallel()

	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)

	genesis := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(privKey),
	)

	// Same init code as the zero-balance test — but this time the CREATE
	// tx sends 1 wei to the new contract so its balance is non-zero at
	// SELFDESTRUCT time.
	initCode := pushAddrSelfdestruct(params.SystemAddress.Value())

	baseFee := m.Genesis.BaseFee().Uint64()
	gasPrice := baseFee * 2
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)

	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, gen *blockgen.BlockGen) {
		nonce := gen.TxNonce(senderAddr)
		tx, txErr := types.SignTx(
			types.NewContractCreation(nonce, uint256.NewInt(1), 1_000_000, uint256.NewInt(gasPrice), initCode),
			*signer,
			privKey,
		)
		require.NoError(t, txErr)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	require.Equal(t, types.ReceiptStatusSuccessful, chainPack.Receipts[0][0].Status)

	bal, err := types.DecodeBlockAccessListBytes(chainPack.BlockAccessLists[0])
	require.NoError(t, err)

	var sysEntry *types.AccountChanges
	for _, ac := range bal {
		if ac.Address == params.SystemAddress {
			sysEntry = ac
			break
		}
	}
	require.NotNil(t, sysEntry, "BAL must include SystemAddress entry on non-zero-balance SELFDESTRUCT to it")
	require.NotEmpty(t, sysEntry.BalanceChanges, "BAL must record a balance change on the SystemAddress beneficiary when SELFDESTRUCT transfers non-zero value to it")
}

// TestBALIncludesOrdinaryBeneficiaryOnSelfdestructWithZeroBalance is a
// guard for non-SystemAddress beneficiaries: SELFDESTRUCT with zero
// contract balance to an ordinary EOA must still record the beneficiary
// in the BAL. The SystemAddress carve-out filter does not apply here,
// so the entry's inclusion follows directly from the general EIP-7928
// rule for SELFDESTRUCT beneficiaries.
func TestBALIncludesOrdinaryBeneficiaryOnSelfdestructWithZeroBalance(t *testing.T) {
	t.Parallel()

	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)

	// An arbitrary non-SystemAddress beneficiary EOA. Pick something
	// distinct from the sender / coinbase / system contracts so we can
	// tell it apart in the BAL.
	beneficiary := common.HexToAddress("0x00000000000000000000000000000000000000aa")

	genesis := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(privKey),
	)

	initCode := pushAddrSelfdestruct(beneficiary)

	baseFee := m.Genesis.BaseFee().Uint64()
	gasPrice := baseFee * 2
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)

	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, gen *blockgen.BlockGen) {
		nonce := gen.TxNonce(senderAddr)
		tx, txErr := types.SignTx(
			types.NewContractCreation(nonce, uint256.NewInt(0), 1_000_000, uint256.NewInt(gasPrice), initCode),
			*signer,
			privKey,
		)
		require.NoError(t, txErr)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	require.Equal(t, types.ReceiptStatusSuccessful, chainPack.Receipts[0][0].Status)

	bal, err := types.DecodeBlockAccessListBytes(chainPack.BlockAccessLists[0])
	require.NoError(t, err)

	var found bool
	for _, ac := range bal {
		if ac.Address.Value() == beneficiary {
			found = true
			break
		}
	}
	require.True(t, found, "ordinary EOA beneficiary must appear in BAL on SELFDESTRUCT-to-it")
}
