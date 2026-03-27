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

package txtype

import (
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

// AAHandler handles RIP-7560 native account abstraction transactions (type 5).
//
// AA transactions differ from standard types in several ways:
//   - Gated by the pool's AllowAA config flag (and eventually a fork activation)
//   - Allow non-EOA senders (the sender is a smart contract wallet)
//   - Use a different intrinsic gas formula (IsAATxn flag)
//   - Require ERC-7562 opcode restriction validation via ethBackend.AAValidation()
//
// Note: The ERC-7562 validation call to ethBackend currently lives in pool.go
// and is NOT yet delegated through this handler. Phase 2 of the design
// (Validation independence) will move that call into a self-contained
// txtype/validate/ package and wire it through ValidateTx.
type AAHandler struct{ DefaultHandler }

func (AAHandler) TypeByte() byte { return types.AccountAbstractionTxType }
func (AAHandler) Name() string   { return "account-abstraction" }

// ForkRequired returns true when AllowAA is set in the pool config.
// This maps to the existing cfg.AllowAA flag; a proper fork activation
// (similar to IsPrague for SetCode) will replace this in a future phase.
func (AAHandler) ForkRequired(forks ForkState) bool { return forks.AllowAA }

// CanCreate returns false — AA transactions cannot deploy contracts.
// (The deployer is handled via a dedicated DeployerData frame, not via
// the standard IsContractDeploy path.)
func (AAHandler) CanCreate() bool { return false }

// IntrinsicGasFlags signals that AA intrinsic gas must use the AA formula.
func (AAHandler) IntrinsicGasFlags() IntrinsicGasFlags {
	return IntrinsicGasFlags{IsAATxn: true}
}

// ValidateTx is a stub in Phase 1.
//
// The real validation calls ethBackend.AAValidation() to run ERC-7562 opcode
// restriction checks. That RPC call currently lives in pool.go:961 and will
// be moved into txtype/validate/ in Phase 2, at which point this method will
// delegate to a self-contained MempoolValidator.
func (AAHandler) ValidateTx(_ types.Transaction, _ bool, _ *txpoolcfg.Config) txpoolcfg.DiscardReason {
	// Phase 2: call mempoolValidator.ValidateAA(txn, cfg) here.
	return txpoolcfg.NotSet
}
