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
	"github.com/erigontech/erigon/txnprovider/txtype/validate"
)

// AAHandler handles RIP-7560 native account abstraction transactions (type 5).
//
// AA transactions differ from standard types in several ways:
//   - Gated by the pool's AllowAA config flag (and eventually a fork activation)
//   - Allow non-EOA senders (the sender is a smart contract wallet)
//   - Use a different intrinsic gas formula (IsAATxn flag)
//   - ValidateTx performs static ERC-7562 bytecode analysis on the sender contract
//
// Construct with NewAAHandler to enable bytecode validation.  The zero value
// (AAHandler{}) has a nil reader and skips bytecode validation — safe for
// contexts without state access (tests, the global registry).
type AAHandler struct {
	DefaultHandler
	reader validate.CodeReader
}

// NewAAHandler creates an AAHandler with the given CodeReader for static
// ERC-7562 bytecode validation at pool admission.
func NewAAHandler(reader validate.CodeReader) AAHandler {
	return AAHandler{reader: reader}
}

func (AAHandler) TypeByte() byte { return types.AccountAbstractionTxType }
func (AAHandler) Name() string   { return "account-abstraction" }

// ForkRequired returns true when AllowAA is set in the pool config.
func (AAHandler) ForkRequired(forks ForkState) bool { return forks.AllowAA }

// CanCreate returns false — AA transactions cannot deploy contracts directly.
func (AAHandler) CanCreate() bool { return false }

// IntrinsicGasFlags signals that AA intrinsic gas must use the AA formula.
func (AAHandler) IntrinsicGasFlags() IntrinsicGasFlags {
	return IntrinsicGasFlags{IsAATxn: true}
}

// ValidateTx performs static ERC-7562 bytecode analysis on the sender contract.
//
// Rules checked statically (ERC-7562 §3):
//   - Banned environment opcodes [OP-011]–[OP-014]: ORIGIN, GASPRICE, BLOCKHASH,
//     COINBASE, TIMESTAMP, NUMBER, PREVRANDAO, GASLIMIT, BASEFEE, BLOBHASH,
//     BLOBBASEFEE, INVALID, SELFDESTRUCT, SELFBALANCE
//   - BALANCE ban [OP-011]
//   - GAS must be immediately followed by a *CALL [OP-031]
//
// Rules that require execution context (CALL-with-value, storage slot
// restrictions, out-of-gas revert ban) remain in ValidationRulesTracer and
// are enforced at execution time.
//
// If no CodeReader was provided (nil reader), validation is skipped.
func (h AAHandler) ValidateTx(tx types.Transaction, _ bool, _ *txpoolcfg.Config) txpoolcfg.DiscardReason {
	if h.reader == nil {
		return txpoolcfg.NotSet
	}
	aaTx, ok := tx.(*types.AccountAbstractionTransaction)
	if !ok {
		return txpoolcfg.InvalidAA
	}
	code, err := h.reader.Code(aaTx.SenderAddress.Value())
	if err != nil {
		return txpoolcfg.ErrGetCode
	}
	if v := validate.ERC7562Rules.Validate(code); v != nil {
		return txpoolcfg.InvalidAA
	}
	return txpoolcfg.NotSet
}
