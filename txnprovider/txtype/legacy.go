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

import "github.com/erigontech/erigon/execution/types"

// LegacyHandler handles untyped (pre-EIP-2718) legacy transactions (type 0).
// All behaviour is inherited from DefaultHandler.
type LegacyHandler struct{ DefaultHandler }

func (LegacyHandler) TypeByte() byte { return types.LegacyTxType }
func (LegacyHandler) Name() string   { return "legacy" }

// AccessListHandler handles EIP-2930 access-list transactions (type 1).
// All behaviour is inherited from DefaultHandler.
type AccessListHandler struct{ DefaultHandler }

func (AccessListHandler) TypeByte() byte { return types.AccessListTxType }
func (AccessListHandler) Name() string   { return "access-list" }

// DynamicFeeHandler handles EIP-1559 dynamic-fee transactions (type 2).
// All behaviour is inherited from DefaultHandler.
type DynamicFeeHandler struct{ DefaultHandler }

func (DynamicFeeHandler) TypeByte() byte { return types.DynamicFeeTxType }
func (DynamicFeeHandler) Name() string   { return "dynamic-fee" }
