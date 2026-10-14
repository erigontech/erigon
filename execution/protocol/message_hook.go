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
package protocol

import (
	"fmt"
	"sync"

	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// MessageHook acts on state around the execution of the messages of a
// registered transaction type.
type MessageHook interface {
	// BeforeExecute runs before the message's checks. An error rejects the
	// transaction.
	BeforeExecute(state evmtypes.IntraBlockState, msg Message) error
	// AfterExecute runs after the message executed; failed reports a reverted
	// or halted outer call.
	AfterExecute(state evmtypes.IntraBlockState, msg Message, failed bool) error
}

var (
	messageHooksMu sync.RWMutex
	messageHooks   = map[byte]MessageHook{}
)

// RegisterMessageHook registers hook for the messages of txType, which must
// be registered with types.RegisterTxType. It panics on any other type, a nil
// hook or a type that already has one.
func RegisterMessageHook(txType byte, hook MessageHook) {
	if !types.IsRegisteredTxType(txType) {
		panic(fmt.Sprintf("protocol: RegisterMessageHook: transaction type %d is not registered", txType))
	}
	if hook == nil {
		panic(fmt.Sprintf("protocol: RegisterMessageHook: nil hook for transaction type %d", txType))
	}
	messageHooksMu.Lock()
	defer messageHooksMu.Unlock()
	if _, ok := messageHooks[txType]; ok {
		panic(fmt.Sprintf("protocol: RegisterMessageHook: transaction type %d already has a hook", txType))
	}
	messageHooks[txType] = hook
}

func unregisterMessageHook(txType byte) {
	messageHooksMu.Lock()
	defer messageHooksMu.Unlock()
	delete(messageHooks, txType)
}

func messageHook(msg Message) MessageHook {
	typed, ok := msg.(interface{ TxType() byte })
	if !ok || typed.TxType() == 0 {
		return nil
	}
	messageHooksMu.RLock()
	defer messageHooksMu.RUnlock()
	return messageHooks[typed.TxType()]
}
