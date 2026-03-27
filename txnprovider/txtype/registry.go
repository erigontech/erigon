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

// Package txtype provides the transaction type registry for the modular
// transaction pipeline. It defines TypeHandler — the interface every transaction
// type must implement — and the Registry that maps type bytes to handlers.
//
// The registry replaces scattered type switches throughout the txpool and
// execution pipeline with a single dispatch point. New transaction types
// (EIP-8141 Frame transactions, etc.) are added by registering a handler;
// the pipeline code itself does not change.
//
// Migration phases:
//
//	Phase 1 (this package): Define interfaces + DefaultHandler. No pipeline changes.
//	Phase 2: Migrate pool validation to call handler.Validate().
//	Phase 3: Migrate pool operations (add/remove/promote) through PoolPolicy.
//	Phase 4: Migrate execution path through Executor.
//	Phase 5: Add FrameHandler for EIP-8141, register via registry.Register().
package txtype

import "fmt"

// TypeHandler defines how a transaction type behaves at each pipeline stage.
// Implementations should embed DefaultHandler and override only what they need.
type TypeHandler interface {
	// TypeByte returns the EIP-2718 transaction type byte.
	TypeByte() byte
	// Name returns a human-readable name for logging and metrics.
	Name() string

	Validator
	PoolPolicy
}

// Registry maps type bytes to TypeHandlers.
// It is not safe for concurrent writes; populate at init time.
type Registry struct {
	handlers map[byte]TypeHandler
}

// NewRegistry creates an empty Registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[byte]TypeHandler)}
}

// Register adds a handler to the registry. Panics on duplicate registration.
func (r *Registry) Register(h TypeHandler) {
	if _, exists := r.handlers[h.TypeByte()]; exists {
		panic("txtype: duplicate handler registration for type " + h.Name())
	}
	r.handlers[h.TypeByte()] = h
}

// Get returns the handler for the given type byte and whether it was found.
func (r *Registry) Get(typeByte byte) (TypeHandler, bool) {
	h, ok := r.handlers[typeByte]
	return h, ok
}

// Global is the default registry, populated at init time by each handler package.
// The pool uses this registry for type dispatch.
var Global = NewRegistry()

func init() {
	Global.Register(LegacyHandler{})
	Global.Register(AccessListHandler{})
	Global.Register(DynamicFeeHandler{})
	Global.Register(BlobHandler{})
	Global.Register(SetCodeHandler{})
	Global.Register(AAHandler{})
}

// TypeName returns the human-readable name for a transaction type byte.
// Falls back to the numeric representation if unregistered.
func TypeName(typeByte byte) string {
	if h, ok := Global.Get(typeByte); ok {
		return h.Name()
	}
	return fmt.Sprintf("type(%d)", typeByte)
}
