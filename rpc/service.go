// Copyright 2019 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unicode"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

var (
	contextType      = reflect.TypeFor[context.Context]()
	jsonStreamType   = reflect.TypeFor[jsonstream.Stream]()
	errorType        = reflect.TypeFor[error]()
	subscriptionType = reflect.TypeFor[Subscription]()
	stringType       = reflect.TypeFor[string]()

	jsonNull       = []byte("null")
	jsonEmptyArray = []byte("[]")
)

// invoker is the interface implemented by typed, generic-registered RPC methods.
// It replaces the reflection-based callback on the hot dispatch path.
type invoker interface {
	invoke(ctx context.Context, params json.RawMessage) (any, error)
}

// Method is a typed RPC handler that avoids reflection on every call.
// Register it with [RegisterMethod] on a [Server].
type Method[Params any, Result any] struct {
	fn func(ctx context.Context, params Params) (Result, error)
}

// invoke implements [invoker]. It unmarshals params into the typed Params value
// and calls fn, returning the result as any.
//
// Supported params forms, matching the reflection-based path:
//   - omitted / null / [] → zero-value Params; handlers must validate required fields
//   - [obj]               → single-element positional array unwrapped to obj
//     (go-ethereum client convention when calling with one struct arg)
//   - [a, b, ...]         → multi-element positional array unmarshalled directly
//     into Params (works when Params is a slice or array type)
//
// Leading JSON whitespace is trimmed before these checks so whitespace-padded
// payloads are handled identically to the reflection-based path.
func (m Method[Params, Result]) invoke(ctx context.Context, raw json.RawMessage) (any, error) {
	var p Params
	raw = bytes.TrimSpace(raw)
	if len(raw) > 0 && !bytes.Equal(raw, jsonNull) && !bytes.Equal(raw, jsonEmptyArray) {
		// Params must be a JSON array, matching the JSON-RPC 2.0 positional-args convention
		// used by the reflection-based path.
		if raw[0] != '[' {
			return nil, &InvalidParamsError{"non-array args"}
		}
		var arr []json.RawMessage
		if err := json.Unmarshal(raw, &arr); err != nil {
			return nil, &InvalidParamsError{err.Error()}
		}
		switch len(arr) {
		case 0:
			// leave p at zero value
		case 1:
			// Single-element positional array: [obj] → obj.
			// go-ethereum client wraps single struct args in an array.
			if err := json.Unmarshal(arr[0], &p); err != nil {
				return nil, &InvalidParamsError{err.Error()}
			}
		default:
			// Multi-element array: unmarshal the whole array into Params directly.
			// Works when Params is a slice or array type.
			if err := json.Unmarshal(raw, &p); err != nil {
				return nil, &InvalidParamsError{err.Error()}
			}
		}
	}
	result, err := m.fn(ctx, p)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// typedEntry holds a registered typed handler alongside its pre-cached metrics timers,
// matching the timer-caching pattern used by [callback].
type typedEntry struct {
	inv          invoker
	timerSuccess metrics.Summary
	timerFailure metrics.Summary
}

type serviceRegistry struct {
	mu       sync.RWMutex
	services map[string]service
	typed    map[string]typedEntry // typed generic handlers, checked before reflection-based ones
	logger   log.Logger
}

// service represents a registered object.
type service struct {
	name          string               // name for service
	callbacks     map[string]*callback // registered handlers
	subscriptions map[string]*callback // available subscriptions/notifications
}

// callback is a method callback which was registered in the server
type callback struct {
	fn          reflect.Value  // the function
	rcvr        reflect.Value  // receiver object of method, set if fn is method
	argTypes    []reflect.Type // input argument types
	hasCtx      bool           // method's first argument is a context (not included in argTypes)
	errPos      int            // err return idx, of -1 when method cannot return error
	isSubscribe bool           // true if this is a subscription callback
	streamable  bool           // support JSON streaming (more efficient for large responses)
	logger      log.Logger

	timerSuccess metrics.Summary // pre-cached success timer, avoids per-call GetOrCreateSummary
	timerFailure metrics.Summary // pre-cached failure timer
}

func (r *serviceRegistry) registerName(name string, rcvr any) error {
	rcvrVal := reflect.ValueOf(rcvr)
	if name == "" {
		return fmt.Errorf("no service name for type %s", rcvrVal.Type().String())
	}
	callbacks := suitableCallbacks(rcvrVal, r.logger)
	if len(callbacks) == 0 {
		return fmt.Errorf("service %T doesn't have any suitable methods/subscriptions to expose", rcvr)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.services == nil {
		r.services = make(map[string]service)
	}
	svc, ok := r.services[name]
	if !ok {
		svc = service{
			name:          name,
			callbacks:     make(map[string]*callback),
			subscriptions: make(map[string]*callback),
		}
		r.services[name] = svc
	}
	for shortName, cb := range callbacks {
		// Pre-cache metrics timers using the full "namespace_method" name so the
		// hot call path never needs to call GetOrCreateSummary or fmt.Sprintf.
		fullMethod := name + serviceMethodSeparator + shortName
		cb.timerSuccess = newRPCServingTimerMS(fullMethod, true)
		cb.timerFailure = newRPCServingTimerMS(fullMethod, false)
		if cb.isSubscribe {
			svc.subscriptions[shortName] = cb
		} else {
			svc.callbacks[shortName] = cb
		}
	}
	return nil
}

// callback returns the callback corresponding to the given RPC method name.
func (r *serviceRegistry) callback(method string) *callback {
	svc, name, ok := strings.Cut(method, serviceMethodSeparator)
	if !ok {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.services[svc].callbacks[name]
}

// registerTyped registers a typed generic handler for the given full method name
// (e.g. "eth_blockNumber"). Typed handlers take priority over reflection-based ones.
// It also ensures the namespace appears in services so that RPCService.Modules()
// includes typed-only namespaces.
func (r *serviceRegistry) registerTyped(name string, inv invoker) {
	ns, _, _ := strings.Cut(name, serviceMethodSeparator)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.typed == nil {
		r.typed = make(map[string]typedEntry)
	}
	r.typed[name] = typedEntry{
		inv:          inv,
		timerSuccess: newRPCServingTimerMS(name, true),
		timerFailure: newRPCServingTimerMS(name, false),
	}
	// Ensure the namespace is visible in services so RPCService.Modules() lists it.
	if r.services == nil {
		r.services = make(map[string]service)
	}
	if _, ok := r.services[ns]; !ok {
		r.services[ns] = service{
			name:          ns,
			callbacks:     make(map[string]*callback),
			subscriptions: make(map[string]*callback),
		}
	}
}

// invokerFor returns the typed entry registered for method, or zero value if none.
func (r *serviceRegistry) invokerFor(method string) (typedEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.typed[method]
	return e, ok
}

// subscription returns a subscription callback in the given service.
func (r *serviceRegistry) subscription(service, name string) *callback {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.services[service].subscriptions[name]
}

// suitableCallbacks iterates over the methods of the given type. It determines if a method
// satisfies the criteria for a RPC callback or a subscription callback and adds it to the
// collection of callbacks. See server documentation for a summary of these criteria.
func suitableCallbacks(receiver reflect.Value, logger log.Logger) map[string]*callback {
	typ := receiver.Type()
	callbacks := make(map[string]*callback)
	for m := 0; m < typ.NumMethod(); m++ {
		method := typ.Method(m)
		if method.PkgPath != "" {
			continue // method not exported
		}
		name := formatName(method.Name)
		cb := newCallback(receiver, method.Func, name, logger)
		if cb == nil {
			continue // function invalid
		}
		callbacks[name] = cb
	}
	return callbacks
}

// newCallback turns fn (a function) into a callback object. It returns nil if the function
// is unsuitable as an RPC callback.
func newCallback(receiver, fn reflect.Value, name string, logger log.Logger) *callback {
	fntype := fn.Type()
	c := &callback{fn: fn, rcvr: receiver, errPos: -1, isSubscribe: isPubSub(fntype), logger: logger}
	// Determine parameter types. They must all be exported or builtin types.
	c.makeArgTypes()

	// Verify return types. The function must return at most one error
	// and/or one other non-error value.
	outs := make([]reflect.Type, fntype.NumOut())
	for i := 0; i < fntype.NumOut(); i++ {
		outs[i] = fntype.Out(i)
	}
	if len(outs) > 2 {
		logger.Warn(fmt.Sprintf("Cannot register RPC callback [%s] - maximum 2 return values are allowed, got %d", name, len(outs)))
		return nil
	}
	// If an error is returned, it must be the last returned value.
	switch {
	case len(outs) == 1 && isErrorType(outs[0]):
		c.errPos = 0
	case len(outs) == 2:
		if isErrorType(outs[0]) || !isErrorType(outs[1]) {
			logger.Warn(fmt.Sprintf("Cannot register RPC callback [%s] - error must the last return value", name))
			return nil
		}
		c.errPos = 1
	}
	// If there is only one return value (error), and the last argument is jsonstream.Stream, mark it as streamable
	if len(outs) != 1 && c.streamable {
		log.Warn(fmt.Sprintf("Cannot register RPC callback [%s] - streamable method may only return 1 value (error)", name))
		return nil
	}
	return c
}

// makeArgTypes composes the argTypes list.
func (c *callback) makeArgTypes() {
	fntype := c.fn.Type()
	// Skip receiver and context.Context parameter (if present).
	firstArg := 0
	if c.rcvr.IsValid() {
		firstArg++
	}
	if fntype.NumIn() > firstArg && fntype.In(firstArg) == contextType {
		c.hasCtx = true
		firstArg++
	}
	// Check if method is streamable
	numArgs := fntype.NumIn()
	if fntype.NumIn() > firstArg && fntype.In(numArgs-1) == jsonStreamType {
		c.streamable = true
		numArgs--
	}
	// Add all remaining parameters (expect json stream, if present)
	c.argTypes = make([]reflect.Type, numArgs-firstArg)
	for i := firstArg; i < numArgs; i++ {
		c.argTypes[i-firstArg] = fntype.In(i)
	}
}

// call invokes the callback.
func (c *callback) call(ctx context.Context, method string, args []reflect.Value, stream jsonstream.Stream) (res any, errRes error) {
	// Create the argument slice.
	fullargs := make([]reflect.Value, 0, 2+len(args))
	if c.rcvr.IsValid() {
		fullargs = append(fullargs, c.rcvr)
	}
	if c.hasCtx {
		fullargs = append(fullargs, reflect.ValueOf(ctx))
	}
	fullargs = append(fullargs, args...)
	if c.streamable {
		fullargs = append(fullargs, reflect.ValueOf(stream))
	}

	// Catch panic while running the callback.
	defer func() {
		if err := recover(); err != nil {
			c.logger.Error("RPC method " + method + " crashed: " + fmt.Sprintf("%v\n%s", err, dbg.Stack()))
			errRes = errors.New("method handler crashed")
		}
	}()
	// Run the callback.
	results := c.fn.Call(fullargs)
	if len(results) == 0 {
		return nil, nil
	}
	if c.errPos >= 0 && !results[c.errPos].IsNil() {
		err := results[c.errPos].Interface().(error)
		return reflect.Value{}, err
	}
	if dbg.RpcDropResponse {
		return nil, nil
	}
	return results[0].Interface(), nil
}

// Is t context.Context or *context.Context?
func isContextType(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t == contextType
}

// Does t satisfy the error interface?
func isErrorType(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t.Implements(errorType)
}

// Is t Subscription or *Subscription?
func isSubscriptionType(t reflect.Type) bool {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t == subscriptionType
}

// isPubSub tests whether the given method has as first argument a context.Context and
// returns the pair (Subscription, error).
func isPubSub(methodType reflect.Type) bool {
	// numIn(0) is the receiver type
	if methodType.NumIn() < 2 || methodType.NumOut() != 2 {
		return false
	}
	return isContextType(methodType.In(1)) &&
		isSubscriptionType(methodType.Out(0)) &&
		isErrorType(methodType.Out(1))
}

// formatName converts to first character of name to lowercase.
func formatName(name string) string {
	ret := []rune(name)
	if len(ret) > 0 {
		ret[0] = unicode.ToLower(ret[0])
	}
	return string(ret)
}
