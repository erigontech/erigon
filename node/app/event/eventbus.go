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

package event

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/node/app"
	"github.com/erigontech/erigon/node/app/util"
)

// BusSubscriber defines subscription-related bus behavior
type BusSubscriber interface {
	Subscribe(fn interface{}) error
	SubscribeAsync(fn interface{}) error
	SubscribeOnce(fn interface{}) error
	SubscribeOnceAsync(fn interface{}) error
	Unsubscribe(handler interface{}) error
}

// BusPublisher defines publishing-related bus behavior
type BusPublisher interface {
	Publish(args ...interface{}) int
}

// BusController defines bus control behavior (checking handler's presence, synchronization)
type BusController interface {
	HasCallback(argTypes ...reflect.Type) bool
	WaitAsync()
}

// EventBus englobes global (subscribe, publish, control) bus behavior
type EventBus interface {
	BusController
	BusSubscriber
	BusPublisher
}

// eventBus - box for handlers and callbacks.
type eventBus struct {
	execPool      util.ExecPool
	handlerMap    *handlerMap
	lock          sync.RWMutex // a lock for the map
	wg            sync.WaitGroup
	prevQueueSize int
}

type handlerMap struct {
	nextArgInterfaces map[reflect.Type]int
	nextArgMap        map[reflect.Type]*handlerMap
	handlers          []*eventHandler
}

func (hmap *handlerMap) clone() *handlerMap {
	if hmap == nil {
		return nil
	}
	cloned := &handlerMap{
		nextArgInterfaces: make(map[reflect.Type]int, len(hmap.nextArgInterfaces)),
		nextArgMap:        make(map[reflect.Type]*handlerMap, len(hmap.nextArgMap)),
	}
	for k, v := range hmap.nextArgInterfaces {
		cloned.nextArgInterfaces[k] = v
	}
	if len(hmap.handlers) > 0 {
		cloned.handlers = make([]*eventHandler, len(hmap.handlers))
		copy(cloned.handlers, hmap.handlers)
	}
	for argType, nextMap := range hmap.nextArgMap {
		cloned.nextArgMap[argType] = nextMap.clone()
	}
	return cloned
}

func (hmap *handlerMap) collectOnceHandlers() []*eventHandler {
	var result []*eventHandler
	for _, h := range hmap.handlers {
		if h.flagOnce {
			result = append(result, h)
		}
	}
	for _, nextMap := range hmap.nextArgMap {
		result = append(result, nextMap.collectOnceHandlers()...)
	}
	return result
}

func (hmap *handlerMap) removeOnceHandler(callback reflect.Value) {
	if idx, _ := hmap.findHandlerIdx(callback); idx >= 0 {
		hmap.removeHandler(idx)
	}
	for _, nextMap := range hmap.nextArgMap {
		nextMap.removeOnceHandler(callback)
	}
}

func (hmap *handlerMap) publish(bus *eventBus, args []interface{}, argIndex int) int {
	var pubcount int = 0

	if argIndex < len(args) {
		argType := reflect.TypeOf(args[argIndex])
		nextArgMap := hmap.nextArgMap

		for mapType, nextMap := range nextArgMap {
			//fmt.Printf("%s->%s (%v)\n", argType, mapType, argType == mapType || argType.AssignableTo(mapType))
			if argType == mapType || argType.AssignableTo(mapType) {
				// fmt.Printf("%s->%s\n", argType, mapType)
				// TODO need to recurse so we cover all paths ?
				pubcount += nextMap.publish(bus, args, argIndex+1)
			}
		}
	}

	if argIndex == len(args) {
		if handlers := hmap.handlers; len(handlers) > 0 {
			// Handlers slice may be changed by removeHandler and Unsubscribe during iteration,
			// so make a copy and iterate the copied slice.
			copyHandlers := make([]*eventHandler, 0, len(handlers))
			copyHandlers = append(copyHandlers, handlers...)

			for _, handler := range copyHandlers {
				pubcount++

				logEnabled := log.TraceEnabled()

				if !handler.async {
					handler.doPublish(handler.bus, logEnabled, args...)
				} else {
					asyncHandler := handler
					bus.wg.Add(1)

					if log.TraceEnabled() {
						log.Trace("Exec handler func",
							"handler", fmt.Sprint(asyncHandler),
							"bus", app.LogInstance(asyncHandler.bus),
							"poolSize", bus.execPool.PoolSize(),
							"queueSize", bus.execPool.QueueSize(),
							"args", fmt.Sprint(args...))
					}

					bus.execPool.Exec(func() {
						bus.lock.RLock()
						handlerBus := asyncHandler.bus
						bus.lock.RUnlock()
						if handlerBus != nil {
							asyncHandler.doPublish(handlerBus, logEnabled, args...)
						} else {
							if logEnabled {
								log.Trace("Ignoring callback",
									"handler", fmt.Sprint(asyncHandler),
									"bus", app.LogInstance(handlerBus),
									"args", fmt.Sprint(args...))
							}
						}
						bus.wg.Done()
					})

					queueSize := bus.execPool.QueueSize()

					if queueSize > 0 {
						if queueSize > bus.prevQueueSize {
							if queueSize == 10 || queueSize == 20 || queueSize == 50 || queueSize%100 == 0 {
								log.Debug("Execpool overflowing",
									"bus", app.LogInstance(bus),
									"poolSize", bus.execPool.PoolSize(),
									"queueSize", bus.execPool.QueueSize())
							}
						} else if queueSize < bus.prevQueueSize {
							if queueSize == 10 || queueSize == 20 || queueSize == 50 || queueSize%100 == 0 {
								log.Debug("Execpool overflow recovering",
									"bus", app.LogInstance(bus),
									"poolSize", bus.execPool.PoolSize(),
									"queueSize", bus.execPool.QueueSize())
							}
						}

						bus.prevQueueSize = queueSize
					}
				}
			}
		}
	}

	return pubcount
}

func (hmap *handlerMap) removeHandler(idx int) {
	l := len(hmap.handlers)

	if !(0 <= idx && idx < l) {
		return
	}

	copy(hmap.handlers[idx:], hmap.handlers[idx+1:])
	hmap.handlers[l-1] = nil // or the zero value of T
	hmap.handlers = hmap.handlers[:l-1]
}

func (hmap *handlerMap) findHandlerIdx(callback reflect.Value) (int, *eventHandler) {
	for idx, handler := range hmap.handlers {
		if handler.callBack == callback {
			return idx, handler
		}
	}
	return -1, nil
}

type eventHandler struct {
	bus      *eventBus
	callBack reflect.Value
	flagOnce bool
	async    bool
}

func (handler *eventHandler) doPublish(bus *eventBus, logEnabled bool, args ...interface{}) {
	passedArguments := make([]reflect.Value, len(args))
	for i, arg := range args {
		passedArguments[i] = reflect.ValueOf(arg)
	}

	if logEnabled {
		log.Trace("Calling callback",
			"bus", app.LogInstance(bus),
			"async", handler.async,
			"callback", fmt.Sprint(handler.callBack),
			"args", fmt.Sprint(args...))
	}

	defer func() {
		var err error
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = fmt.Errorf("%w, stack: %s", e, dbg.Stack())
			} else {
				err = fmt.Errorf("panic: %v, stack: %s", r, dbg.Stack())
			}

			log.Error("Handler panicked",
				"bus", app.LogInstance(bus),
				"async", handler.async,
				"callback", fmt.Sprint(handler.callBack),
				"args", fmt.Sprint(args...),
				//TODO Stack().
				"err", err)
		}
	}()

	handler.callBack.Call(passedArguments)
}

// NewEventBus returns new eventBus with empty handlers.
func NewEventBus(execPool util.ExecPool) EventBus {
	b := &eventBus{
		execPool,
		&handlerMap{nil, map[reflect.Type]*handlerMap{}, []*eventHandler{}},
		sync.RWMutex{},
		sync.WaitGroup{},
		0,
	}
	return b
}

// doSubscribe handles the subscription logic and is utilized by the public Subscribe functions
func (bus *eventBus) doSubscribe(fn interface{}, handler *eventHandler) error {
	bus.lock.Lock()
	defer bus.lock.Unlock()

	fnType := reflect.TypeOf(fn)
	if !(fnType.Kind() == reflect.Func) {
		return fmt.Errorf("%s is not of type reflect.Func", reflect.TypeOf(fn).Kind())
	}

	argCount := fnType.NumIn()
	currentMap := bus.handlerMap

	for argIndex := 0; argIndex < argCount; argIndex++ {
		argType := fnType.In(argIndex)

		if nextMap, ok := currentMap.nextArgMap[argType]; ok {
			currentMap = nextMap
		} else {
			nextMap = &handlerMap{nil, map[reflect.Type]*handlerMap{}, []*eventHandler{}}
			currentMap.nextArgMap[argType] = nextMap
			currentMap = nextMap
		}
	}

	if idx, _ := currentMap.findHandlerIdx(reflect.ValueOf(fn)); idx >= 0 {
		return fmt.Errorf("duplicate subscribe")
	}

	currentMap.handlers = append(currentMap.handlers, handler)

	return nil
}

// Subscribe subscribes to a topic.
// Returns error if `fn` is not a function.
func (bus *eventBus) Subscribe(fn interface{}) error {
	return bus.doSubscribe(fn, &eventHandler{bus, reflect.ValueOf(fn), false, false})
}

// SubscribeAsync subscribes to a topic with an asynchronous callback
// Transactional determines whether subsequent callbacks for a topic are
// run serially (true) or concurrently (false)
// Returns error if `fn` is not a function.
func (bus *eventBus) SubscribeAsync(fn interface{}) error {
	return bus.doSubscribe(fn, &eventHandler{bus, reflect.ValueOf(fn), false, true})
}

// SubscribeOnce subscribes to a topic once. Handler will be removed after executing.
// Returns error if `fn` is not a function.
func (bus *eventBus) SubscribeOnce(fn interface{}) error {
	return bus.doSubscribe(fn, &eventHandler{bus, reflect.ValueOf(fn), true, false})
}

// SubscribeOnceAsync subscribes to a topic once with an asynchronous callback
// Handler will be removed after executing.
// Returns error if `fn` is not a function.
func (bus *eventBus) SubscribeOnceAsync(fn interface{}) error {
	return bus.doSubscribe(fn, &eventHandler{bus, reflect.ValueOf(fn), true, true})
}

// HasCallback returns true if any callbacks exist for the passed in types,
//
//	if the type is singular and is a function its argument types are
//	used for the lookup, otherwise the types are assumed to be argument
//	types
func (bus *eventBus) HasCallback(types ...reflect.Type) bool {
	bus.lock.Lock()
	defer bus.lock.Unlock()

	argCount := len(types)
	currentMap := bus.handlerMap

	for argIndex := 0; argIndex < argCount; argIndex++ {
		argType := types[argIndex]

		nextArgMap := currentMap.nextArgMap

		if nextMap, ok := nextArgMap[argType]; ok {
			currentMap = nextMap
		} else {
			currentMap = nil
			for mapType, nextMap := range nextArgMap {
				if argType.AssignableTo(mapType) {
					currentMap = nextMap
					break
				}
			}

			if currentMap == nil {
				return false
			}
		}
	}

	return len(currentMap.handlers) > 0
}

// Unsubscribe removes callback defined for a topic.
// Returns error if there are no callbacks subscribed to the topic.
func (bus *eventBus) Unsubscribe(fn interface{}) error {
	bus.lock.Lock()
	defer bus.lock.Unlock()

	fnType := reflect.TypeOf(fn)

	argCount := fnType.NumIn()
	currentMap := bus.handlerMap
	prevMaps := make([]*handlerMap, 0, argCount)

	for argIndex := 0; argIndex < argCount; argIndex++ {
		argType := fnType.In(argIndex)

		if nextMap, ok := currentMap.nextArgMap[argType]; ok {
			prevMaps = append(prevMaps, currentMap)
			currentMap = nextMap
		} else {
			return fmt.Errorf("handler %v not subscrbed", fn)
		}
	}

	if idx, handler := currentMap.findHandlerIdx(reflect.ValueOf(fn)); idx >= 0 {
		currentMap.removeHandler(idx)
		handler.bus = nil
	} else {
		return fmt.Errorf("handler %v not subscrbed", fn)
	}

	if len(currentMap.handlers)+len(currentMap.nextArgMap) == 0 {
		for argIndex := argCount - 2; argIndex >= 0; argIndex-- {
			prevMap := prevMaps[argIndex]
			delete(prevMap.nextArgMap, fnType.In(argIndex+1))
			if len(prevMap.handlers)+len(prevMap.nextArgMap) == 0 {
				break
			}
		}
	}

	return nil
}

// Publish executes callback defined for a topic. Any additional argument will be transferred to the callback.
// Handlers are snapshotted under lock, then executed without holding the lock to avoid deadlocks
// if a handler calls Publish, Subscribe, or Unsubscribe. Once-handlers are removed from the
// original map under lock after execution completes.
func (bus *eventBus) Publish(args ...interface{}) int {
	bus.lock.Lock()
	snapshot := bus.handlerMap.clone()
	bus.lock.Unlock()

	count := snapshot.publish(bus, args, 0)

	// Remove once-handlers from the real map under lock.
	if onceHandlers := snapshot.collectOnceHandlers(); len(onceHandlers) > 0 {
		bus.lock.Lock()
		for _, h := range onceHandlers {
			bus.handlerMap.removeOnceHandler(h.callBack)
		}
		bus.lock.Unlock()
	}

	return count
}

// WaitAsync waits for all async callbacks to complete
func (bus *eventBus) WaitAsync() {
	bus.wg.Wait()
}
