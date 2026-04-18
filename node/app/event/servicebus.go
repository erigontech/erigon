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
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/erigontech/erigon/node/app/util"
)

type ServiceBus struct {
	eventBus         EventBus
	execPool         util.ExecPool
	busMap           map[interface{}]*ManagedEventBus
	mapLock          *sync.Mutex
	registrations    map[uintptr][]interface{}
	registrationLock sync.Mutex
}

func NewServiceBus(execPool util.ExecPool) *ServiceBus {
	return &ServiceBus{NewEventBus(execPool), execPool, make(map[interface{}]*ManagedEventBus), &sync.Mutex{}, make(map[uintptr][]interface{}), sync.Mutex{}}
}

func (bus *ServiceBus) GetEventBus(key interface{}) *ManagedEventBus {
	bus.mapLock.Lock()
	defer bus.mapLock.Unlock()

	if ebus, ok := bus.busMap[key]; ok {
		return ebus
	}

	ebus := NewManagedEventBus(bus, key)
	bus.busMap[key] = ebus

	return ebus
}

func (bus *ServiceBus) Deactivate() { /*
		for (Map.Entry<Object,EventBus> busMapEntry:eventBusMap.entrySet()) {
			LOG.info("Unregistering from Event Bus: " + busMapEntry.getKey() );
			 busMapEntry.getValue().unregister(this);
		}
	*/
}

// objectPointer returns reflect.Value.Pointer() if object has a kind that
// supports it (pointer-like). Returns false otherwise so callers can return a
// descriptive error rather than panic inside reflect.
func objectPointer(object interface{}) (uintptr, bool) {
	v := reflect.ValueOf(object)
	switch v.Kind() {
	case reflect.Pointer, reflect.Chan, reflect.Map, reflect.Func, reflect.Slice, reflect.UnsafePointer:
		return v.Pointer(), true
	default:
		return 0, false
	}
}

func (bus *ServiceBus) Register(object interface{}, fns ...interface{}) (err error) {
	objectPtr, ok := objectPointer(object)
	if !ok {
		return fmt.Errorf("ServiceBus.Register: object kind %s is not supported; pass a pointer", reflect.ValueOf(object).Kind())
	}
	bus.registrationLock.Lock()
	defer bus.registrationLock.Unlock()
	for _, fn := range fns {
		if reflect.TypeOf(fn).Kind() != reflect.Func {
			return fmt.Errorf("invalid type: %T should be func", fn)
		}
		bus.registrations[objectPtr] = append(bus.registrations[objectPtr], fn)
		err = bus.eventBus.SubscribeAsync(fn)
		if err != nil {
			break
		}
	}

	return err
}

func (bus *ServiceBus) UnregisterAll(object interface{}) error {
	objectPtr, ok := objectPointer(object)
	if !ok {
		return fmt.Errorf("ServiceBus.UnregisterAll: object kind %s is not supported; pass a pointer", reflect.ValueOf(object).Kind())
	}

	bus.registrationLock.Lock()
	defer bus.registrationLock.Unlock()

	fns := bus.registrations[objectPtr]
	var errs []error
	for _, fn := range fns {
		if err := bus.eventBus.Unsubscribe(fn); err != nil {
			errs = append(errs, err)
		}
	}
	delete(bus.registrations, objectPtr)

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (bus *ServiceBus) Unregister(object interface{}, fns ...interface{}) error {
	objectPtr, ok := objectPointer(object)
	if !ok {
		return fmt.Errorf("ServiceBus.Unregister: object kind %s is not supported; pass a pointer", reflect.ValueOf(object).Kind())
	}
	bus.registrationLock.Lock()
	if registrations, ok := bus.registrations[objectPtr]; ok && len(registrations) > 0 {
		for _, fn := range fns {
			removeRegistration(bus.registrations, objectPtr, findRegistrationIndex(bus.registrations, objectPtr, fn))
		}
	}
	bus.registrationLock.Unlock()

	var errs []error
	for _, fn := range fns {
		if err := bus.eventBus.Unsubscribe(fn); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}

func (bus *ServiceBus) Registrations(object interface{}) []interface{} {
	objectPtr, ok := objectPointer(object)
	if !ok {
		return nil
	}
	bus.registrationLock.Lock()
	defer bus.registrationLock.Unlock()
	// Return a copy so callers cannot mutate the internal slice without the lock.
	src := bus.registrations[objectPtr]
	if len(src) == 0 {
		return nil
	}
	out := make([]interface{}, len(src))
	copy(out, src)
	return out
}

func (bus *ServiceBus) Post(args ...interface{}) int {
	return bus.eventBus.Publish(args...)
}
