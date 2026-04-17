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
)

type ManagedEventBus struct {
	serviceBus       *ServiceBus
	eventBus         EventBus
	key              interface{}
	registrations    map[uintptr][]interface{}
	registrationLock sync.Mutex
}

func NewManagedEventBus(serviceBus *ServiceBus, key interface{}) *ManagedEventBus {
	return &ManagedEventBus{serviceBus, NewEventBus(serviceBus.execPool), key, make(map[uintptr][]interface{}), sync.Mutex{}}
}

func (bus *ManagedEventBus) String() string {
	return fmt.Sprintf("ManagedEventBus [key=%v]", bus.key)
}

func (bus *ManagedEventBus) Register(object interface{}, fns ...interface{}) (err error) {
	objectVal := reflect.ValueOf(object)
	if len(fns) == 0 {
		for i := 0; i < objectVal.NumMethod(); i++ {
			if method := objectVal.Method(i); method.Type().NumIn() > 0 && method.Type().NumOut() == 0 {
				fns = append(fns, method.Interface())
			}
		}
	}
	// reflect.Value.Pointer() only supports kinds with an addressable pointer.
	// Reject others with a descriptive error instead of panicking.
	switch objectVal.Kind() {
	case reflect.Pointer, reflect.Chan, reflect.Map, reflect.Func, reflect.Slice, reflect.UnsafePointer:
	default:
		return fmt.Errorf("ManagedEventBus.Register: object kind %s is not supported; pass a pointer", objectVal.Kind())
	}
	objectPtr := objectVal.Pointer()

	bus.registrationLock.Lock()
	for _, fn := range fns {
		bus.registrations[objectPtr] = append(bus.registrations[objectPtr], fn)
		err = bus.eventBus.SubscribeAsync(fn)
		if err != nil {
			break
		}
	}
	bus.registrationLock.Unlock()
	return err
}

func (bus *ManagedEventBus) UnregisterAll(object interface{}) error {
	objectVal := reflect.ValueOf(object)
	switch objectVal.Kind() {
	case reflect.Pointer, reflect.Chan, reflect.Map, reflect.Func, reflect.Slice, reflect.UnsafePointer:
	default:
		return fmt.Errorf("ManagedEventBus.UnregisterAll: object kind %s is not supported; pass a pointer", objectVal.Kind())
	}
	objectPtr := objectVal.Pointer()

	bus.registrationLock.Lock()
	for _, fn := range bus.registrations[objectPtr] {
		err := bus.eventBus.Unsubscribe(fn)

		if err != nil {
			return err
		}
	}
	delete(bus.registrations, objectPtr)
	bus.registrationLock.Unlock()
	return nil
}

func (bus *ManagedEventBus) Unregister(object interface{}, fn interface{}) error {
	objectPtr := reflect.ValueOf(object).Pointer()
	bus.registrationLock.Lock()
	if registrations, ok := bus.registrations[objectPtr]; ok && len(registrations) > 0 {
		removeRegistration(bus.registrations, objectPtr, findRegistrationIndex(bus.registrations, objectPtr, fn))
	}
	bus.registrationLock.Unlock()
	return bus.eventBus.Unsubscribe(fn)
}

func (bus *ManagedEventBus) Post(args ...interface{}) int {
	return bus.eventBus.Publish(args...)
}

func removeRegistration(registrations map[uintptr][]interface{}, objectPtr uintptr, idx int) {
	if _, ok := registrations[objectPtr]; !ok || idx < 0 {
		return
	}
	l := len(registrations[objectPtr])

	copy(registrations[objectPtr][idx:], registrations[objectPtr][idx+1:])
	registrations[objectPtr][l-1] = nil
	registrations[objectPtr] = registrations[objectPtr][:l-1]
}

func findRegistrationIndex(registrations map[uintptr][]interface{}, objectPtr uintptr, fn interface{}) int {
	if _, ok := registrations[objectPtr]; ok {
		for idx, subscription := range registrations[objectPtr] {
			//fmt.Printf("%v=%v (%v)\n", subscription, fn, reflect.ValueOf(subscription).Pointer() == reflect.ValueOf(fn).Pointer())
			if reflect.ValueOf(subscription) == reflect.ValueOf(fn) {
				return idx
			}
		}
	}
	return -1
}
