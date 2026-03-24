package util

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

func IsNil[T any](t T) bool {
	v := reflect.ValueOf(t)
	kind := v.Kind()
	// Must be one of these types to be nillable
	return (kind == reflect.Ptr ||
		kind == reflect.Interface ||
		kind == reflect.Slice ||
		kind == reflect.Map ||
		kind == reflect.Chan ||
		kind == reflect.Func) &&
		v.IsNil()
}

func Must[T any](result T, err error) T {
	if err != nil {
		panic(fmt.Sprintf("%v Failed: %s", reflect.TypeOf(result), err))
	}

	return result
}

func MustAwait[T any](cres chan T, cerr chan error) T {
	return Must(Await[T](cres, cerr))
}

type AwaitHandler[T any] interface {
	Handle(result T, err error) (T, error)
}

type AwaitHandlerFunc[T any] func(result T, err error) (T, error)

func (f AwaitHandlerFunc[T]) Handle(result T, err error) (T, error) {
	return f(result, err)
}

func Await[T any](cres chan T, cerr chan error, handler ...AwaitHandler[T]) (result T, err error) {
	if cres == nil || cerr == nil {
		return result, fmt.Errorf("Await Failed: channels are undefined")
	}

	awaitResult := true
	awaitError := true

	for awaitResult || awaitError {
		select {
		case res, ok := <-cres:
			awaitResult = false
			if ok {
				var err error
				for _, h := range handler {
					res, err = h.Handle(res, err)
				}

				return res, err
			}
		case err, ok := <-cerr:
			awaitError = false
			if ok {
				for _, h := range handler {
					_, err = h.Handle(result, err)
				}

				return result, err
			}
		}
	}

	return result, err
}

func MakeResultChannels[T any]() (chan T, chan error) {
	return make(chan T, 1), make(chan error, 1)
}

func CloseResultChannels[T any](cres chan T, cerr chan error) (chan T, chan error) {
	close(cres)
	close(cerr)
	return cres, cerr
}

func ReturnResultChannels[T any](res T, err error) (chan T, chan error) {
	cres, cerr := MakeResultChannels[T]()

	if err != nil {
		cerr <- err
	} else {
		cres <- res
	}

	return CloseResultChannels(cres, cerr)
}

func NopErrorChannel() chan error {
	cerr := make(chan error, 1)
	close(cerr)
	return cerr
}

type ChannelGroup struct {
	pending       []reflect.SelectCase
	active        []reflect.SelectCase
	pendingRemove []reflect.Value
	mutex         sync.Mutex
	context       context.Context
	cancelFunc    context.CancelFunc
}

func NewChannelGroup(waitContext context.Context) *ChannelGroup {
	mux := &ChannelGroup{
		mutex: sync.Mutex{},
	}

	mux.context, mux.cancelFunc = context.WithCancel(waitContext)
	mux.pending = []reflect.SelectCase{
		{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(mux.context.Done()),
		}}

	return mux
}

func (mux *ChannelGroup) Add(ichan interface{}) *ChannelGroup {
	mux.mutex.Lock()
	mux.pending = append(mux.pending, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ichan),
	})
	mux.mutex.Unlock()
	return mux
}

func (mux *ChannelGroup) Remove(ichan interface{}) *ChannelGroup {
	mux.mutex.Lock()
	mux.pendingRemove = append(mux.pendingRemove, reflect.ValueOf(ichan))
	mux.mutex.Unlock()
	return mux
}

func (mux *ChannelGroup) Wait(chanFunc func(interface{}, interface{}, bool) (bool, bool), errorFunc func(error)) bool {
	if mux == nil {
		return false
	}

	inputCount := 0
	hasResult := false

WAIT:
	for {
		mux.mutex.Lock()
		mux.active = append(mux.active, mux.pending...)
		inputCount += len(mux.pending)
		mux.pending = nil
		var active []reflect.SelectCase
		for _, a := range mux.active {
			var remove bool
			for _, r := range mux.pendingRemove {
				if a.Chan == r {
					remove = true
					break
				}
			}

			if !remove {
				active = append(active, a)
			} else {
				inputCount--
			}
		}
		mux.pendingRemove = nil
		mux.active = active
		mux.mutex.Unlock()

		if inputCount <= 1 {
			return hasResult
		}

		if mux.context.Err() != nil {
			if errorFunc != nil {
				errorFunc(mux.context.Err())
			}
			return hasResult
		}

		if chosen, recv, recvOK := reflect.Select(mux.active); recvOK {
			var gotResult, done bool

			if chosen != 0 {
				for _, r := range mux.pendingRemove {
					if mux.active[chosen].Chan == r {
						continue WAIT
					}
				}

				gotResult, done = chanFunc(mux.active[chosen].Chan.Interface(), recv.Interface(), true)
			}
			if gotResult {
				hasResult = true
			}
			if done || chosen == 0 {
				return hasResult
			}
		} else {
			if chosen == 0 {
				return hasResult
			}
			gotResult, done := chanFunc(mux.active[chosen].Chan.Interface(), recv.Interface(), false)
			if gotResult {
				hasResult = true
			}
			if done {
				return hasResult
			}
			mux.active[chosen].Chan = reflect.ValueOf(nil)
			inputCount--
		}
	}
}

func (mux *ChannelGroup) Cancel() {
	mux.cancelFunc()
}

func (mux *ChannelGroup) Done() bool {
	return mux.context.Err() != nil
}

func (mux *ChannelGroup) Context() context.Context {
	return mux.context
}

func AwaitError(cerr chan error) (err error) {
	if cerr == nil {
		return fmt.Errorf("Await Failed: channel is undefined")
	}
	return <-cerr
}
