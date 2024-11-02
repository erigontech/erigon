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
	pending    []reflect.SelectCase
	active     []reflect.SelectCase
	mutex      sync.Mutex
	context    context.Context
	cancelFunc context.CancelFunc
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

func (mux *ChannelGroup) Wait(chanFunc func(interface{}) (bool, bool), errorFunc func(error)) bool {

	inputCount := 0
	hasResult := false

	for {
		mux.mutex.Lock()
		mux.active = append(mux.active, mux.pending...)
		inputCount += len(mux.pending)
		mux.pending = nil
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

		chosen, recv, recvOK := reflect.Select(mux.active)
		if recvOK {

			var gotResult, done bool

			if chosen != 0 {
				gotResult, done = chanFunc(recv.Interface())
			}

			if gotResult {
				hasResult = true
			}

			if done || chosen == 0 {
				return hasResult
			}

		} else {
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

type resultMultiplexer[T any] struct {
	channels *ChannelGroup
}

func ResultMultiplexer[T any](waitContext context.Context) *resultMultiplexer[T] {
	return &resultMultiplexer[T]{
		channels: NewChannelGroup(waitContext),
	}

}

func (mux *resultMultiplexer[T]) Add(cres chan T, cerr chan error) *resultMultiplexer[T] {
	mux.channels.
		Add(cres).
		Add(cerr)

	return mux
}

func (mux *resultMultiplexer[T]) Context() context.Context {
	return mux.channels.Context()
}

func (mux *resultMultiplexer[T]) Cancel() {
	mux.channels.Cancel()
}

func (mux *resultMultiplexer[T]) Done() bool {
	return mux.channels.Done()
}

func (mux *resultMultiplexer[T]) AwaitOne() (res T, err error) {
	mux.channels.Wait(func(ichan interface{}) (bool, bool) {
		switch crecv := ichan.(type) {
		case chan error:
			err = <-crecv
			return false, false
		case chan interface{}:
			val := <-crecv
			res = val.(T)
			mux.channels.Cancel()
			return true, true
		default:
			if crecv == mux.channels.Context().Done() {
				err = mux.channels.Context().Err()
				return false, true
			}

			return false, false
		}
	}, nil)

	return res, err
}

func (mux *resultMultiplexer[T]) AwaitAll() (results []T, errors []error) {
	mux.channels.Wait(func(ichan interface{}) (bool, bool) {
		switch crecv := ichan.(type) {
		case chan error:
			err := <-crecv
			errors = append(errors, err)
			return false, false
		case chan interface{}:
			res := <-crecv
			results = append(results, res.(T))
			return true, false
		default:
			if crecv == mux.channels.Context().Done() {
				errors = append(errors, mux.channels.Context().Err())
				return false, true
			}

			return false, false
		}
	}, nil)

	return results, errors
}

func (mux *resultMultiplexer[T]) Select() (chan T, chan error) {
	cres, cerr := MakeResultChannels[T]()

	go func() {
		mux.channels.Wait(func(ichan interface{}) (bool, bool) {
			switch crecv := ichan.(type) {
			case chan error:
				err := <-crecv
				cerr <- err
				return false, false
			case chan interface{}:
				res := <-crecv
				cres <- res.(T)
				return true, false
			default:
				if crecv == mux.channels.Context().Done() {
					cerr <- mux.channels.Context().Err()
					return false, true
				}

				return false, false
			}
		}, nil)

		CloseResultChannels(cres, cerr)
	}()

	return cres, cerr
}

type errorMultiplexer struct {
	channels *ChannelGroup
}

func ErrorMultiplexer(waitContext context.Context) *errorMultiplexer {
	return &errorMultiplexer{
		channels: NewChannelGroup(waitContext),
	}
}

func (mux *errorMultiplexer) Add(cerr chan error) *errorMultiplexer {
	mux.channels.
		Add(cerr)

	return mux
}

func (mux *errorMultiplexer) Context() context.Context {
	return mux.channels.Context()
}

func (mux *errorMultiplexer) Cancel() {
	mux.channels.Cancel()
}

func (mux *errorMultiplexer) Done() bool {
	return mux.channels.Done()
}

func (mux *errorMultiplexer) AwaitOne() (err error) {
	mux.channels.Wait(func(ichan interface{}) (bool, bool) {
		switch crecv := ichan.(type) {
		case chan error:
			err = <-crecv
			return true, true
		default:
			if crecv == mux.channels.Context().Done() {
				err = mux.channels.Context().Err()
				return true, true
			}

			return false, false
		}
	}, nil)

	return err
}

func (mux *errorMultiplexer) AwaitAll() (errors []error) {

	mux.channels.Wait(func(ichan interface{}) (bool, bool) {
		switch crecv := ichan.(type) {
		case chan error:
			err := <-crecv
			errors = append(errors, err)
			return true, false
		default:
			if crecv == mux.channels.Context().Done() {
				errors = append(errors, mux.channels.Context().Err())
				return true, true
			}

			return false, false
		}
	}, nil)

	return errors
}

func (mux *errorMultiplexer) Select() chan error {
	cerr := make(chan error, 1)

	go func() {
		mux.channels.Wait(func(ichan interface{}) (bool, bool) {
			switch crecv := ichan.(type) {
			case chan error:
				err := <-crecv
				cerr <- err
				return true, false
			default:
				if crecv == mux.channels.Context().Done() {
					cerr <- mux.channels.Context().Err()
					return true, true
				}

				return false, false
			}
		}, nil)

		close(cerr)
	}()

	return cerr
}

func AwaitError(cerr chan error) (err error) {
	if cerr == nil {
		return fmt.Errorf("Await Failed: channel is undefined")
	}

	return <-cerr
}

func AwaitErrors(waitContext context.Context, cerrs ...chan error) (err error) {
	var chans []chan error

	for _, cerr := range cerrs {
		if cerr != nil {
			chans = append(chans, cerr)
		}
	}

	if len(chans) == 0 {
		return fmt.Errorf("Await Failed: no channels to await")
	}

	mux := ErrorMultiplexer(waitContext)

	for _, cerr := range chans {
		if cerr != nil {
			mux.Add(cerr)
		}
	}

	if errs := mux.AwaitAll(); len(errs) > 0 {
		return Errors(errs)
	}

	return nil
}

type ExitFunc func(exitContext context.Context) []chan error

func AwaitExit(exitContext context.Context, exitFuncs ...ExitFunc) error {
	var errorChannels []chan error

	for _, exitFunc := range exitFuncs {
		errorChannels = append(errorChannels, exitFunc(exitContext)...)
	}

	return AwaitErrors(exitContext, errorChannels...)
}
