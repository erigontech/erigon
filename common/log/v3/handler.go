package log

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/go-stack/stack"
)

// Handler interface defines where and how log records are written.
// A logger prints its log records by writing to a Handler.
// Handlers are composable, providing you great flexibility in combining
// them to achieve the logging structure that suits your applications.
type Handler interface {
	Log(r *Record) error
	// Enabled reports whether the handler handles records at the given level.
	// The handler ignores records whose level is higher.
	// It is called early, before any arguments are processed,
	// to save effort if the log event should be discarded.
	// If called from a Logger method, the first argument is the context
	// passed to that method, or context.Background() if nil was passed
	// or the method does not take a context.
	// The context is passed so Enabled can use its values
	// to make a decision.
	Enabled(ctx context.Context, lvl Lvl) bool
}

// StreamHandler writes log records to an io.Writer
// with the given format. StreamHandler can be used
// to easily begin writing log records to other
// outputs.
//
// StreamHandler wraps itself with LazyHandler and SyncHandler
// to evaluate Lazy objects and perform safe concurrent writes.
func StreamHandler(wr io.Writer, fmtr Format) Handler {
	return LazyHandler(SyncHandler(streamHandler{wr: wr, fmtr: fmtr}))
}

type streamHandler struct {
	wr   io.Writer
	fmtr Format
}

func (h streamHandler) Log(r *Record) error {
	_, err := h.wr.Write(h.fmtr.Format(r))
	return err
}

func (h streamHandler) Enabled(ctx context.Context, lvl Lvl) bool {
	return true
}

// SyncHandler can be wrapped around a handler to guarantee that
// only a single Log operation can proceed at a time. It's necessary
// for thread-safe concurrent writes.
func SyncHandler(h Handler) Handler {
	var mu sync.Mutex
	return syncHandler{mu: &mu, h: h}
}

type syncHandler struct {
	mu *sync.Mutex
	h  Handler
}

func (h syncHandler) Log(r *Record) error {
	defer h.mu.Unlock()
	h.mu.Lock()
	return h.h.Log(r)
}

func (h syncHandler) Enabled(ctx context.Context, lvl Lvl) bool {
	return h.h.Enabled(ctx, lvl)
}

const DefaultLogMaxSize = 1 << 27 // 128 Mb

// FileHandler returns a handler which writes log records to the give file
// using the given format. If the path
// already exists, FileHandler will append to the given file. If it does not,
// FileHandler will create the file with mode 0644.
func FileHandler(path string, fmtr Format, maxFileSize uint64) (Handler, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	rotating := &rotatingWriter{
		file:       f,
		logMaxSize: maxFileSize,
	}
	return closingHandler{rotating, StreamHandler(rotating, fmtr)}, nil
}

type rotatingWriter struct {
	file       *os.File
	logMaxSize uint64
}

// Write checks if current log size + expected write size is larger than limit.
// If limit outreached, file is truncated then write is called.
func (r *rotatingWriter) Write(p []byte) (n int, err error) {
	info, err := r.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("rotating log %q stat: %w", r.file.Name(), err)
	}
	if uint64(info.Size())+uint64(len(p)) > r.logMaxSize {
		if err := r.file.Truncate(0); err != nil {
			return 0, fmt.Errorf("rotating log %q truncating: %w", r.file.Name(), err)
		}
	}
	n, err = r.file.Write(p)
	if err != nil {
		return 0, fmt.Errorf("rotating log %q write: %w", r.file.Name(), err)
	}
	if err := r.file.Sync(); err != nil {
		return 0, fmt.Errorf("rotating log %q sync: %w", r.file.Name(), err)
	}
	return n, nil
}

func (r *rotatingWriter) Close() error {
	return r.file.Close()
}

// NetHandler opens a socket to the given address and writes records
// over the connection.
func NetHandler(network, addr string, fmtr Format) (Handler, error) {
	conn, err := net.Dial(network, addr) //nolint:noctx
	if err != nil {
		return nil, err
	}

	return closingHandler{conn, StreamHandler(conn, fmtr)}, nil
}

// XXX: closingHandler is essentially unused at the moment
// it's meant for a future time when the Handler interface supports
// a possible Close() operation
type closingHandler struct {
	io.WriteCloser
	Handler
}

func (h *closingHandler) Close() error {
	return h.WriteCloser.Close()
}

// CallerFileHandler returns a Handler that adds the line number and file of
// the calling function to the context with key "caller".
func CallerFileHandler(h Handler) Handler {
	return callerFileHandler{h: h}
}

type callerFileHandler struct {
	h Handler
}

func (h callerFileHandler) Log(r *Record) error {
	r.Ctx = append(r.Ctx, "caller", fmt.Sprint(r.Call(5)))
	return h.h.Log(r)
}

func (h callerFileHandler) Enabled(ctx context.Context, lvl Lvl) bool {
	return h.h.Enabled(ctx, lvl)
}

// CallerFuncHandler returns a Handler that adds the calling function name to
// the context with key "fn".
func CallerFuncHandler(h Handler) Handler {
	return callerFuncHandler{h: h}
}

type callerFuncHandler struct {
	h Handler
}

func (h callerFuncHandler) Log(r *Record) error {
	r.Ctx = append(r.Ctx, "fn", fmt.Sprintf("%+n", r.Call(5)))
	return h.h.Log(r)
}

func (h callerFuncHandler) Enabled(ctx context.Context, lvl Lvl) bool {
	return h.h.Enabled(ctx, lvl)
}

// CallerStackHandler returns a Handler that adds a stack trace to the context
// with key "stack". The stack trace is formatted as a space separated list of
// call sites inside matching []'s. The most recent call site is listed first.
// Each call site is formatted according to format. See the documentation of
// package github.com/go-stack/stack for the list of supported formats.
func CallerStackHandler(format string, h Handler) Handler {
	return callerStackHandler{format: format, h: h}
}

type callerStackHandler struct {
	format string
	h      Handler
}

func (h callerStackHandler) Log(r *Record) error {
	s := stack.Trace().TrimBelow(r.Call(5)).TrimRuntime()
	if len(s) > 0 {
		r.Ctx = append(r.Ctx, "stack", fmt.Sprintf(h.format, s))
	}
	return h.h.Log(r)
}

func (h callerStackHandler) Enabled(ctx context.Context, lvl Lvl) bool {
	return h.h.Enabled(ctx, lvl)
}

// FilterHandler returns a Handler that only writes records to the
// wrapped Handler if the given function evaluates true. For example,
// to only log records where the 'err' key is not nil:
//
//	logger.SetHandler(FilterHandler(func(r *Record) bool {
//	    for i := 0; i < len(r.Ctx); i += 2 {
//	        if r.Ctx[i] == "err" {
//	            return r.Ctx[i+1] != nil
//	        }
//	    }
//	    return false
//	}, h))
func FilterHandler(fn func(r *Record) bool, h Handler) Handler {
	return filterHandler{fn: fn, h: h}
}

type filterHandler struct {
	fn func(r *Record) bool
	h  Handler
}

func (h filterHandler) Log(r *Record) error {
	if h.fn(r) {
		return h.h.Log(r)
	}
	return nil
}

func (h filterHandler) Enabled(ctx context.Context, lvl Lvl) bool {
	return h.h.Enabled(ctx, lvl)
}

// MatchFilterHandler returns a Handler that only writes records
// to the wrapped Handler if the given key in the logged
// context matches the value. For example, to only log records
// from your ui package:
//
//	log.MatchFilterHandler("pkg", "app/ui", log.StdoutHandler)
func MatchFilterHandler(key string, value interface{}, h Handler) Handler {
	return FilterHandler(func(r *Record) (pass bool) {
		switch key {
		case r.KeyNames.Lvl:
			return r.Lvl == value
		case r.KeyNames.Time:
			return r.Time == value
		case r.KeyNames.Msg:
			return r.Msg == value
		}

		for i := 0; i < len(r.Ctx); i += 2 {
			if r.Ctx[i] == key {
				return r.Ctx[i+1] == value
			}
		}
		return false
	}, h)
}

// LvlFilterHandler returns a Handler that only writes
// records which are less than the given verbosity
// level to the wrapped Handler. For example, to only
// log Error/Crit records:
//
//	log.LvlFilterHandler(log.LvlError, log.StdoutHandler)
func LvlFilterHandler(maxLvl Lvl, h Handler) Handler {
	return lvlFilterHandler{maxLvl: maxLvl, h: h}
}

type lvlFilterHandler struct {
	maxLvl Lvl
	h      Handler
}

func (h lvlFilterHandler) Log(r *Record) error {
	if h.Enabled(context.Background(), r.Lvl) {
		return h.h.Log(r)
	}
	return nil
}

func (h lvlFilterHandler) Enabled(ctx context.Context, lvl Lvl) bool {
	return lvl <= h.maxLvl
}

// MultiHandler dispatches any write to each of its handlers.
// It also provides the max log lvl across all sub-handlers.
// This is useful for writing different types of log information
// to different locations. For example, to log to a file and
// standard error:
//
//	log.MultiHandler(
//	    log.Must.FileHandler("/var/log/app.log", log.LogfmtFormat()),
//	    log.StderrHandler)
func MultiHandler(hs ...Handler) Handler {
	return multiHandler{hs: hs}
}

type multiHandler struct {
	hs []Handler
}

func (h multiHandler) Log(r *Record) error {
	var accErr error
	for i, subH := range h.hs {
		if !subH.Enabled(context.Background(), r.Lvl) {
			continue
		}
		err := subH.Log(r)
		if err == nil {
			continue
		}

		err = fmt.Errorf("handler %d failed: %w", i, err)
		if accErr == nil {
			accErr = err
		} else {
			accErr = fmt.Errorf("%w: %w", accErr, err)
		}
	}
	return accErr
}

func (h multiHandler) Enabled(ctx context.Context, lvl Lvl) bool {
	for _, subH := range h.hs {
		if subH.Enabled(ctx, lvl) {
			return true
		}
	}
	return false
}

// FailoverHandler writes all log records to the first handler
// specified, but will failover and write to the second handler if
// the first handler has failed, and so on for all handlers specified.
// It also provides the max log lvl across all failover handlers.
// For example you might want to log to a network socket, but failover
// to writing to a file if the network fails, and then to
// standard out if the file write fails:
//
//	log.FailoverHandler(
//	    log.Must.NetHandler("tcp", ":9090", log.JsonFormat()),
//	    log.Must.FileHandler("/var/log/app.log", log.LogfmtFormat()),
//	    log.StdoutHandler)
//
// All writes that do not go to the first handler will add context with keys of
// the form "failover_err_{idx}" which explain the error encountered while
// trying to write to the handlers before them in the list.
func FailoverHandler(hs ...Handler) Handler {
	return failoverHandler{hs: hs}
}

type failoverHandler struct {
	hs []Handler
}

func (h failoverHandler) Log(r *Record) error {
	var err error
	for i, subH := range h.hs {
		err = subH.Log(r)
		if err == nil {
			return nil
		}
		r.Ctx = append(r.Ctx, fmt.Sprintf("failover_err_%d", i), err)
	}
	return err
}

func (h failoverHandler) Enabled(ctx context.Context, lvl Lvl) bool {
	for _, subH := range h.hs {
		if subH.Enabled(ctx, lvl) {
			return true
		}
	}
	return false
}

// ChannelHandler writes all records to the given channel.
// It blocks if the channel is full. Useful for async processing
// of log messages, it's used by BufferedHandler.
func ChannelHandler(recs chan<- *Record) Handler {
	return channelHandler{recs: recs}
}

type channelHandler struct {
	recs chan<- *Record
}

func (h channelHandler) Log(r *Record) error {
	h.recs <- r
	return nil
}

func (h channelHandler) Enabled(ctx context.Context, lvl Lvl) bool {
	return true
}

// BufferedHandler writes all records to a buffered
// channel of the given size which flushes into the wrapped
// handler whenever it is available for writing. Since these
// writes happen asynchronously, all writes to a BufferedHandler
// never return an error and any errors from the wrapped handler are ignored.
func BufferedHandler(bufSize int, h Handler) Handler {
	recs := make(chan *Record, bufSize)
	go func() {
		for m := range recs {
			_ = h.Log(m)
		}
	}()
	return bufferedHandler{baseHandler: h, channelHandler: channelHandler{recs}}
}

type bufferedHandler struct {
	baseHandler    Handler
	channelHandler channelHandler
}

func (h bufferedHandler) Log(r *Record) error {
	return h.channelHandler.Log(r)
}

func (h bufferedHandler) Enabled(ctx context.Context, lvl Lvl) bool {
	return h.baseHandler.Enabled(ctx, lvl)
}

// LazyHandler writes all values to the wrapped handler after evaluating
// any lazy functions in the record's context. It is already wrapped
// around StreamHandler and SyslogHandler in this library, you'll only need
// it if you write your own Handler.
func LazyHandler(h Handler) Handler {
	return lazyHandler{h: h}
}

type lazyHandler struct {
	h Handler
}

func (h lazyHandler) Log(r *Record) error {
	// go through the values (odd indices) and reassign
	// the values of any lazy fn to the result of its execution
	hadErr := false
	for i := 1; i < len(r.Ctx); i += 2 {
		lz, ok := r.Ctx[i].(Lazy)
		if ok {
			v, err := evaluateLazy(lz)
			if err != nil {
				hadErr = true
				r.Ctx[i] = err
			} else {
				if cs, ok := v.(stack.CallStack); ok {
					v = cs.TrimBelow(r.Call(5)).TrimRuntime()
				}
				r.Ctx[i] = v
			}
		}
	}

	if hadErr {
		r.Ctx = append(r.Ctx, errorKey, "bad lazy")
	}

	return h.h.Log(r)
}

func (h lazyHandler) Enabled(ctx context.Context, lvl Lvl) bool {
	return h.h.Enabled(ctx, lvl)
}

func evaluateLazy(lz Lazy) (interface{}, error) {
	t := reflect.TypeOf(lz.Fn)

	if t.Kind() != reflect.Func {
		return nil, fmt.Errorf("INVALID_LAZY, not func: %+v", lz.Fn)
	}

	if t.NumIn() > 0 {
		return nil, fmt.Errorf("INVALID_LAZY, func takes args: %+v", lz.Fn)
	}

	if t.NumOut() == 0 {
		return nil, fmt.Errorf("INVALID_LAZY, no func return val: %+v", lz.Fn)
	}

	value := reflect.ValueOf(lz.Fn)
	results := value.Call([]reflect.Value{})
	if len(results) == 1 {
		return results[0].Interface(), nil
	}
	values := make([]interface{}, len(results))
	for i, v := range results {
		values[i] = v.Interface()
	}
	return values, nil
}

// DiscardHandler reports success for all writes but does nothing.
// It is useful for dynamically disabling logging at runtime via
// a Logger's SetHandler method.
func DiscardHandler() Handler {
	return discardHandler{}
}

type discardHandler struct{}

func (h discardHandler) Log(r *Record) error {
	return nil
}

func (h discardHandler) Enabled(ctx context.Context, lvl Lvl) bool {
	return false
}

// Must object provides the following Handler creation functions
// which instead of returning an error parameter only return a Handler
// and panic on failure: FileHandler, NetHandler, SyslogHandler, SyslogNetHandler
var Must muster

func must(h Handler, err error) Handler {
	if err != nil {
		panic(err)
	}
	return h
}

type muster struct{}

func (m muster) FileHandler(path string, fmtr Format) Handler {
	return must(FileHandler(path, fmtr, DefaultLogMaxSize))
}

func (m muster) NetHandler(network, addr string, fmtr Format) Handler {
	return must(NetHandler(network, addr, fmtr))
}

// swapHandler wraps another handler that may be swapped out
// dynamically at runtime in a thread-safe fashion.
type swapHandler struct {
	handler atomic.Value
}

func (h *swapHandler) Log(r *Record) error {
	return (*h.handler.Load().(*Handler)).Log(r)
}

func (h *swapHandler) Enabled(ctx context.Context, lvl Lvl) bool {
	return (*h.handler.Load().(*Handler)).Enabled(ctx, lvl)
}

func (h *swapHandler) Swap(newHandler Handler) {
	h.handler.Store(&newHandler)
}

func (h *swapHandler) Get() Handler {
	return *h.handler.Load().(*Handler)
}
