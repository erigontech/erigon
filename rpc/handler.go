// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/log/v3"
)

// handler handles JSON-RPC messages. There is one handler per connection. Note that
// handler is not safe for concurrent use. Message handling never blocks indefinitely
// because RPCs are processed on background goroutines launched by handler.
//
// The entry points for incoming messages are:
//
//	h.handleMsg(message)
//	h.handleBatch(message)
//
// Outgoing calls use the requestOp struct. Register the request before sending it
// on the connection:
//
//	op := &requestOp{ids: ...}
//	h.addRequestOp(op)
//
// Now send the request, then wait for the reply to be delivered through handleMsg:
//
//	if err := op.wait(...); err != nil {
//	    h.removeRequestOp(op) // timeout, etc.
//	}
type handler struct {
	reg            *serviceRegistry
	unsubscribeCb  *callback
	idgen          func() ID                      // subscription ID generator
	respWait       map[string]*requestOp          // active client requests
	clientSubs     map[string]*ClientSubscription // active client subscriptions
	callWG         sync.WaitGroup                 // pending call goroutines
	rootCtx        context.Context                // canceled by close()
	cancelRoot     func()                         // cancel function for rootCtx
	conn           jsonWriter                     // where responses will be sent
	logger         log.Logger
	allowSubscribe bool

	allowList     AllowList // a list of explicitly allowed methods, if empty -- everything is allowed
	forbiddenList ForbiddenList

	subLock             sync.Mutex
	serverSubs          map[ID]*Subscription
	maxBatchConcurrency uint
	traceRequests       bool
}

type callProc struct {
	ctx       context.Context
	notifiers []*Notifier
}

func HandleError(err error, stream *jsoniter.Stream) error {
	if err != nil {
		//return msg.errorResponse(err)
		stream.WriteObjectField("error")
		stream.WriteObjectStart()
		stream.WriteObjectField("code")
		ec, ok := err.(Error)
		if ok {
			stream.WriteInt(ec.ErrorCode())
		} else {
			stream.WriteInt(defaultErrorCode)
		}
		stream.WriteMore()
		stream.WriteObjectField("message")
		stream.WriteString(fmt.Sprintf("%v", err))
		de, ok := err.(DataError)
		if ok {
			stream.WriteMore()
			stream.WriteObjectField("data")
			data, derr := json.Marshal(de.ErrorData())
			if derr == nil {
				stream.Write(data)
			} else {
				stream.WriteString(fmt.Sprintf("%v", derr))
			}
		}
		stream.WriteObjectEnd()
	}

	return nil
}

func newHandler(connCtx context.Context, conn jsonWriter, idgen func() ID, reg *serviceRegistry, allowList AllowList, maxBatchConcurrency uint, traceRequests bool, logger log.Logger) *handler {
	rootCtx, cancelRoot := context.WithCancel(connCtx)
	forbiddenList := newForbiddenList()
	h := &handler{
		reg:            reg,
		idgen:          idgen,
		conn:           conn,
		respWait:       make(map[string]*requestOp),
		clientSubs:     make(map[string]*ClientSubscription),
		rootCtx:        rootCtx,
		cancelRoot:     cancelRoot,
		allowSubscribe: true,
		serverSubs:     make(map[ID]*Subscription),
		logger:         logger,
		allowList:      allowList,
		forbiddenList:  forbiddenList,

		maxBatchConcurrency: maxBatchConcurrency,
		traceRequests:       traceRequests,
	}

	if conn.remoteAddr() != "" {
		h.logger = h.logger.New("conn", conn.remoteAddr())
	}
	h.unsubscribeCb = newCallback(reflect.Value{}, reflect.ValueOf(h.unsubscribe), "unsubscribe")
	return h
}

// handleBatch executes all messages in a batch and returns the responses.
func (h *handler) handleBatch(msgs []*jsonrpcMessage) {
	// Emit error response for empty batches:
	if len(msgs) == 0 {
		h.startCallProc(func(cp *callProc) {
			h.conn.writeJSON(cp.ctx, errorMessage(&invalidRequestError{"empty batch"}))
		})
		return
	}

	// Handle non-call messages first:
	calls := make([]*jsonrpcMessage, 0, len(msgs))
	for _, msg := range msgs {
		if handled := h.handleImmediate(msg); !handled {
			calls = append(calls, msg)
		}
	}
	if len(calls) == 0 {
		return
	}
	// Process calls on a goroutine because they may block indefinitely:
	h.startCallProc(func(cp *callProc) {
		// All goroutines will place results right to this array. Because requests order must match reply orders.
		answersWithNils := make([]interface{}, len(msgs))
		// Bounded parallelism pattern explanation https://blog.golang.org/pipelines#TOC_9.
		boundedConcurrency := make(chan struct{}, h.maxBatchConcurrency)
		defer close(boundedConcurrency)
		wg := sync.WaitGroup{}
		wg.Add(len(msgs))
		for i := range calls {
			boundedConcurrency <- struct{}{}
			go func(i int) {
				defer func() {
					wg.Done()
					<-boundedConcurrency
				}()

				select {
				case <-cp.ctx.Done():
					return
				default:
				}

				buf := bytes.NewBuffer(nil)
				stream := jsoniter.NewStream(jsoniter.ConfigDefault, buf, 4096)
				if res := h.handleCallMsg(cp, calls[i], stream); res != nil {
					answersWithNils[i] = res
				}
				_ = stream.Flush()
				if buf.Len() > 0 && answersWithNils[i] == nil {
					answersWithNils[i] = json.RawMessage(buf.Bytes())
				}
			}(i)
		}
		wg.Wait()
		answers := make([]interface{}, 0, len(msgs))
		for _, answer := range answersWithNils {
			if answer != nil {
				answers = append(answers, answer)
			}
		}
		h.addSubscriptions(cp.notifiers)
		if len(answers) > 0 {
			h.conn.writeJSON(cp.ctx, answers)
		}
		for _, n := range cp.notifiers {
			n.activate()
		}
	})
}

// handleMsg handles a single message.
func (h *handler) handleMsg(msg *jsonrpcMessage, stream *jsoniter.Stream) {
	if ok := h.handleImmediate(msg); ok {
		return
	}
	h.startCallProc(func(cp *callProc) {
		needWriteStream := false
		if stream == nil {
			stream = jsoniter.NewStream(jsoniter.ConfigDefault, nil, 4096)
			needWriteStream = true
		}
		answer := h.handleCallMsg(cp, msg, stream)
		h.addSubscriptions(cp.notifiers)
		if answer != nil {
			buffer, _ := json.Marshal(answer)
			stream.Write(buffer)
		}
		if needWriteStream {
			h.conn.writeJSON(cp.ctx, json.RawMessage(stream.Buffer()))
		} else {
			stream.Write([]byte("\n"))
		}
		for _, n := range cp.notifiers {
			n.activate()
		}
	})
}

// close cancels all requests except for inflightReq and waits for
// call goroutines to shut down.
func (h *handler) close(err error, inflightReq *requestOp) {
	h.cancelAllRequests(err, inflightReq)
	h.callWG.Wait()
	h.cancelRoot()
	h.cancelServerSubscriptions(err)
}

// addRequestOp registers a request operation.
func (h *handler) addRequestOp(op *requestOp) {
	for _, id := range op.ids {
		h.respWait[string(id)] = op
	}
}

// removeRequestOps stops waiting for the given request IDs.
func (h *handler) removeRequestOp(op *requestOp) {
	for _, id := range op.ids {
		delete(h.respWait, string(id))
	}
}

// cancelAllRequests unblocks and removes pending requests and active subscriptions.
func (h *handler) cancelAllRequests(err error, inflightReq *requestOp) {
	didClose := make(map[*requestOp]bool)
	if inflightReq != nil {
		didClose[inflightReq] = true
	}

	for id, op := range h.respWait {
		// Remove the op so that later calls will not close op.resp again.
		delete(h.respWait, id)

		if !didClose[op] {
			op.err = err
			close(op.resp)
			didClose[op] = true
		}
	}
	for id, sub := range h.clientSubs {
		delete(h.clientSubs, id)
		sub.quitWithError(false, err)
	}
}

func (h *handler) addSubscriptions(nn []*Notifier) {
	h.subLock.Lock()
	defer h.subLock.Unlock()

	for _, n := range nn {
		if sub := n.takeSubscription(); sub != nil {
			h.serverSubs[sub.ID] = sub
		}
	}
}

// cancelServerSubscriptions removes all subscriptions and closes their error channels.
func (h *handler) cancelServerSubscriptions(err error) {
	h.subLock.Lock()
	defer h.subLock.Unlock()

	for id, s := range h.serverSubs {
		s.err <- err
		close(s.err)
		delete(h.serverSubs, id)
	}
}

// startCallProc runs fn in a new goroutine and starts tracking it in the h.calls wait group.
func (h *handler) startCallProc(fn func(*callProc)) {
	h.callWG.Add(1)
	go func() {
		ctx, cancel := context.WithCancel(h.rootCtx)
		defer h.callWG.Done()
		defer cancel()
		fn(&callProc{ctx: ctx})
	}()
}

// handleImmediate executes non-call messages. It returns false if the message is a
// call or requires a reply.
func (h *handler) handleImmediate(msg *jsonrpcMessage) bool {
	start := time.Now()
	switch {
	case msg.isNotification():
		if strings.HasSuffix(msg.Method, notificationMethodSuffix) {
			h.handleSubscriptionResult(msg)
			return true
		}
		return false
	case msg.isResponse():
		h.handleResponse(msg)
		h.logger.Trace("Handled RPC response", "reqid", idForLog{msg.ID}, "t", time.Since(start))
		return true
	default:
		return false
	}
}

// handleSubscriptionResult processes subscription notifications.
func (h *handler) handleSubscriptionResult(msg *jsonrpcMessage) {
	var result subscriptionResult
	if err := json.Unmarshal(msg.Params, &result); err != nil {
		h.logger.Trace("Dropping invalid subscription message")
		return
	}
	if h.clientSubs[result.ID] != nil {
		h.clientSubs[result.ID].deliver(result.Result)
	}
}

// handleResponse processes method call responses.
func (h *handler) handleResponse(msg *jsonrpcMessage) {
	op := h.respWait[string(msg.ID)]
	if op == nil {
		h.logger.Trace("Unsolicited RPC response", "reqid", idForLog{msg.ID})
		return
	}
	delete(h.respWait, string(msg.ID))
	// For normal responses, just forward the reply to Call/BatchCall.
	if op.sub == nil {
		op.resp <- msg
		return
	}
	// For subscription responses, start the subscription if the server
	// indicates success. EthSubscribe gets unblocked in either case through
	// the op.resp channel.
	defer close(op.resp)
	if msg.Error != nil {
		op.err = msg.Error
		return
	}
	if op.err = json.Unmarshal(msg.Result, &op.sub.subid); op.err == nil {
		go op.sub.start()
		h.clientSubs[op.sub.subid] = op.sub
	}
}

// handleCallMsg executes a call message and returns the answer.
func (h *handler) handleCallMsg(ctx *callProc, msg *jsonrpcMessage, stream *jsoniter.Stream) *jsonrpcMessage {
	start := time.Now()
	switch {
	case msg.isNotification():
		h.handleCall(ctx, msg, stream)
		if h.traceRequests {
			h.logger.Info("Served", "t", time.Since(start), "method", msg.Method, "params", string(msg.Params))
		} else {
			h.logger.Trace("Served", "t", time.Since(start), "method", msg.Method, "params", string(msg.Params))
		}
		return nil
	case msg.isCall():
		resp := h.handleCall(ctx, msg, stream)
		if resp != nil && resp.Error != nil {
			if resp.Error.Data != nil {
				h.logger.Warn("Served", "method", msg.Method, "reqid", idForLog{msg.ID}, "t", time.Since(start),
					"err", resp.Error.Message, "errdata", resp.Error.Data)
			} else {
				h.logger.Warn("Served", "method", msg.Method, "reqid", idForLog{msg.ID}, "t", time.Since(start),
					"err", resp.Error.Message)
			}
		}
		if h.traceRequests {
			h.logger.Info("Served", "t", time.Since(start), "method", msg.Method, "reqid", idForLog{msg.ID}, "params", string(msg.Params))
		} else {
			h.logger.Trace("Served", "t", time.Since(start), "method", msg.Method, "reqid", idForLog{msg.ID}, "params", string(msg.Params))
		}
		return resp
	case msg.hasValidID():
		return msg.errorResponse(&invalidRequestError{"invalid request"})
	default:
		return errorMessage(&invalidRequestError{"invalid request"})
	}
}

func (h *handler) isMethodAllowedByGranularControl(method string) bool {
	_, isForbidden := h.forbiddenList[method]
	if len(h.allowList) == 0 {
		return !isForbidden
	}

	_, ok := h.allowList[method]
	return ok
}

// handleCall processes method calls.
func (h *handler) handleCall(cp *callProc, msg *jsonrpcMessage, stream *jsoniter.Stream) *jsonrpcMessage {
	if msg.isSubscribe() {
		return h.handleSubscribe(cp, msg, stream)
	}
	var callb *callback
	if msg.isUnsubscribe() {
		callb = h.unsubscribeCb
	} else if h.isMethodAllowedByGranularControl(msg.Method) {
		callb = h.reg.callback(msg.Method)
	}
	if callb == nil {
		return msg.errorResponse(&methodNotFoundError{method: msg.Method})
	}
	args, err := parsePositionalArguments(msg.Params, callb.argTypes)
	if err != nil {
		return msg.errorResponse(&InvalidParamsError{err.Error()})
	}
	start := time.Now()
	answer := h.runMethod(cp.ctx, msg, callb, args, stream)

	// Collect the statistics for RPC calls if metrics is enabled.
	// We only care about pure rpc call. Filter out subscription.
	if callb != h.unsubscribeCb {
		rpcRequestGauge.Inc()
		if answer != nil && answer.Error != nil {
			failedReqeustGauge.Inc()
		}
		newRPCServingTimerMS(msg.Method, answer == nil || answer.Error == nil).UpdateDuration(start)
	}
	return answer
}

// handleSubscribe processes *_subscribe method calls.
func (h *handler) handleSubscribe(cp *callProc, msg *jsonrpcMessage, stream *jsoniter.Stream) *jsonrpcMessage {
	if !h.allowSubscribe {
		return msg.errorResponse(ErrNotificationsUnsupported)
	}

	// Subscription method name is first argument.
	name, err := parseSubscriptionName(msg.Params)
	if err != nil {
		return msg.errorResponse(&InvalidParamsError{err.Error()})
	}
	namespace := msg.namespace()
	callb := h.reg.subscription(namespace, name)
	if callb == nil {
		return msg.errorResponse(&subscriptionNotFoundError{namespace, name})
	}

	// Parse subscription name arg too, but remove it before calling the callback.
	argTypes := append([]reflect.Type{stringType}, callb.argTypes...)
	args, err := parsePositionalArguments(msg.Params, argTypes)
	if err != nil {
		return msg.errorResponse(&InvalidParamsError{err.Error()})
	}
	args = args[1:]

	// Install notifier in context so the subscription handler can find it.
	n := &Notifier{h: h, namespace: namespace}
	cp.notifiers = append(cp.notifiers, n)
	ctx := context.WithValue(cp.ctx, notifierKey{}, n)

	return h.runMethod(ctx, msg, callb, args, stream)
}

// runMethod runs the Go callback for an RPC method.
func (h *handler) runMethod(ctx context.Context, msg *jsonrpcMessage, callb *callback, args []reflect.Value, stream *jsoniter.Stream) *jsonrpcMessage {
	if !callb.streamable {
		result, err := callb.call(ctx, msg.Method, args, stream)
		if err != nil {
			return msg.errorResponse(err)
		}
		return msg.response(result)
	}

	stream.WriteObjectStart()
	stream.WriteObjectField("jsonrpc")
	stream.WriteString("2.0")
	stream.WriteMore()
	if msg.ID != nil {
		stream.WriteObjectField("id")
		stream.Write(msg.ID)
		stream.WriteMore()
	}
	stream.WriteObjectField("result")
	_, err := callb.call(ctx, msg.Method, args, stream)
	if err != nil {
		writeNilIfNotPresent(stream)
		stream.WriteMore()
		HandleError(err, stream)
	}
	stream.WriteObjectEnd()
	stream.Flush()
	return nil
}

var nullAsBytes = []byte{110, 117, 108, 108}

// there are many avenues that could lead to an error being handled in runMethod, so we need to check
// if nil has already been written to the stream before writing it again here
func writeNilIfNotPresent(stream *jsoniter.Stream) {
	if stream == nil {
		return
	}
	b := stream.Buffer()
	hasNil := true
	if len(b) >= 4 {
		b = b[len(b)-4:]
		for i, v := range nullAsBytes {
			if v != b[i] {
				hasNil = false
				break
			}
		}
	} else {
		hasNil = false
	}
	if !hasNil {
		stream.WriteNil()
	}
}

// unsubscribe is the callback function for all *_unsubscribe calls.
func (h *handler) unsubscribe(ctx context.Context, id ID) (bool, error) {
	h.subLock.Lock()
	defer h.subLock.Unlock()

	s := h.serverSubs[id]
	if s == nil {
		return false, ErrSubscriptionNotFound
	}
	close(s.err)
	delete(h.serverSubs, id)
	return true, nil
}

type idForLog struct{ json.RawMessage }

func (id idForLog) String() string {
	if s, err := strconv.Unquote(string(id.RawMessage)); err == nil {
		return s
	}
	return string(id.RawMessage)
}
