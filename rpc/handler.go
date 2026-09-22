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
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// handler handles JSON-RPC messages. There is one handler per connection. Note that
// handler is not safe for concurrent use. On a connection, message handling never blocks
// indefinitely because RPCs are processed on background goroutines launched by handler;
// with inlineCalls they run on the caller's goroutine, which a single HTTP request owns.
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
	inlineCalls    bool // the caller waits for every answer, as a single HTTP request does
	batchLimit     int

	allowList     AllowList // a list of explicitly allowed methods, if empty -- everything is allowed
	forbiddenList ForbiddenList

	subLock             sync.Mutex
	serverSubs          map[ID]*Subscription
	maxBatchConcurrency uint
	traceRequests       bool

	//slow requests
	slowLogThreshold time.Duration
	slowLogBlacklist []string
}

type callProc struct {
	ctx       context.Context
	notifiers []*RemoteNotifier
}

func HandleError(err error, stream jsonstream.Stream) {
	if err != nil {
		stream.WriteObjectField("error")
		stream.WriteObjectStart()
		stream.WriteObjectField("code")
		if ec, ok := errors.AsType[Error](err); ok {
			stream.WriteInt(ec.ErrorCode())
		} else {
			stream.WriteInt(ErrCodeDefault)
		}
		stream.WriteMore()
		stream.WriteObjectField("message")
		stream.WriteString(err.Error())
		if de, ok := errors.AsType[DataError](err); ok {
			stream.WriteMore()
			stream.WriteObjectField("data")
			data, derr := json.Marshal(de.ErrorData())
			if derr == nil {
				stream.WriteRawBytes(data)
			} else {
				stream.WriteString(derr.Error())
			}
		}
		stream.WriteObjectEnd()
	}
}

func newHandler(
	connCtx context.Context,
	conn jsonWriter,
	idgen func() ID,
	reg *serviceRegistry,
	batchLimit int,
	allowList AllowList,
	maxBatchConcurrency uint,
	traceRequests bool,
	logger log.Logger,
	rpcSlowLogThreshold time.Duration,
) *handler {
	rootCtx, cancelRoot := context.WithCancel(connCtx)
	forbiddenList := ForbiddenList{}

	h := &handler{
		reg:            reg,
		idgen:          idgen,
		conn:           conn,
		respWait:       make(map[string]*requestOp),
		clientSubs:     make(map[string]*ClientSubscription),
		rootCtx:        rootCtx,
		cancelRoot:     cancelRoot,
		allowSubscribe: true,
		batchLimit:     batchLimit,
		serverSubs:     make(map[ID]*Subscription),
		logger:         logger,
		allowList:      allowList,
		forbiddenList:  forbiddenList,

		maxBatchConcurrency: maxBatchConcurrency,
		traceRequests:       traceRequests,

		slowLogThreshold: rpcSlowLogThreshold,
		slowLogBlacklist: rpccfg.SlowLogBlackList,
	}

	if conn.remoteAddr() != "" {
		h.logger = h.logger.New("conn", conn.remoteAddr())
	}
	h.unsubscribeCb = newCallback(reflect.Value{}, reflect.ValueOf(h.unsubscribe), "unsubscribe", h.logger)

	return h
}

func (h *handler) isRpcMethodNeedsCheck(method string) bool {
	return !slices.Contains(h.slowLogBlacklist, method)
}

// inOrderMethods change state that a later call of the same batch may depend on, such as a
// sender's next nonce in the txpool. A batch holding one runs its calls one by one, in order.
var inOrderMethods = map[string]struct{}{
	"eth_sendRawTransaction":     {},
	"eth_sendRawTransactionSync": {},
	"graphql_sendRawTransaction": {},
	"eth_uninstallFilter":        {},
	"eth_getFilterChanges":       {},
	"admin_addPeer":              {},
	"admin_removePeer":           {},
	"admin_addTrustedPeer":       {},
	"admin_removeTrustedPeer":    {},
	"debug_setHead":              {},
	"debug_setGCPercent":         {},
	"debug_setMemoryLimit":       {},
	"eth_submitWork":             {},
	"eth_submitHashrate":         {},
}

// hasInOrderCall also counts subscribe calls: each adds to the batch's notifiers, which two
// goroutines must not append to at once.
func hasInOrderCall(calls []*jsonrpcMessage) bool {
	for _, msg := range calls {
		if _, ok := inOrderMethods[msg.Method]; ok || msg.isSubscribe() || strings.HasPrefix(msg.Method, "engine_") {
			return true
		}
	}
	return false
}

// answerBatchCall runs one call of a batch and returns its answer, or nil when it needs none.
func (h *handler) answerBatchCall(cp *callProc, msg *jsonrpcMessage) []byte {
	select {
	case <-cp.ctx.Done():
		return nil
	default:
	}

	// A non-nil res is an error answer that still has to be written. On nil the answer
	// is already in the stream, or the message needs none.
	buf := bytes.NewBuffer(nil)
	stream := jsonstream.Get(buf)
	defer jsonstream.Put(stream)
	if res := h.handleCallMsg(cp, msg, stream); res != nil {
		res.writeTo(stream)
	}
	_ = stream.Flush()
	if buf.Len() == 0 {
		return nil
	}
	return buf.Bytes()
}

// handleBatch executes all messages in a batch and returns the responses.
func (h *handler) handleBatch(msgs []*jsonrpcMessage) {
	// Emit error response for empty batches:
	if len(msgs) == 0 {
		h.startCallProc(func(cp *callProc) {
			if err := h.conn.WriteJSON(cp.ctx, errorMessage(&invalidRequestError{"empty batch"})); err != nil {
				h.logger.Debug("Failed to write RPC error response", "err", err)
			}
		})
		return
	}
	// Apply limit on total number of requests.
	if h.batchLimit != 0 && len(msgs) > h.batchLimit {
		h.startCallProc(func(cp *callProc) {
			h.respondWithBatchTooLarge(cp, msgs)
		})
		return
	}

	// Handle non-call messages first:
	calls := make([]*jsonrpcMessage, 0, len(msgs))
	h.handleResponses(msgs, func(msg *jsonrpcMessage) {
		calls = append(calls, msg)
	})
	if len(calls) == 0 {
		return
	}

	// Calls may block indefinitely, so they go to a goroutine unless the caller waits anyway:
	h.startCallProc(func(cp *callProc) {
		// Answers go to their request's slot, because the reply order must match the request order.
		answersWithNils := make([][]byte, len(calls))
		if hasInOrderCall(calls) {
			for i, msg := range calls {
				answersWithNils[i] = h.answerBatchCall(cp, msg)
			}
		} else {
			// Bounded parallelism pattern explanation https://blog.golang.org/pipelines#TOC_9.
			boundedConcurrency := make(chan struct{}, h.maxBatchConcurrency)
			defer close(boundedConcurrency)
			wg := sync.WaitGroup{}
			for i := range calls {
				boundedConcurrency <- struct{}{}
				wg.Go(func() {
					defer func() {
						<-boundedConcurrency
					}()
					answersWithNils[i] = h.answerBatchCall(cp, calls[i])
				})
			}
			wg.Wait()
		}
		h.addSubscriptions(cp.notifiers)
		h.sendBatchAnswers(cp.ctx, answersWithNils)
		for _, n := range cp.notifiers {
			if err := n.activate(); err != nil {
				h.logger.Debug("Failed to activate RPC notifier", "err", err)
			}
		}
	})
}

// sendBatchAnswers sends the answers in request order, leaving out calls that have none.
func (h *handler) sendBatchAnswers(ctx context.Context, answers [][]byte) {
	batch := slices.DeleteFunc(answers, func(answer []byte) bool { return answer == nil })
	if len(batch) == 0 {
		return
	}
	if err := h.conn.WriteJSON(ctx, rawBatch(batch)); err != nil {
		h.logger.Debug("Failed to write RPC batch response", "err", err)
	}
}

// answerBuffered serves a call for a transport that has no stream to write
// through: the whole response is built in a pooled stream and sent in one piece.
// It owns the stream, so the pool gets it back on any exit.
func (h *handler) answerBuffered(cp *callProc, msg *jsonrpcMessage) {
	stream := jsonstream.Get(nil)
	defer jsonstream.Put(stream)

	h.answerInto(cp, msg, stream)
	if err := h.conn.WriteJSON(cp.ctx, rawResponse(stream.Buffer())); err != nil {
		h.logger.Debug("Failed to write RPC response", "err", err)
	}
}

// answerInto runs the call and leaves its response in stream. The call writes a success
// itself; only an error answer is encoded here.
func (h *handler) answerInto(cp *callProc, msg *jsonrpcMessage, stream jsonstream.Stream) {
	answer := h.handleCallMsg(cp, msg, stream)
	h.addSubscriptions(cp.notifiers)
	if answer != nil {
		answer.writeTo(stream)
	}
}

func (h *handler) respondWithBatchTooLarge(cp *callProc, batch []*jsonrpcMessage) {
	reason := fmt.Sprintf("batch limit %d exceeded (can increase by --rpc.batch.limit). Requested batch of size: %d", h.batchLimit, len(batch))
	resp := errorMessage(&invalidRequestError{reason})
	// Find the first call and add its "id" field to the error.
	// This is the best we can do, given that the protocol doesn't have a way
	// of reporting an error for the entire batch.
	for _, msg := range batch {
		if msg.isCall() {
			resp.ID = msg.ID
			break
		}
	}
	if err := h.conn.WriteJSON(cp.ctx, []*jsonrpcMessage{resp}); err != nil {
		h.logger.Debug("Failed to write RPC batch-too-large response", "err", err)
	}
}

// handleMsg handles a single message.
func (h *handler) handleMsg(msg *jsonrpcMessage, stream jsonstream.Stream) {
	if ok := h.handleImmediate(msg); ok {
		return
	}
	h.startCallProc(func(cp *callProc) {
		if stream == nil {
			h.answerBuffered(cp, msg)
		} else {
			h.answerInto(cp, msg, stream)
			stream.WriteRaw("\n")
		}
		for _, n := range cp.notifiers {
			if err := n.activate(); err != nil {
				h.logger.Debug("Failed to activate RPC notifier", "err", err)
			}
		}
	})
}

// handleResponses processes method call responses.
func (h *handler) handleResponses(batch []*jsonrpcMessage, handleCall func(*jsonrpcMessage)) {
	var resolvedOps []*requestOp
	handleResp := func(msg *jsonrpcMessage) {
		op := h.respWait[string(msg.ID)]
		if op == nil {
			h.logger.Debug("Unsolicited RPC response", "reqid", idForLog(msg.ID))
			return
		}
		resolvedOps = append(resolvedOps, op)
		delete(h.respWait, string(msg.ID))

		// For subscription responses, start the subscription if the server
		// indicates success. EthSubscribe gets unblocked in either case through
		// the op.resp channel.
		if op.sub != nil {
			if msg.Error != nil {
				op.err = msg.Error
			} else {
				op.err = json.Unmarshal(msg.Result, &op.sub.subid)
				if op.err == nil {
					go op.sub.start()
					h.clientSubs[op.sub.subid] = op.sub
				}
			}
		}

		if !op.hadResponse {
			op.hadResponse = true
			op.resp <- batch
		}
	}

	for _, msg := range batch {
		start := time.Now()
		switch {
		case msg.isResponse():
			handleResp(msg)
			h.logger.Trace("Handled RPC response", "reqid", idForLog(msg.ID), "duration", time.Since(start))

		case msg.isNotification():
			if strings.HasSuffix(msg.Method, notificationMethodSuffix) {
				h.handleSubscriptionResult(msg)
				continue
			}
			handleCall(msg)

		default:
			handleCall(msg)
		}
	}

	for _, op := range resolvedOps {
		h.removeRequestOp(op)
	}
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

func (h *handler) addSubscriptions(nn []*RemoteNotifier) {
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

// startCallProc runs fn in a new goroutine tracked by h.callWG, or on the caller's goroutine when inlineCalls is set.
func (h *handler) startCallProc(fn func(*callProc)) {
	run := func() {
		ctx, cancel := context.WithCancel(h.rootCtx)
		defer cancel()
		fn(&callProc{ctx: ctx})
	}
	if h.inlineCalls {
		run()
		return
	}
	h.callWG.Go(run)
}

// handleImmediate executes non-call messages. It returns false if the message is a
// call or requires a reply.
func (h *handler) handleImmediate(msg *jsonrpcMessage) bool {
	switch {
	case msg.isNotification():
		if strings.HasSuffix(msg.Method, notificationMethodSuffix) {
			h.handleSubscriptionResult(msg)
			return true
		}
		return false
	case msg.isResponse():
		h.handleResponse(msg)
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
		h.logger.Trace("[rpc] unsolicited response", "reqid", idForLog(msg.ID))
		return
	}
	delete(h.respWait, string(msg.ID))
	// For normal responses, just forward the reply to Call/BatchCall.
	if op.sub == nil {
		op.resp <- []*jsonrpcMessage{msg}
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

// handleCallMsg executes a call message. It returns the error answer, or nil once the
// response is in the stream or the message needs none.
func (h *handler) handleCallMsg(ctx *callProc, msg *jsonrpcMessage, stream jsonstream.Stream) *jsonrpcMessage {
	switch {
	case msg.isNotification():
		_, _ = h.handleCall(ctx, msg, stream)
		if h.traceRequests {
			h.logger.Info("[rpc] served", "method", msg.Method, "params", string(msg.Params))
		}
		return nil
	case msg.isCall():
		var doSlowLog bool
		if h.slowLogThreshold > 0 {
			doSlowLog = h.isRpcMethodNeedsCheck(msg.Method)
			if doSlowLog {
				slowTimer := time.AfterFunc(h.slowLogThreshold, func() {
					h.logger.Info("[rpc.slow] running", "method", msg.Method, "reqid", idForLog(msg.ID), "params", string(msg.Params))
				})
				defer slowTimer.Stop()
			}
		}

		var start time.Time
		if doSlowLog {
			start = time.Now()
		}

		resp, answered := h.handleCall(ctx, msg, stream)

		if doSlowLog {
			requestDuration := time.Since(start)
			if requestDuration > h.slowLogThreshold {
				h.logger.Info("[rpc.slow] finished", "method", msg.Method, "reqid", idForLog(msg.ID), "duration", requestDuration)
			}
		}

		if !errors.Is(ctx.ctx.Err(), context.Canceled) {
			switch {
			case answered != nil:
				h.logger.Warn("[rpc] served", "method", msg.Method, "reqid", idForLog(msg.ID), "err", answered)
			case resp != nil && resp.Error != nil && resp.Error.Data != nil:
				h.logger.Warn("[rpc] served", "method", msg.Method, "reqid", idForLog(msg.ID),
					"err", resp.Error.Message, "errdata", resp.Error.Data)
			case resp != nil && resp.Error != nil:
				h.logger.Warn("[rpc] served", "method", msg.Method, "reqid", idForLog(msg.ID),
					"err", resp.Error.Message)
			}
		}
		if h.traceRequests {
			h.logger.Info("Served", "method", msg.Method, "reqid", idForLog(msg.ID), "params", string(msg.Params))
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
func (h *handler) handleCall(cp *callProc, msg *jsonrpcMessage, stream jsonstream.Stream) (*jsonrpcMessage, error) {
	allowed := h.isMethodAllowedByGranularControl(msg.Method)
	if msg.isSubscribe() && allowed {
		return h.handleSubscribe(cp, msg, stream)
	}
	var callb *callback
	if msg.isUnsubscribe() {
		callb = h.unsubscribeCb
	} else if allowed {
		callb = h.reg.callback(msg.Method)
	}
	if callb == nil {
		return msg.errorResponse(&methodNotFoundError{method: msg.Method}), nil
	}
	args, err := parsePositionalArguments(msg.Params, callb.argTypes)
	if err != nil {
		return msg.errorResponse(&InvalidParamsError{err.Error()}), nil
	}
	start := time.Now()
	answer, answered := h.runMethod(cp.ctx, msg, callb, args, stream)

	// Collect the statistics for RPC calls if metrics is enabled.
	// We only care about pure rpc call. Filter out subscription.
	if callb != h.unsubscribeCb {
		rpcRequestGauge.Inc()
		if answered != nil || (answer != nil && answer.Error != nil) {
			failedReqeustGauge.Inc()
			callb.timerFailure.ObserveDuration(start)
		} else {
			callb.timerSuccess.ObserveDuration(start)
		}
	}
	return answer, answered
}

// handleSubscribe processes *_subscribe method calls.
func (h *handler) handleSubscribe(cp *callProc, msg *jsonrpcMessage, stream jsonstream.Stream) (*jsonrpcMessage, error) {
	if !h.allowSubscribe {
		return msg.errorResponse(ErrNotificationsUnsupported), nil
	}

	// Subscription method name is first argument.
	name, err := parseSubscriptionName(msg.Params)
	if err != nil {
		return msg.errorResponse(&InvalidParamsError{err.Error()}), nil
	}
	namespace := msg.namespace()
	callb := h.reg.subscription(namespace, name)
	if callb == nil {
		return msg.errorResponse(&subscriptionNotFoundError{namespace, name}), nil
	}

	// Parse subscription name arg too, but remove it before calling the callback.
	argTypes := append([]reflect.Type{stringType}, callb.argTypes...)
	args, err := parsePositionalArguments(msg.Params, argTypes)
	if err != nil {
		return msg.errorResponse(&InvalidParamsError{err.Error()}), nil
	}
	args = args[1:]

	// Install notifier in context so the subscription handler can find it.
	n := &RemoteNotifier{h: h, namespace: namespace}
	cp.notifiers = append(cp.notifiers, n)
	ctx := ContextWithNotifier(cp.ctx, n)

	return h.runMethod(ctx, msg, callb, args, stream)
}

// remapDBOverload converts kv.ErrReadTxLimitExceeded into a JSON-RPC -32005 error and sets
// the HTTP 503 flag in ctx so ServeHTTP can write the correct status before flushing.
func remapDBOverload(ctx context.Context, err error) error {
	if errors.Is(err, kv.ErrReadTxLimitExceeded) {
		SetOverloadedFlag(ctx)
		return &CustomError{Code: ErrCodeServerOverloaded, Message: ErrMsgServerOverloaded}
	}
	return err
}

// runMethod runs the Go callback for an RPC method. It returns either a response for the caller to
// write, or the error it already answered with in the stream.
func (h *handler) runMethod(ctx context.Context, msg *jsonrpcMessage, callb *callback, args []reflect.Value, stream jsonstream.Stream) (*jsonrpcMessage, error) {
	if !callb.streamable {
		result, err := callb.call(ctx, msg.Method, args, stream)
		if err != nil {
			return msg.errorResponse(remapDBOverload(ctx, err)), nil
		}
		if msg.isNotification() {
			return nil, nil
		}
		return nil, msg.writeResponse(stream, result)
	}

	return nil, writeLazyResponse(stream, msg.ID, func(rs *jsonstream.LazyFieldStream) error {
		if _, err := callb.call(ctx, msg.Method, args, rs); err != nil {
			return remapDBOverload(ctx, err)
		}
		return nil
	})
}

// writeTo writes a response built as a message, such as an error; success results go through
// writeResponse. Nothing here may reach the underlying writer: the response must stay in the
// stream buffer until the caller flushes, or the HTTP status is committed before ServeHTTP can set it.
func (msg *jsonrpcMessage) writeTo(stream jsonstream.Stream) {
	buf, err := json.Marshal(msg)
	if err != nil {
		buf, err = json.Marshal(msg.errorResponse(err))
	}
	if err == nil {
		stream.WriteRawBytes(buf)
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

type idForLog json.RawMessage

func (id idForLog) String() string {
	if s, err := strconv.Unquote(string(id)); err == nil {
		return s
	}
	return string(id)
}
