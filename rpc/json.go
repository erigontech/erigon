// Copyright 2015 The go-ethereum Authors
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
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/erigontech/erigon/rpc/jsonstream"
)

const (
	vsn                      = "2.0"
	serviceMethodSeparator   = "_"
	subscribeMethodSuffix    = "_subscribe"
	unsubscribeMethodSuffix  = "_unsubscribe"
	notificationMethodSuffix = "_subscription"

	defaultWriteTimeout = 10 * time.Minute
)

var null = json.RawMessage("null")

type subscriptionResult struct {
	ID     string          `json:"subscription"`
	Result json.RawMessage `json:"result,omitempty"`
}

// A value of this type can a JSON-RPC request, notification, successful response or
// error response. Which one it is depends on the fields.
type jsonrpcMessage struct {
	Version string          `json:"jsonrpc,omitempty"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Error   *jsonError      `json:"error,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
}

func (msg *jsonrpcMessage) isNotification() bool {
	return msg.ID == nil && msg.Method != ""
}

func (msg *jsonrpcMessage) isCall() bool {
	return msg.hasValidID() && msg.Method != ""
}

func (msg *jsonrpcMessage) isResponse() bool {
	return msg.hasValidID() && msg.Method == "" && msg.Params == nil && (msg.Result != nil || msg.Error != nil)
}

func (msg *jsonrpcMessage) hasValidID() bool {
	return len(msg.ID) > 0 && msg.ID[0] != '{' && msg.ID[0] != '['
}

func (msg *jsonrpcMessage) isSubscribe() bool {
	return strings.HasSuffix(msg.Method, subscribeMethodSuffix)
}

func (msg *jsonrpcMessage) isUnsubscribe() bool {
	return strings.HasSuffix(msg.Method, unsubscribeMethodSuffix)
}

func (msg *jsonrpcMessage) namespace() string {
	ns, _, _ := strings.Cut(msg.Method, serviceMethodSeparator)
	return ns
}

func (msg *jsonrpcMessage) String() string {
	b, err := json.Marshal(msg)
	if err != nil {
		return ""
	}
	return string(b)
}

func (msg *jsonrpcMessage) errorResponse(err error) *jsonrpcMessage {
	resp := errorMessage(err)
	resp.ID = msg.ID
	return resp
}

// fastJSONResult lets an RPC result implement fast JSON marshalling where needed — e.g. large payloads that benefit from skipping the reflection-based path.
type fastJSONResult interface {
	MarshalFastJSON() ([]byte, error)
}

// fastJSONMarshalerTo is a fastJSONResult that encodes straight into the response stream. Only a
// type above rpc/jsonstream can name the stream; a type below it implements encoding.TextAppender
// instead and the stream quotes the text. An implementation that fails after its first write
// leaves part of the result behind, so that response carries both result and error.
type fastJSONMarshalerTo interface {
	MarshalFastJSONTo(s *jsonstream.StackStream) error
}

// writeResponse streams result into stream as the response; a result that fails to encode becomes the error.
// The id is copied verbatim, so unlike json.Marshal it keeps '<', '>', '&' and U+2028/2029 unescaped.
func (msg *jsonrpcMessage) writeResponse(stream jsonstream.Stream, result any) error {
	return writeLazyResponse(stream, msg.ID, func(rs *jsonstream.LazyFieldStream) error {
		if isNilPointer(result) {
			return json.NewEncoder(encoderWriter{rs}).Encode(result)
		}
		if fm, ok := result.(fastJSONMarshalerTo); ok {
			if err := fm.MarshalFastJSONTo(rs.Open()); err != nil {
				return err
			}
			return rs.Err() // a latched write error left a placeholder in the stream
		}
		if fm, ok := result.(fastJSONResult); ok {
			enc, err := fm.MarshalFastJSON()
			if err == nil && len(enc) > 0 {
				rs.WriteRawBytes(enc)
			}
			return err
		}
		// A TextAppender's JSON is taken to be its quoted text, so this must stay ahead of the
		// reflection encoder and must not catch a type whose json.Marshaler writes something else.
		if ta, ok := result.(encoding.TextAppender); ok {
			rs.Open().WriteQuotedText(ta)
			return rs.Err()
		}
		return json.NewEncoder(encoderWriter{rs}).Encode(result)
	})
}

// writeLazyResponse writes the response envelope and lets write fill "result", which opens on its first value.
// An error from write becomes "error", after closing whatever part of the result was written, and is returned
// for the caller's metrics and logs.
func writeLazyResponse(stream jsonstream.Stream, id json.RawMessage, write func(*jsonstream.LazyFieldStream) error) error {
	stream.WriteObjectStart()
	stream.WriteObjectField("jsonrpc")
	stream.WriteString(vsn)
	if id != nil {
		stream.WriteObjectField("id")
		stream.WriteRawBytes(id)
	}
	rs := jsonstream.NewLazyFieldStream(stream, "result", false)
	err := write(rs)
	if err != nil {
		// A marshaller that failed before writing leaves an empty field: unwrite it, or the
		// response would carry result and error both.
		if rs.Written() && !rs.RewindIfEmpty() {
			rs.CloseIfOpen()
		}
		HandleError(err, stream)
	} else if !rs.Written() {
		// A response carries exactly one of result and error, so a callback that
		// succeeded without writing still owes a result.
		rs.WriteNil()
	}
	stream.WriteObjectEnd()
	return err
}

// encoderWriter hands the stream json.Encoder output without its trailing newline. Encode writes once, after
// the whole value has encoded.
type encoderWriter struct{ stream jsonstream.Stream }

func (w encoderWriter) Write(b []byte) (int, error) {
	w.stream.WriteRawBytes(bytes.TrimSuffix(b, []byte{'\n'}))
	return len(b), nil
}

// isNilPointer catches a typed nil whose value-receiver method would panic, where json writes null.
func isNilPointer(v any) bool {
	rv := reflect.ValueOf(v)
	return rv.Kind() == reflect.Pointer && rv.IsNil()
}

func errorMessage(err error) *jsonrpcMessage {
	return &jsonrpcMessage{Version: vsn, ID: null, Error: newJsonError(err)}
}

type jsonError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func (err *jsonError) Error() string {
	if err.Message == "" {
		return fmt.Sprintf("json-rpc error %d", err.Code)
	}
	return err.Message
}

func (err *jsonError) ErrorCode() int {
	return err.Code
}

func (err *jsonError) ErrorData() any {
	return err.Data
}

func NewJsonError(code int, message string, data any) any {
	return &jsonError{Code: code, Message: message, Data: data}
}

func NewJsonErrorFromErr(err error) any {
	return newJsonError(err)
}

func newJsonError(err error) *jsonError {
	jsonErr := &jsonError{Code: ErrCodeDefault, Message: err.Error()}
	var ec Error
	ok := errors.As(err, &ec)
	if ok {
		jsonErr.Code = ec.ErrorCode()
	}
	var de DataError
	ok = errors.As(err, &de)
	if ok {
		jsonErr.Data = de.ErrorData()
	}
	return jsonErr
}

// Conn is a subset of the methods of net.Conn which are sufficient for ServerCodec.
type Conn interface {
	io.ReadWriteCloser
	SetWriteDeadline(time.Time) error
}

type deadlineCloser interface {
	io.Closer
	SetWriteDeadline(time.Time) error
}

// ConnRemoteAddr wraps the RemoteAddr operation, which returns a description
// of the peer address of a connection. If a Conn also implements ConnRemoteAddr, this
// description is used in log messages.
type ConnRemoteAddr interface {
	RemoteAddr() string
}

// jsonCodec reads and writes JSON-RPC messages to the underlying connection. It also has
// support for parsing arguments and serializing (result) objects.
type jsonCodec struct {
	remote  string
	closer  sync.Once         // close closed channel once
	closeCh chan any          // closed on Close
	decode  func(v any) error // decoder to allow multiple transports
	// readFrame is set only by transports that delimit messages themselves. Each
	// call must return bytes it does not reuse: parsed messages point into them
	// and are handled asynchronously, so they outlive the call that read them.
	readFrame    func() ([]byte, error)
	encMu        sync.Mutex        // guards the encoder
	encode       func(v any) error // encoder to allow multiple transports
	conn         deadlineCloser
	writeTimeout time.Duration // used if the context has no deadline, counted once the write holds the connection
	held         *heldConn     // the socket, when the transport can hold writes back; nil otherwise
}

// newFuncCodec creates a codec that uses the given functions to read and write. If conn
// implements ConnRemoteAddr, log messages include the remote address. decode must reject
// invalid JSON, reading a message relies on it. A transport with a frame reader never calls
// decode, so it may be nil.
func newFuncCodec(conn deadlineCloser, encode, decode func(v any) error, readFrame func() ([]byte, error)) *jsonCodec {
	codec := &jsonCodec{
		closeCh:      make(chan any),
		encode:       encode,
		decode:       decode,
		readFrame:    readFrame,
		conn:         conn,
		writeTimeout: defaultWriteTimeout,
	}
	if ra, ok := conn.(ConnRemoteAddr); ok {
		codec.remote = ra.RemoteAddr()
	}
	return codec
}

// rawResponse is a pre-assembled, already-valid JSON response the codec writes verbatim,
// skipping json.Encoder's redundant appendCompact re-scan; a distinct type keeps that path opt-in.
type rawResponse []byte

// rawBatch is a batch response kept as its already-encoded answers in request order, so a
// transport can stream them instead of first joining them into one buffer.
type rawBatch [][]byte

func (b rawBatch) writeTo(s jsonstream.Stream) {
	s.WriteArrayStart()
	for _, answer := range b {
		s.WriteRawBytes(answer)
	}
	s.WriteArrayEnd()
}

// NewCodec creates a codec on the given connection. If conn implements ConnRemoteAddr, log
// messages will use it to include the remote address of the connection.
func NewCodec(conn Conn) ServerCodec {
	dec := json.NewDecoder(conn)
	dec.UseNumber()
	c := newFuncCodec(conn, newJSONEncoder(conn), dec.Decode, nil)
	c.held, _ = conn.(*heldConn)
	return c
}

// newJSONEncoder returns the writer side every JSON transport shares.
func newJSONEncoder(conn Conn) func(v any) error {
	enc := json.NewEncoder(conn)
	return func(v any) error {
		switch r := v.(type) {
		case rawResponse:
			_, err := conn.Write(append(r, '\n'))
			return err
		case rawBatch:
			s := jsonstream.Get(conn)
			defer jsonstream.Put(s)
			r.writeTo(s)
			s.WriteRaw("\n")
			return s.Flush()
		default:
			return enc.Encode(v)
		}
	}
}

func (c *jsonCodec) remoteAddr() string {
	return c.remote
}

func (c *jsonCodec) peerInfo() PeerInfo {
	// This returns "ipc" because all other built-in transports have a separate codec type.
	return PeerInfo{Transport: "ipc", RemoteAddr: c.remote}
}

func (c *jsonCodec) ReadBatch() (messages []*jsonrpcMessage, batch bool, err error) {
	rawmsg, err := c.readMessage()
	if err != nil {
		return nil, false, err
	}
	messages, batch, err = parseMessage(rawmsg)
	if err != nil {
		return nil, false, err
	}
	for i, msg := range messages {
		if msg == nil {
			// Message is JSON 'null'. Replace with zero value so it
			// will be treated like any other invalid message.
			messages[i] = new(jsonrpcMessage)
		}
	}
	return messages, batch, nil
}

// readMessage returns the bytes of the next message, checked to be valid JSON.
func (c *jsonCodec) readMessage() (json.RawMessage, error) {
	// A stream has no framing, so the decoder finds the message end and checks it.
	if c.readFrame == nil {
		var rawmsg json.RawMessage
		if err := c.decode(&rawmsg); err != nil {
			return nil, err
		}
		return rawmsg, nil
	}
	// The transport delimits the message, so one read and one check will do.
	// Decoding into a json.RawMessage would scan twice and copy.
	frame, err := c.readFrame()
	if err != nil {
		return nil, err
	}
	if !json.Valid(frame) {
		// Decode the broken message to report where it went wrong. Unmarshal
		// checks syntax the same way Valid does, so it fails here too. The
		// fallback only guards against the two ever disagreeing.
		var rawmsg json.RawMessage
		if err := json.Unmarshal(frame, &rawmsg); err != nil {
			return nil, err
		}
		return nil, errors.New("invalid JSON request")
	}
	return frame, nil
}

func (c *jsonCodec) WriteJSON(ctx context.Context, v any) error {
	c.encMu.Lock()
	defer c.encMu.Unlock()

	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(c.writeTimeout)
	}
	if err := c.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return c.encode(v)
}

// coalesce runs send with the socket held, so the messages it writes leave in one socket write.
func (c *jsonCodec) coalesce(send func()) (err error) {
	if c.held == nil {
		send()
		return nil
	}
	c.held.hold()
	defer func() {
		// Writes set the socket deadline under encMu, so the release holds it too: a concurrent write
		// would re-arm the deadline mid-flush.
		c.encMu.Lock()
		defer c.encMu.Unlock()
		if err = c.held.release(time.Now().Add(c.writeTimeout)); err != nil {
			_ = c.held.Conn.Close()
		}
	}()
	send()
	return nil
}

func (c *jsonCodec) Close() {
	c.closer.Do(func() {
		close(c.closeCh)
		c.conn.Close()
	})
}

// Closed returns a channel which will be closed when Close is called
func (c *jsonCodec) closed() <-chan any {
	return c.closeCh
}

// parseMessage parses raw bytes as a (batch of) JSON-RPC message(s). There are no error
// checks in this function because the raw message has already been syntax-checked when it
// is called. Any non-JSON-RPC messages in the input return the zero value of
// jsonrpcMessage.
func parseMessage(raw json.RawMessage) ([]*jsonrpcMessage, bool, error) {
	if !isBatch(raw) {
		// ReadBatch turns a nil message into a zero one, which is how null is rejected.
		if isJSONNull(raw) {
			return []*jsonrpcMessage{nil}, false, nil
		}
		msg := new(jsonrpcMessage)
		fillMessage(raw, msg)
		return []*jsonrpcMessage{msg}, false, nil
	}
	var msgs []*jsonrpcMessage
	forEachJSONElement(raw, func(elem []byte) bool {
		if isJSONNull(elem) {
			msgs = append(msgs, nil)
			return true
		}
		msg := new(jsonrpcMessage)
		fillMessage(elem, msg)
		msgs = append(msgs, msg)
		return true
	})
	return msgs, true, nil
}

// fillMessage picks a message apart into msg. Input that does not hold an object
// leaves msg zero, and the handler rejects it later.
func fillMessage(input []byte, msg *jsonrpcMessage) {
	// The raw fields point into input rather than being copied out of it, which
	// matters because params is nearly all of a large request.
	forEachJSONField(input, func(key, value []byte) {
		if bytes.IndexByte(key, '\\') >= 0 {
			// encoding/json unescapes object keys, so an escaped spelling of a
			// known field has to match too.
			var name string
			if err := json.Unmarshal([]byte(`"`+string(key)+`"`), &name); err != nil {
				return
			}
			key = []byte(name)
		}
		switch string(key) {
		case "jsonrpc":
			// The decoded fields go through encoding/json, which unescapes the
			// strings. A value that does not decode zeroes the field, so a
			// repeated key cannot leave an earlier value standing.
			if json.Unmarshal(value, &msg.Version) != nil {
				msg.Version = ""
			}
		case "id":
			msg.ID = value
		case "method":
			if json.Unmarshal(value, &msg.Method) != nil {
				msg.Method = ""
			}
		case "params":
			msg.Params = value
		case "error":
			if json.Unmarshal(value, &msg.Error) != nil {
				msg.Error = nil
			}
		case "result":
			msg.Result = value
		}
	})
}

// isBatch returns true when the first non-whitespace characters is '['
func isBatch(raw json.RawMessage) bool {
	for _, c := range raw {
		// skip insignificant whitespace (http://www.ietf.org/rfc/rfc4627.txt)
		if c == 0x20 || c == 0x09 || c == 0x0a || c == 0x0d {
			continue
		}
		return c == '['
	}
	return false
}

// parsePositionalArguments tries to parse the given args to an array of values with the
// given types. It returns the parsed values or an error when the args could not be
// parsed. Missing optional arguments are returned as reflect.Zero values.
func parsePositionalArguments(rawArgs json.RawMessage, types []reflect.Type) ([]reflect.Value, error) {
	var args []reflect.Value
	switch {
	case len(bytes.TrimSpace(rawArgs)) == 0 || isJSONNull(rawArgs):
		// "params" is optional and may be empty. Also allow "params":null even though it's
		// not in the spec because our own client used to send it.
	case isBatch(rawArgs):
		// Read argument array.
		var err error
		if args, err = parseArgumentArray(rawArgs, types); err != nil {
			return nil, err
		}
	default:
		return nil, errors.New("non-array args")
	}
	// Set any missing args to nil.
	for i := len(args); i < len(types); i++ {
		if types[i].Kind() != reflect.Pointer {
			return nil, fmt.Errorf("missing value for required argument %d", i)
		}
		args = append(args, reflect.Zero(types[i]))
	}
	return args, nil
}

// parseArgumentArray decodes an already syntax-checked argument array.
func parseArgumentArray(rawArgs json.RawMessage, types []reflect.Type) ([]reflect.Value, error) {
	// Cutting the array into elements first means each argument is decoded once.
	// A json.Decoder would walk every argument twice, once to find where it ends.
	args := make([]reflect.Value, 0, len(types))
	var scanErr error
	forEachJSONElement(rawArgs, func(elem []byte) bool {
		i := len(args)
		if i >= len(types) {
			scanErr = fmt.Errorf("too many arguments, want at most %d", len(types))
			return false
		}
		if types[i].Kind() != reflect.Pointer && isJSONNull(elem) {
			scanErr = fmt.Errorf("missing value for required argument %d", i)
			return false
		}
		argval := reflect.New(types[i])
		if err := decodeArgument(elem, argval.Interface()); err != nil {
			scanErr = fmt.Errorf("invalid argument %d: %w", i, err)
			return false
		}
		args = append(args, argval.Elem())
		return true
	})
	return args, scanErr
}

// decodeArgument decodes one already syntax-checked argument value.
func decodeArgument(elem []byte, arg any) error {
	// A type that unmarshals itself is called directly, which skips the
	// validation pass json.Unmarshal runs first.
	if u, ok := arg.(json.Unmarshaler); ok && !isJSONNull(elem) {
		return u.UnmarshalJSON(elem)
	}
	return json.Unmarshal(elem, arg)
}

func isJSONNull(raw json.RawMessage) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte("null"))
}

// parseSubscriptionName extracts the subscription name from an encoded argument array.
func parseSubscriptionName(rawArgs json.RawMessage) (string, error) {
	dec := json.NewDecoder(bytes.NewReader(rawArgs))
	if tok, _ := dec.Token(); tok != json.Delim('[') {
		return "", errors.New("non-array args")
	}
	v, _ := dec.Token()
	method, ok := v.(string)
	if !ok {
		return "", errors.New("expected subscription name as first argument")
	}
	return method, nil
}
