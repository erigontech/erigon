// Copyright 2016 The go-ethereum Authors
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
	"fmt"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/race"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

func TestNewID(t *testing.T) {
	hexchars := "0123456789ABCDEFabcdef"
	for range 100 {
		id := string(NewID())
		if !strings.HasPrefix(id, "0x") {
			t.Fatalf("invalid ID prefix, want '0x...', got %s", id)
		}

		id = id[2:]
		if len(id) == 0 || len(id) > 32 {
			t.Fatalf("invalid ID length, want len(id) > 0 && len(id) <= 32), got %d", len(id))
		}

		for i := 0; i < len(id); i++ {
			if strings.IndexByte(hexchars, id[i]) == -1 {
				t.Fatalf("unexpected byte, want any valid hex char, got %c", id[i])
			}
		}
	}
}

func TestSubscriptions(t *testing.T) {
	logger := log.New()
	var (
		namespaces        = []string{"eth", "bzz"}
		service           = &notificationTestService{}
		subCount          = len(namespaces)
		notificationCount = 3

		server                 = NewServer(50, false /* traceRequests */, false /* debugSingleRequests */, true, logger, 100)
		clientConn, serverConn = net.Pipe()
		out                    = json.NewEncoder(clientConn)
		in                     = json.NewDecoder(clientConn)
		successes              = make(chan subConfirmation)
		notifications          = make(chan subscriptionResult)
		errors                 = make(chan error, subCount*notificationCount+1)
	)

	// setup and start server
	for _, namespace := range namespaces {
		if err := server.RegisterName(namespace, service); err != nil {
			t.Fatalf("unable to register test service %v", err)
		}
	}
	go server.ServeCodec(NewCodec(serverConn), 0)
	defer server.Stop()

	// wait for message and write them to the given channels
	go waitForMessages(in, successes, notifications, errors)

	// create subscriptions one by one
	for i, namespace := range namespaces {
		request := map[string]any{
			"id":      i,
			"method":  fmt.Sprintf("%s_subscribe", namespace),
			"version": "2.0",
			"params":  []any{"someSubscription", notificationCount, i},
		}
		if err := out.Encode(&request); err != nil {
			t.Fatalf("Could not create subscription: %v", err)
		}
	}

	timeout := time.After(30 * time.Second)
	subids := make(map[string]string, subCount)
	count := make(map[string]int, subCount)
	allReceived := func() bool {
		done := len(count) == subCount
		for _, c := range count {
			if c < notificationCount {
				done = false
			}
		}
		return done
	}
	for !allReceived() {
		select {
		case confirmation := <-successes: // subscription created
			subids[namespaces[confirmation.reqid]] = string(confirmation.subid)
		case notification := <-notifications:
			count[notification.ID]++
		case err := <-errors:
			t.Fatal(err)
		case <-timeout:
			for _, namespace := range namespaces {
				subid, found := subids[namespace]
				if !found {
					t.Errorf("subscription for %q not created", namespace)
					continue
				}
				if count, found := count[subid]; !found || count < notificationCount {
					t.Errorf("didn't receive all notifications (%d<%d) in time for namespace %q", count, notificationCount, namespace)
				}
			}
			t.Fatal("timed out")
		}
	}
}

// Every subscribe call of a batch must take effect, though they all add to the notifiers the
// batch shares.
func TestBatchSubscriptionsAllNotify(t *testing.T) {
	logger := log.New()
	server := NewServer(50, false /* traceRequests */, false /* debugSingleRequests */, true, logger, 100)
	if err := server.RegisterName("nftest", new(notificationTestService)); err != nil {
		t.Fatal(err)
	}
	clientConn, serverConn := net.Pipe()
	go server.ServeCodec(NewCodec(serverConn), 0)
	defer server.Stop()

	const subs = 16
	batch := make([]map[string]any, subs)
	for i := range batch {
		batch[i] = map[string]any{"jsonrpc": "2.0", "id": i, "method": "nftest_subscribe", "params": []any{"someSubscription", 1, i}}
	}
	if err := json.NewEncoder(clientConn).Encode(batch); err != nil {
		t.Fatal(err)
	}
	if err := clientConn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	in := json.NewDecoder(clientConn)
	for notified := 0; notified < subs; {
		var msg json.RawMessage
		if err := in.Decode(&msg); err != nil {
			t.Fatalf("%d of %d subscriptions notified: %v", notified, subs, err)
		}
		if msg[0] != '[' {
			notified++
		}
	}
}

// This test checks that unsubscribing works.
func TestServerUnsubscribe(t *testing.T) {
	logger := log.New()
	p1, p2 := net.Pipe()
	defer p2.Close()

	// Start the server.
	server := newTestServer(logger)
	service := &notificationTestService{unsubscribed: make(chan string, 1)}
	if err := server.RegisterName("nftest2", service); err != nil {
		t.Fatal(err)
	}
	go server.ServeCodec(NewCodec(p1), 0)

	// Subscribe.
	if err := p2.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := p2.Write([]byte(`{"jsonrpc":"2.0","id":1,"method":"nftest2_subscribe","params":["someSubscription",0,10]}`)); err != nil {
		t.Fatal(err)
	}

	// Handle received messages.
	var (
		resps         = make(chan subConfirmation)
		notifications = make(chan subscriptionResult)
		errors        = make(chan error, 1)
	)
	go waitForMessages(json.NewDecoder(p2), resps, notifications, errors)

	// Receive the subscription ID.
	var sub subConfirmation
	select {
	case sub = <-resps:
	case err := <-errors:
		t.Fatal(err)
	}

	// Unsubscribe and check that it is handled on the server side.
	if _, err := p2.Write([]byte(`{"jsonrpc":"2.0","method":"nftest2_unsubscribe","params":["` + sub.subid + `"]}`)); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case id := <-service.unsubscribed:
			if id != string(sub.subid) {
				t.Errorf("wrong subscription ID unsubscribed")
			}
			return
		case err := <-errors:
			t.Fatal(err)
		case <-notifications:
			// drop notifications
		}
	}
}

type subConfirmation struct {
	reqid int
	subid ID
}

// waitForMessages reads RPC messages from 'in' and dispatches them into the given channels.
// It stops if there is an error.
func waitForMessages(in *json.Decoder, successes chan subConfirmation, notifications chan subscriptionResult, errors chan error) {
	for {
		resp, notification, err := readAndValidateMessage(in)
		switch {
		case err != nil:
			errors <- err
			return
		case resp != nil:
			successes <- *resp
		default:
			notifications <- *notification
		}
	}
}

func readAndValidateMessage(in *json.Decoder) (*subConfirmation, *subscriptionResult, error) {
	var msg jsonrpcMessage
	if err := in.Decode(&msg); err != nil {
		return nil, nil, fmt.Errorf("decode error: %w", err)
	}
	switch {
	case msg.isNotification():
		var res subscriptionResult
		if err := json.Unmarshal(msg.Params, &res); err != nil {
			return nil, nil, fmt.Errorf("invalid subscription result: %w", err)
		}
		return nil, &res, nil
	case msg.isResponse():
		var c subConfirmation
		if msg.Error != nil {
			return nil, nil, msg.Error
		} else if err := json.Unmarshal(msg.Result, &c.subid); err != nil {
			return nil, nil, fmt.Errorf("invalid response: %w", err)
		} else if err := json.Unmarshal(msg.ID, &c.reqid); err != nil {
			return nil, nil, fmt.Errorf("invalid request id: %w", err)
		} else {
			return &c, nil, nil
		}
	default:
		return nil, nil, fmt.Errorf("unrecognized message: %v", msg)
	}
}

type streamedPayload struct{}

func (streamedPayload) MarshalFastJSONTo(w *jsonstream.StackStream) error {
	w.WriteHex([]byte{0xab})
	return nil
}

// valueFastJSON has a value receiver, so a typed nil panics unless Notify sends it down the
// reflection path.
type valueFastJSON struct{ data []byte }

func (b valueFastJSON) MarshalFastJSONTo(w *jsonstream.StackStream) error {
	w.WriteHex(b.data)
	return nil
}

func TestNotifyUsesFastJSON(t *testing.T) {
	t.Parallel()

	for payload, want := range map[any]string{streamedPayload{}: `"0xab"`, (*valueFastJSON)(nil): "null", &valueFastJSON{data: []byte{0xab}}: `"0xab"`} {
		n := &RemoteNotifier{sub: &Subscription{ID: "0x1"}}
		if err := n.Notify("0x1", payload); err != nil {
			t.Fatal(err)
		}
		if len(n.buffer) != 1 || string(n.buffer[0]) != want {
			t.Fatalf("%T: want %s, got %#v", payload, want, n.buffer)
		}
	}
}

type wireOnly struct{ v int }

func (w wireOnly) LocalValue() any { return w.v }

// A payload that carries a wire encoding reaches an in-process subscriber as the value it wraps.
func TestLocalNotifierDeliversLocalValue(t *testing.T) {
	resc, closec := make(chan any, 1), make(chan any)
	n := NewLocalNotifier("eth", resc, closec)
	sub := n.CreateSubscription()
	if err := n.Notify(sub.ID, wireOnly{v: 7}); err != nil {
		t.Fatal(err)
	}
	if got := <-resc; got != 7 {
		t.Fatalf("delivered %#v, want 7", got)
	}
}

// The notification is built around bytes that are already encoded, and must come out as the
// message json.Marshal made of them.
func TestNotificationMatchesMarshalledMessage(t *testing.T) {
	result := json.RawMessage(`[{"blockHash":"0x01","logs":[]},{"blockHash":"0x02","logs":[]}]`)
	params, err := json.Marshal(&subscriptionResult{ID: "0x9a", Result: result})
	if err != nil {
		t.Fatal(err)
	}
	for _, namespace := range []string{"eth", `quote"back\slash`} {
		want, err := json.Marshal(&jsonrpcMessage{Version: vsn, Method: namespace + notificationMethodSuffix, Params: params})
		if err != nil {
			t.Fatal(err)
		}
		w := &captureWriter{}
		n := &RemoteNotifier{h: &handler{conn: w}, prefix: notificationPrefix(namespace, "0x9a"), activated: true}
		if err := n.send(result); err != nil {
			t.Fatal(err)
		}
		if got := w.got; !bytes.Equal(got, want) {
			t.Fatalf("notification = %s, want %s", got, want)
		}
	}
}

// An activated notifier streams the whole notification: the prefix, the result and "}}".
// emptyStreamed writes nothing; a notification still carries a result, as a response does.
type emptyStreamed struct{}

func (emptyStreamed) MarshalFastJSONTo(*jsonstream.StackStream) error { return nil }

func TestNotifyStreamsTheNotification(t *testing.T) {
	for payload, result := range map[any]string{streamedPayload{}: `"0xab"`, 7: `7`, (*valueFastJSON)(nil): `null`, emptyStreamed{}: `null`} {
		w := &captureWriter{}
		n := &RemoteNotifier{h: &handler{conn: w}, prefix: notificationPrefix("eth", "0x9a"), sub: &Subscription{ID: "0x9a"}, activated: true}
		if err := n.Notify("0x9a", payload); err != nil {
			t.Fatal(err)
		}
		if want := string(notificationPrefix("eth", "0x9a")) + result + "}}"; string(w.got) != want {
			t.Fatalf("%T: notification = %s, want %s", payload, w.got, want)
		}
	}
}

// A notification buffered before activation carries a result too.
func TestBufferedNotificationOfEmptyMarshallerCarriesNull(t *testing.T) {
	w := &captureWriter{}
	n := &RemoteNotifier{h: &handler{conn: w}, prefix: notificationPrefix("eth", "0x9a"), sub: &Subscription{ID: "0x9a"}}
	if err := n.Notify("0x9a", emptyStreamed{}); err != nil {
		t.Fatal(err)
	}
	if err := n.activate(); err != nil {
		t.Fatal(err)
	}
	if want := `{"jsonrpc":"2.0","method":"eth_subscription","params":{"subscription":"0x9a","result":null}}`; string(w.got) != want {
		t.Fatalf("notification = %s, want %s", w.got, want)
	}
}

// captureWriter keeps a copy of what it is given: a notification's buffer is reused once the
// write returns.
type captureWriter struct{ got []byte }

func (w *captureWriter) WriteJSON(_ context.Context, v any) error {
	w.got = bytes.Clone(v.(rawResponse))
	return nil
}
func (*captureWriter) closed() <-chan any { return nil }
func (*captureWriter) remoteAddr() string { return "" }

type discardWriter struct{}

func (discardWriter) WriteJSON(context.Context, any) error { return nil }
func (discardWriter) closed() <-chan any                   { return nil }
func (discardWriter) remoteAddr() string                   { return "" }

// Every subscriber gets the same encoded result, so wrapping it for one subscriber must not
// allocate its size: at 2000 subscribers a block's receipts would be allocated 2000 times.
func TestNotifySendDoesNotAllocateResult(t *testing.T) {
	result := json.RawMessage(`"` + strings.Repeat("x", 160*1024) + `"`)
	n := &RemoteNotifier{h: &handler{conn: discardWriter{}}, prefix: notificationPrefix("eth", "0x9a"), activated: true}
	send := func() {
		if err := n.send(result); err != nil {
			t.Fatal(err)
		}
	}
	send()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	const runs = 100
	for range runs {
		send()
	}
	runtime.ReadMemStats(&m1)
	perSend := (m1.TotalAlloc - m0.TotalAlloc) / runs
	if !race.Enabled && perSend > uint64(len(result))/10 { // the race detector drops sync.Pool puts at random
		t.Fatalf("a send allocates %d bytes for a %d-byte result", perSend, len(result))
	}
}
