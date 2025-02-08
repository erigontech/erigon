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
	"container/list"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/rand"
	"reflect"
	"sync"
	"time"
)

var (
	// ErrNotificationsUnsupported is returned when the connection doesn't support notifications
	ErrNotificationsUnsupported = errors.New("notifications not supported")
	// ErrNotificationNotFound is returned when the notification for the given id is not found
	ErrSubscriptionNotFound = errors.New("subscription not found")
)

var globalGen = randomIDGenerator()

// ID defines a pseudo random number that is used to identify RPC subscriptions.
type ID string

// NewID returns a new, random ID.
func NewID() ID {
	return globalGen()
}

// randomIDGenerator returns a function that generates cryptographically secure random IDs.
func randomIDGenerator() func() ID {
    return func() ID {
        id := make([]byte, 16)
        if _, err := crand.Read(id); err != nil {
            // Fallback to less secure but still functional method
            binary.BigEndian.PutUint64(id[:8], uint64(time.Now().UnixNano()))
            binary.BigEndian.PutUint64(id[8:], rand.Uint64())
        }
        return encodeID(id)
    }
}

// encodeID converts a byte slice to a subscription ID.
// The input must be exactly 16 bytes long.
func encodeID(b []byte) ID {
    if len(b) != 16 {
        panic("invalid ID length, expected 16 bytes")
    }
    
    // More efficient string building
    hexStr := hex.EncodeToString(b)
    // Find first non-zero character
    start := 0
    for start < len(hexStr) && hexStr[start] == '0' {
        start++
    }
    if start == len(hexStr) {
        return "0x0"
    }
    return ID("0x" + hexStr[start:])
}

type notifierKey struct{}

// NotifierFromContext returns the Notifier value stored in ctx, if any.
func NotifierFromContext(ctx context.Context) (*Notifier, bool) {
	n, ok := ctx.Value(notifierKey{}).(*Notifier)
	return n, ok
}

// Notifier is tied to a RPC connection that supports subscriptions.
// Server callbacks use the notifier to send notifications.
type Notifier struct {
	h         *handler
	namespace string

	mu           sync.Mutex
	sub          *Subscription
	buffer       []json.RawMessage
	callReturned bool
	activated    bool
}

// CreateSubscription returns a new subscription that is coupled to the
// RPC connection. By default subscriptions are inactive and notifications
// are dropped until the subscription is marked as active. This is done
// by the RPC server after the subscription ID is send to the client.
func (n *Notifier) CreateSubscription() *Subscription {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.sub != nil {
		panic("can't create multiple subscriptions with Notifier")
	} else if n.callReturned {
		panic("can't create subscription after subscribe call has returned")
	}
	n.sub = &Subscription{ID: n.h.idgen(), namespace: n.namespace, err: make(chan error, 1)}
	return n.sub
}

// Notify sends a notification to the client with the given data as payload.
// If an error occurs the RPC connection is closed and the error is returned.
func (n *Notifier) Notify(id ID, data interface{}) error {
	enc, err := json.Marshal(data)
	if err != nil {
		return err
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if n.sub == nil {
		panic("can't Notify before subscription is created")
	} else if n.sub.ID != id {
		panic("Notify with wrong ID")
	}
	if n.activated {
		return n.send(n.sub, enc)
	}
	n.buffer = append(n.buffer, enc)
	return nil
}

// Closed returns a channel that is closed when the RPC connection is closed.
// Deprecated: use subscription error channel
func (n *Notifier) Closed() <-chan interface{} {
	return n.h.conn.closed()
}

// takeSubscription returns the subscription (if one has been created). No subscription can
// be created after this call.
func (n *Notifier) takeSubscription() *Subscription {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.callReturned = true
	return n.sub
}

// activate is called after the subscription ID was sent to client. Notifications are
// buffered before activation. This prevents notifications being sent to the client before
// the subscription ID is sent to the client.
func (n *Notifier) activate() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, data := range n.buffer {
		if err := n.send(n.sub, data); err != nil {
			return err
		}
	}
	n.activated = true
	return nil
}

func (n *Notifier) send(sub *Subscription, data json.RawMessage) error {
	params, _ := json.Marshal(&subscriptionResult{ID: string(sub.ID), Result: data})
	ctx := context.Background()
	return n.h.conn.WriteJSON(ctx, &jsonrpcMessage{
		Version: vsn,
		Method:  n.namespace + notificationMethodSuffix,
		Params:  params,
	})
}

// A Subscription is created by a notifier and tied to that notifier. The client can use
// this subscription to wait for an unsubscribe request for the client, see Err().
type Subscription struct {
	ID        ID
	namespace string
	err       chan error // closed on unsubscribe
}

// Err returns a channel that is closed when the client send an unsubscribe request.
func (s *Subscription) Err() <-chan error {
	return s.err
}

// MarshalJSON marshals a subscription as its ID.
func (s *Subscription) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.ID)
}

// ClientSubscription is a subscription established through the Client's Subscribe or
// EthSubscribe methods.
type ClientSubscription struct {
	client    *Client
	etype     reflect.Type
	channel   reflect.Value
	namespace string
	subid     string
	in        chan json.RawMessage

	quitOnce sync.Once     // ensures quit is closed once
	quit     chan struct{} // quit is closed when the subscription exits
	errOnce  sync.Once     // ensures err is closed once
	err      chan error
}

func newClientSubscription(c *Client, namespace string, channel reflect.Value) *ClientSubscription {
	sub := &ClientSubscription{
		client:    c,
		namespace: namespace,
		etype:     channel.Type().Elem(),
		channel:   channel,
		quit:      make(chan struct{}),
		err:       make(chan error, 1),
		in:        make(chan json.RawMessage),
	}
	return sub
}

// Err returns the subscription error channel. The intended use of Err is to schedule
// resubscription when the client connection is closed unexpectedly.
//
// The error channel receives a value when the subscription has ended due
// to an error. The received error is nil if Close has been called
// on the underlying client and no other error has occurred.
//
// The error channel is closed when Unsubscribe is called on the subscription.
func (sub *ClientSubscription) Err() <-chan error {
	return sub.err
}

// Unsubscribe unsubscribes the notification and closes the error channel.
// It can safely be called more than once.
func (sub *ClientSubscription) Unsubscribe() {
	sub.quitWithError(true, nil)
	sub.errOnce.Do(func() { close(sub.err) })
}
