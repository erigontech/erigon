// Copyright 2024 The Erigon Authors
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

package services

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/devnet/devnet"
	"github.com/erigontech/erigon/cmd/devnet/devnetutils"
	"github.com/erigontech/erigon/cmd/devnet/scenarios"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/requests"
)

func init() {
	scenarios.RegisterStepHandlers(
		scenarios.StepHandler(InitSubscriptions),
	)
}

var subscriptions map[string]map[requests.SubMethod]*Subscription

func GetSubscription(chainName string, method requests.SubMethod) *Subscription {
	if methods, ok := subscriptions[chainName]; ok {
		if subscription, ok := methods[method]; ok {
			return subscription
		}
	}

	return nil
}

// Subscription houses the client subscription, name and channel for its delivery
type Subscription struct {
	Client    *rpc.Client
	ClientSub *rpc.ClientSubscription
	Name      requests.SubMethod
	SubChan   chan interface{}
}

// NewSubscription returns a new Subscription instance
func NewSubscription(name requests.SubMethod) *Subscription {
	return &Subscription{
		Name:    name,
		SubChan: make(chan interface{}),
	}
}

func InitSubscriptions(ctx context.Context, methods []requests.SubMethod) {
	logger := devnet.Logger(ctx)

	logger.Trace("CONNECTING TO WEBSOCKETS AND SUBSCRIBING TO METHODS...")
	if err := subscribeAll(ctx, methods); err != nil {
		logger.Error("failed to subscribe to all methods", "error", err)
		return
	}
}

// subscribe connects to a websocket client and returns the subscription handler and a channel buffer
func subscribe(client *rpc.Client, method requests.SubMethod, args ...interface{}) (*Subscription, error) {
	methodSub := NewSubscription(method)

	namespace, subMethod, err := devnetutils.NamespaceAndSubMethodFromMethod(string(method))
	if err != nil {
		return nil, fmt.Errorf("cannot get namespace and submethod from method: %v", err)
	}

	arr := append([]interface{}{subMethod}, args...)

	sub, err := client.Subscribe(context.Background(), namespace, methodSub.SubChan, arr...)
	if err != nil {
		return nil, fmt.Errorf("client failed to subscribe: %v", err)
	}

	methodSub.ClientSub = sub
	methodSub.Client = client

	return methodSub, nil
}

func subscribeToMethod(target string, method requests.SubMethod, logger log.Logger) (*Subscription, error) {
	client, err := rpc.DialWebsocket(context.Background(), "ws://"+target, "", logger)

	if err != nil {
		return nil, fmt.Errorf("failed to dial websocket: %v", err)
	}

	sub, err := subscribe(client, method)
	if err != nil {
		return nil, fmt.Errorf("error subscribing to method: %v", err)
	}

	return sub, nil
}

// UnsubscribeAll closes all the client subscriptions and empties their global subscription channel
func UnsubscribeAll() {
	if subscriptions == nil {
		return
	}

	for _, methods := range subscriptions {

		for _, methodSub := range methods {
			if methodSub != nil {
				methodSub.ClientSub.Unsubscribe()
				for len(methodSub.SubChan) > 0 {
					<-methodSub.SubChan
				}
				methodSub.SubChan = nil // avoid memory leak
			}
		}
	}
}

// subscribeAll subscribes to the range of methods provided
func subscribeAll(ctx context.Context, methods []requests.SubMethod) error {
	subscriptions = map[string]map[requests.SubMethod]*Subscription{}
	logger := devnet.Logger(ctx)

	for _, network := range devnet.Networks(ctx) {
		subscriptions[network.Chain] = map[requests.SubMethod]*Subscription{}

		for _, method := range methods {
			sub, err := subscribeToMethod(devnet.HTTPHost(network.Nodes[0]), method, logger)
			if err != nil {
				return err
			}
			subscriptions[network.Chain][method] = sub
		}
	}

	return nil
}
