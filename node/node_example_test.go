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

package node_test

import (
	"context"
	"fmt"
	log2 "log"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/nodecfg"
)

// SampleLifecycle is a trivial network service that can be attached to a node for
// life cycle management.
//
// The following methods are needed to implement a node.Lifecycle:
//   - Start() error              - method invoked when the node is ready to start the service
//   - Stop() error               - method invoked when the node terminates the service
type SampleLifecycle struct{}

func (s *SampleLifecycle) Start() error { fmt.Println("Service starting..."); return nil }
func (s *SampleLifecycle) Stop() error  { fmt.Println("Service stopping..."); return nil }

func ExampleLifecycle() {
	// Create a network node to run protocols with the default values.
	stack, err := node.New(context.Background(), &nodecfg.Config{}, log.New())
	if err != nil {
		log2.Fatalf("Failed to create network node: %v", err)
	}
	defer stack.Close()

	// Create and register a simple network Lifecycle.
	service := new(SampleLifecycle)
	stack.RegisterLifecycle(service)

	// Boot up the entire protocol stack, do a restart and terminate
	if err := stack.Start(); err != nil {
		log2.Fatalf("Failed to start the protocol stack: %v", err)
	}
	if err := stack.Close(); err != nil {
		log2.Fatalf("Failed to stop the protocol stack: %v", err)
	}
	// Output:
	// Service starting...
	// Service stopping...
}
