// Copyright 2015 The go-ethereum Authors
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

package node

import (
	"context"
	"os"
	"os/signal"
	"reflect"
	"syscall"

	"github.com/ledgerwatch/turbo-geth/accounts"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote/remotedbserver"
	"github.com/ledgerwatch/turbo-geth/event"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// ServiceContext is a collection of service independent options inherited from
// the protocol stack, that is passed to all constructors to be optionally used;
// as well as utility methods to operate on the service environment.
type ServiceContext struct {
	services       map[reflect.Type]Service // Index of the already constructed services
	Config         Config
	EventMux       *event.TypeMux    // Event multiplexer used for decoupled notifications
	AccountManager *accounts.Manager // Account manager created by the node.
}

// OpenDatabaseWithFreezer
// FIXME: implement the functionality
func (ctx *ServiceContext) OpenDatabaseWithFreezer(name string, freezer string) (ethdb.Database, error) {
	return ctx.OpenDatabase(name)
}

// OpenDatabase opens an existing database with the given name (or creates one
// if no previous can be found) from within the node's data directory. If the
// node is an ephemeral one, a memory database is returned.
func (ctx *ServiceContext) OpenDatabase(name string) (ethdb.Database, error) {
	if ctx.Config.DataDir == "" {
		return ethdb.NewMemDatabase(), nil
	}

	if ctx.Config.BadgerDB {
		log.Info("Opening Database (Badger)")
		return ethdb.NewBadgerDatabase(ctx.Config.ResolvePath(name + "_badger"))
	}

	log.Info("Opening Database (Bolt)")
	boltDb, err := ethdb.NewBoltDatabase(ctx.Config.ResolvePath(name))
	if err != nil {
		return nil, err
	}
	if ctx.Config.RemoteDbListenAddress != "" {
		// TODO: implement node.Service, then Stop() will called on SIGINT | SIGTERM and we can call cancel() there
		tcpCtx, cancel := context.WithCancel(context.Background())
		go func() {
			ch := make(chan os.Signal, 1)
			signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
			defer signal.Stop(ch)

			select {
			case <-ch:
				log.Info("Got interrupt, shutting down...")
			case <-tcpCtx.Done():
			}

			cancel()
		}()

		go remotedbserver.Listener(tcpCtx, boltDb.DB(), ctx.Config.RemoteDbListenAddress)
	}
	return boltDb, nil
	/*
		if err != nil {
			return nil, err
		}
		root := ctx.config.ResolvePath(name)

			FIXME: restore and move to OpenDatabaseWithFreezer
			switch {
			case freezer == "":
				freezer = filepath.Join(root, "ancient")
			case !filepath.IsAbs(freezer):
				freezer = ctx.config.ResolvePath(freezer)
			}
		return ethdb.NewBoltDatabase(root)
	*/
}

// ResolvePath resolves a user path into the data directory if that was relative
// and if the user actually uses persistent storage. It will return an empty string
// for emphemeral storage and the user's own input for absolute paths.
func (ctx *ServiceContext) ResolvePath(path string) string {
	return ctx.Config.ResolvePath(path)
}

// Service retrieves a currently running service registered of a specific type.
func (ctx *ServiceContext) Service(service interface{}) error {
	element := reflect.ValueOf(service).Elem()
	if running, ok := ctx.services[element.Type()]; ok {
		element.Set(reflect.ValueOf(running))
		return nil
	}
	return ErrServiceUnknown
}

// ExtRPCEnabled returns the indicator whether node enables the external
// RPC(http, ws or graphql).
func (ctx *ServiceContext) ExtRPCEnabled() bool {
	return ctx.Config.ExtRPCEnabled()
}

// ServiceConstructor is the function signature of the constructors needed to be
// registered for service instantiation.
type ServiceConstructor func(ctx *ServiceContext) (Service, error)

// Service is an individual protocol that can be registered into a node.
//
// Notes:
//
// • Service life-cycle management is delegated to the node. The service is allowed to
// initialize itself upon creation, but no goroutines should be spun up outside of the
// Start method.
//
// • Restart logic is not required as the node will create a fresh instance
// every time a service is started.
type Service interface {
	// Protocols retrieves the P2P protocols the service wishes to start.
	Protocols() []p2p.Protocol

	// APIs retrieves the list of RPC descriptors the service provides
	APIs() []rpc.API

	// Start is called after all services have been constructed and the networking
	// layer was also initialized to spawn any goroutines required by the service.
	Start(server *p2p.Server) error

	// Stop terminates all goroutines belonging to the service, blocking until they
	// are all terminated.
	Stop() error
}
