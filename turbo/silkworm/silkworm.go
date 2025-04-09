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

package silkworm

import (
	silkworm_go "github.com/erigontech/silkworm-go"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
)

type Silkworm = silkworm_go.Silkworm
type SilkwormLogLevel = silkworm_go.SilkwormLogLevel
type SentrySettings = silkworm_go.SentrySettings
type RpcDaemonSettings = silkworm_go.RpcDaemonSettings
type RpcInterfaceLogSettings = silkworm_go.RpcInterfaceLogSettings

type HeadersSnapshot = silkworm_go.HeadersSnapshot
type BodiesSnapshot = silkworm_go.BodiesSnapshot
type TransactionsSnapshot = silkworm_go.TransactionsSnapshot
type BlocksSnapshotBundle = silkworm_go.BlocksSnapshotBundle
type InvertedIndexSnapshot = silkworm_go.InvertedIndexSnapshot
type HistorySnapshot = silkworm_go.HistorySnapshot
type DomainSnapshot = silkworm_go.DomainSnapshot
type StateSnapshotBundleLatest = silkworm_go.StateSnapshotBundleLatest
type StateSnapshotBundleHistorical = silkworm_go.StateSnapshotBundleHistorical

var NewFilePath = silkworm_go.NewFilePath
var ErrInterrupted = silkworm_go.ErrInterrupted

func New(dataDirPath string, libMdbxVersion string, numIOContexts uint32, logLevel log.Lvl) (*Silkworm, error) {
	var logVerbosity SilkwormLogLevel
	switch logLevel {
	case log.LvlCrit:
		logVerbosity = silkworm_go.LogLevelCritical
	case log.LvlError:
		logVerbosity = silkworm_go.LogLevelError
	case log.LvlWarn:
		logVerbosity = silkworm_go.LogLevelWarning
	case log.LvlInfo:
		logVerbosity = silkworm_go.LogLevelInfo
	case log.LvlDebug:
		logVerbosity = silkworm_go.LogLevelDebug
	case log.LvlTrace:
		logVerbosity = silkworm_go.LogLevelTrace
	}
	return silkworm_go.New(dataDirPath, libMdbxVersion, numIOContexts, logVerbosity)
}

type RpcDaemonService struct {
	silkworm *Silkworm
	db       kv.RoDB
	settings RpcDaemonSettings
}

func NewRpcDaemonService(s *Silkworm, db kv.RoDB, settings RpcDaemonSettings) RpcDaemonService {
	return RpcDaemonService{
		silkworm: s,
		db:       db,
		settings: settings,
	}
}

func (service RpcDaemonService) Start() error {
	return service.silkworm.StartRpcDaemon(service.db.CHandle(), service.settings)
}

func (service RpcDaemonService) Stop() error {
	return service.silkworm.StopRpcDaemon()
}

type SentryService struct {
	silkworm *Silkworm
	settings SentrySettings
}

func NewSentryService(s *Silkworm, settings SentrySettings) SentryService {
	return SentryService{
		silkworm: s,
		settings: settings,
	}
}

func (service SentryService) Start() error {
	return service.silkworm.SentryStart(service.settings)
}

func (service SentryService) Stop() error {
	return service.silkworm.SentryStop()
}
