package silkworm

import (
	"errors"
	"math/big"

	"github.com/erigontech/silkworm-go"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus"
)

type Silkworm = silkworm_go.Silkworm
type SentrySettings = silkworm_go.SentrySettings
type MappedHeaderSnapshot = silkworm_go.MappedHeaderSnapshot
type MappedBodySnapshot = silkworm_go.MappedBodySnapshot
type MappedTxnSnapshot = silkworm_go.MappedTxnSnapshot
type MappedChainSnapshot = silkworm_go.MappedChainSnapshot

var New = silkworm_go.New
var NewMemoryMappedRegion = silkworm_go.NewMemoryMappedRegion
var NewMappedHeaderSnapshot = silkworm_go.NewMappedHeaderSnapshot
var NewMappedBodySnapshot = silkworm_go.NewMappedBodySnapshot
var NewMappedTxnSnapshot = silkworm_go.NewMappedTxnSnapshot

var ErrInterrupted = silkworm_go.ErrInterrupted

type RpcDaemonService struct {
	silkworm *Silkworm
	db       kv.RoDB
}

func NewRpcDaemonService(s *Silkworm, db kv.RoDB) RpcDaemonService {
	return RpcDaemonService{
		silkworm: s,
		db:       db,
	}
}

func (service RpcDaemonService) Start() error {
	return service.silkworm.StartRpcDaemon(service.db.CHandle())
}

func (service RpcDaemonService) Stop() error {
	return service.silkworm.StopRpcDaemon()
}

type SentryService struct {
	silkworm *silkworm_go.Silkworm
	settings silkworm_go.SentrySettings
}

func NewSentryService(s *Silkworm, settings silkworm_go.SentrySettings) SentryService {
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

func ExecuteBlocks(s *Silkworm, txn kv.Tx, chainID *big.Int, startBlock uint64, maxBlock uint64, batchSize uint64, writeChangeSets, writeReceipts, writeCallTraces bool) (uint64, error) {
	lastExecutedBlock, err := s.ExecuteBlocks(txn.CHandle(), chainID, startBlock, maxBlock, batchSize, writeChangeSets, writeReceipts, writeCallTraces)
	if (err != nil) && errors.Is(err, silkworm_go.ErrInvalidBlock) {
		return lastExecutedBlock, consensus.ErrInvalidBlock
	}
	return lastExecutedBlock, err
}

type CanAddSnapshotsToSilkwarm interface {
	AddSnapshotsToSilkworm(*Silkworm) error
}
