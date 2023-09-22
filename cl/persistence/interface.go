package persistence

import (
	"context"
	"database/sql"
	"io"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"
)

type BlockSource interface {
	GetRange(tx *sql.Tx, ctx context.Context, from uint64, count uint64) ([]*peers.PeeredObject[*cltypes.SignedBeaconBlock], error)
	PurgeRange(tx *sql.Tx, ctx context.Context, from uint64, count uint64) error
}

type BeaconChainWriter interface {
	WriteBlock(tx *sql.Tx, ctx context.Context, block *cltypes.SignedBeaconBlock, canonical bool) error
}

type RawBeaconBlockChain interface {
	BlockWriter(ctx context.Context, slot uint64, blockRoot libcommon.Hash) (io.WriteCloser, error)
	BlockReader(ctx context.Context, slot uint64, blockRoot libcommon.Hash) (io.ReadCloser, error)
	DeleteBlock(ctx context.Context, slot uint64, blockRoot libcommon.Hash) error
}

type BeaconChainDatabase interface {
	BlockSource
	BeaconChainWriter
}
