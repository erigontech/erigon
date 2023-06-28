package engineapi

import (
	"github.com/ledgerwatch/erigon-lib/gointerfaces/engine"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/turbo/builder"
)

type EngineServer struct {
	// Block proposing for proof-of-stake
	payloadId      uint64
	lastParameters *core.BlockBuilderParameters
	builders       map[uint64]*builder.BlockBuilder
	builderFunc    builder.BlockBuilderFunc
	proposing      bool

	engine.UnimplementedEngineServer
}
