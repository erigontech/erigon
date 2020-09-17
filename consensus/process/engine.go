package process

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
)

type RemoteEngine struct {
	consensus.Engine

	consensus.EngineProcess
	exit chan struct{}
}

func NewRemoteEngine(e consensus.Engine, chain consensus.ChainHeaderReader) *RemoteEngine {
	exit := make(chan struct{})

	return &RemoteEngine{
		e,
		NewConsensusProcess(e, chain, exit),
		exit,
	}
}

func (r *RemoteEngine) Close() error {
	err := r.Engine.Close()
	common.SafeClose(r.exit)
	return err
}
