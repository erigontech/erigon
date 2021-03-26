package process

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/params"
)

type RemoteEngine struct {
	consensus.Engine

	consensus.EngineAPI
	exit chan struct{}
}

func NewRemoteEngine(e consensus.Engine, config *params.ChainConfig, workers int) *RemoteEngine {
	exit := make(chan struct{})

	return &RemoteEngine{
		e,
		NewConsensusProcess(e, config, exit, workers),
		exit,
	}
}

func (r *RemoteEngine) Close() error {
	err := r.Engine.Close()
	common.SafeClose(r.exit)
	return err
}
