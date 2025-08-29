package l1infotree

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/eth/ethconfig"
	zkTypes "github.com/erigontech/erigon/zk/types"
	jsonClient "github.com/erigontech/erigon/zkevm/jsonrpc/client"
)

const (
	ExitRootTable            = "zkevm_getExitRootTable"
	infoTreeChanSize         = 100 // Size of the channel to hold info tree updates
	exitRootTableMaxRetry    = 5   // Max retries for exit root table queries
	exitRootTableRetryFactor = 2   // Factor to increase retry delay
)

// InfoTreeL2RpcSyncer is a struct that is used to sync the Info Tree from an L2 Sequencer RPC.
type InfoTreeL2RpcSyncer struct {
	ctx           context.Context
	zkCfg         *ethconfig.Zk
	isSyncStarted atomic.Bool
	infoTreeChan  chan []zkTypes.L1InfoTreeUpdate
	err           error // Error encountered during sync
}

// NewInfoTreeL2RpcSyncer creates a new InfoTreeL2RpcSyncer.
// It is used to sync the Info Tree from an L2 Sequencer RPC.
// The sequencer must have the full Info Tree synced from the L1.
func NewInfoTreeL2RpcSyncer(ctx context.Context, zkCfg *ethconfig.Zk) *InfoTreeL2RpcSyncer {
	return &InfoTreeL2RpcSyncer{
		ctx:          ctx,
		zkCfg:        zkCfg,
		infoTreeChan: nil,
	}
}

func (s *InfoTreeL2RpcSyncer) IsSyncStarted() bool {
	return s.isSyncStarted.Load()
}

func (s *InfoTreeL2RpcSyncer) GetError() error {
	return s.err
}

// RunSyncInfoTree runs the sync process for the Info Tree from an L2 Sequencer RPC and put the updates in the Info Tree chan.
func (s *InfoTreeL2RpcSyncer) RunSyncInfoTree() <-chan []zkTypes.L1InfoTreeUpdate {
	if s.isSyncStarted.Load() {
		return s.infoTreeChan
	}
	s.isSyncStarted.Store(true)
	s.infoTreeChan = make(chan []zkTypes.L1InfoTreeUpdate, infoTreeChanSize) // Buffered channel to avoid blocking

	totalSynced := uint64(0)
	batchSize := s.zkCfg.L2InfoTreeUpdatesBatchSize

	go func() {
		defer func() {
			close(s.infoTreeChan)
			s.isSyncStarted.Store(false)
		}()

		retry := 0
		for {
			select {
			case <-s.ctx.Done():
				return
			default:
				query := exitRootQuery{
					From: totalSynced + 1,
					To:   totalSynced + batchSize,
				}
				infoTree, err := getExitRootTable(s.zkCfg.L2InfoTreeUpdatesURL, query)
				if err != nil {
					log.Info("getExitRootTable retry error", "err", err)
					retry++
					if retry > exitRootTableMaxRetry {
						s.err = err
						return
					}
					time.Sleep(time.Duration(retry*exitRootTableRetryFactor) * time.Second)
				}

				if len(infoTree) == 0 {
					return
				}

				s.infoTreeChan <- infoTree
				totalSynced = query.To
			}
		}
	}()

	return s.infoTreeChan
}

type exitRootQuery struct {
	From uint64 `json:"from"`
	To   uint64 `json:"to"`
}

func getExitRootTable(endpoint string, query exitRootQuery) ([]zkTypes.L1InfoTreeUpdate, error) {
	res, err := jsonClient.JSONRPCCall(endpoint, ExitRootTable, query)
	if err != nil {
		return nil, err
	}

	var updates []zkTypes.L1InfoTreeUpdate
	if err = json.Unmarshal(res.Result, &updates); err != nil {
		return nil, err
	}

	return updates, nil
}
