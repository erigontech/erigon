package l1infotree

import (
	"context"
	"encoding/json"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	zkTypes "github.com/ledgerwatch/erigon/zk/types"
	jsonClient "github.com/ledgerwatch/erigon/zkevm/jsonrpc/client"
	"github.com/ledgerwatch/log/v3"
	"sync/atomic"
	"time"
)

const (
	ExitRootTable = "zkevm_getExitRootTable"
)

// InfoTreeL2RpcSyncer is a struct that is used to sync the Info Tree from an L2 Sequencer RPC.
type InfoTreeL2RpcSyncer struct {
	ctx            context.Context
	zkCfg          *ethconfig.Zk
	isSyncStarted  atomic.Bool
	isSyncFinished atomic.Bool
	infoTreeChan   chan []zkTypes.L1InfoTreeUpdate
}

// NewInfoTreeL2RpcSyncer creates a new InfoTreeL2RpcSyncer.
// It is used to sync the Info Tree from an L2 Sequencer RPC.
// The sequencer must have the full Info Tree synced from the L1.
func NewInfoTreeL2RpcSyncer(ctx context.Context, zkCfg *ethconfig.Zk) *InfoTreeL2RpcSyncer {
	return &InfoTreeL2RpcSyncer{
		ctx:          ctx,
		zkCfg:        zkCfg,
		infoTreeChan: make(chan []zkTypes.L1InfoTreeUpdate),
	}
}

func (s *InfoTreeL2RpcSyncer) IsSyncStarted() bool {
	return s.isSyncStarted.Load()
}

func (s *InfoTreeL2RpcSyncer) IsSyncFinished() bool {
	return s.isSyncFinished.Load()
}

func (s *InfoTreeL2RpcSyncer) GetInfoTreeChan() chan []zkTypes.L1InfoTreeUpdate {
	return s.infoTreeChan
}

// ConsumeInfoTree consumes the Info Tree from the Info Tree chan.
func (s *InfoTreeL2RpcSyncer) ConsumeInfoTree() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.infoTreeChan:
		default:
			if !s.isSyncStarted.Load() {
				return
			}
			time.Sleep(time.Second)
		}
	}
}

// RunSyncInfoTree runs the sync process for the Info Tree from an L2 Sequencer RPC and put the updates in the Info Tree chan.
func (s *InfoTreeL2RpcSyncer) RunSyncInfoTree() {
	if s.isSyncStarted.Load() {
		return
	}
	s.isSyncStarted.Store(true)
	s.isSyncFinished.Store(false)

	totalSynced := uint64(0)
	batchSize := s.zkCfg.L2InfoTreeUpdatesBatchSize

	go func() {
		retry := 0
		for {
			select {
			case <-s.ctx.Done():
				s.isSyncFinished.Store(true)
				break
			default:
				query := exitRootQuery{
					From: totalSynced + 1,
					To:   totalSynced + batchSize,
				}
				infoTree, err := getExitRootTable(s.zkCfg.L2RpcUrl, query)
				if err != nil {
					log.Debug("getExitRootTable retry error", "err", err)
					retry++
					if retry > 5 {
						return
					}
					time.Sleep(time.Duration(retry*2) * time.Second)
				}

				if len(infoTree) == 0 {
					s.isSyncFinished.Store(true)
					return
				}

				s.infoTreeChan <- infoTree
				totalSynced = query.To
			}
		}
	}()
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
