package engineapi

import (
	"errors"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_helpers"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1/eth1_chain_reader.go"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1/eth1_utils"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

const fcuTimeout = 1000 // according to mathematics: 1000 millisecods = 1 second

var errInvalidForkChoiceState = errors.New("forkchoice state is invalid")

func (e *EngineServerExperimental) handleNewPayload(
	logPrefix string,
	block *types.Block,
	chainReader consensus.ChainHeaderReader,
) (*engine_types.PayloadStatus, error) {
	header := block.Header()
	headerNumber := header.Number.Uint64()
	headerHash := block.Hash()

	e.logger.Info(fmt.Sprintf("[%s] Handling new payload", logPrefix), "height", headerNumber, "hash", headerHash)

	currentHeader := chainReader.CurrentHeader()
	var currentHeadNumber *uint64
	if currentHeader != nil {
		currentHeadNumber = new(uint64)
		*currentHeadNumber = currentHeader.Number.Uint64()
	}
	parent := chainReader.GetHeader(header.ParentHash, headerNumber-1)
	if parent == nil {
		e.logger.Debug(fmt.Sprintf("[%s] New payload: need to download parent", logPrefix), "height", headerNumber, "hash", headerHash, "parentHash", header.ParentHash)
		if e.test {
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}

		if !e.blockDownloader.StartDownloading(0, header.ParentHash, headerHash, block) {
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}

		if currentHeadNumber != nil {
			// We try waiting until we finish downloading the PoS blocks if the distance from the head is enough,
			// so that we will perform full validation.
			success := false
			for i := 0; i < 100; i++ {
				time.Sleep(10 * time.Millisecond)
				if e.blockDownloader.Status() == headerdownload.Synced {
					success = true
					break
				}
			}
			if !success {
				return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
			}
			return &engine_types.PayloadStatus{Status: engine_types.ValidStatus, LatestValidHash: &headerHash}, nil
		} else {
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}
	}
	if err := eth1_utils.InsertHeaderAndBodyAndWait(e.ctx, e.executionService, header, block.RawBody()); err != nil {
		return nil, err
	}

	if math.AbsoluteDifference(*currentHeadNumber, headerNumber) >= 32 {
		return &engine_types.PayloadStatus{Status: engine_types.AcceptedStatus}, nil
	}

	e.logger.Debug(fmt.Sprintf("[%s] New payload begin verification", logPrefix))
	status, latestValidHash, err := eth1_utils.ValidateChain(e.ctx, e.executionService, headerHash, headerNumber)
	e.logger.Debug(fmt.Sprintf("[%s] New payload verification ended", logPrefix), "status", status.String(), "err", err)
	if err != nil {
		return nil, err
	}

	if status == execution.ExecutionStatus_BadBlock {
		e.hd.ReportBadHeaderPoS(block.Hash(), latestValidHash)
	}

	return &engine_types.PayloadStatus{
		Status:          convertGrpcStatusToEngineStatus(status),
		LatestValidHash: &latestValidHash,
	}, nil
}

func convertGrpcStatusToEngineStatus(status execution.ExecutionStatus) engine_types.EngineStatus {
	switch status {
	case execution.ExecutionStatus_Success:
		return engine_types.ValidStatus
	case execution.ExecutionStatus_MissingSegment:
		return engine_types.AcceptedStatus
	case execution.ExecutionStatus_TooFarAway:
		return engine_types.AcceptedStatus
	case execution.ExecutionStatus_BadBlock:
		return engine_types.InvalidStatus
	case execution.ExecutionStatus_Busy:
		return engine_types.SyncingStatus
	}
	panic("giulio u stupid.")
}

func (e *EngineServerExperimental) handlesForkChoice(
	logPrefix string,
	chainReader *eth1_chain_reader.ChainReaderEth1,
	forkChoice *engine_types.ForkChoiceState,
	requestId int,
) (*engine_types.PayloadStatus, error) {
	headerHash := forkChoice.HeadHash

	e.logger.Debug(fmt.Sprintf("[%s] Handling fork choice", logPrefix), "headerHash", headerHash)
	headerNumber, err := chainReader.HeaderNumber(headerHash)
	if err != nil {
		return nil, err
	}

	// We do not have header, download.
	if headerNumber == nil {
		e.logger.Debug(fmt.Sprintf("[%s] Fork choice: need to download header with hash %x", logPrefix, headerHash))
		if e.test {
			e.hd.BeaconRequestList.Remove(requestId)
		} else {
			e.blockDownloader.StartDownloading(requestId, headerHash, headerHash, nil)
		}
		return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
	}

	// Header itself may already be in the snapshots, if CL starts off at much earlier state than Erigon
	header := chainReader.GetHeader(headerHash, *headerNumber)
	if header == nil {
		e.logger.Debug(fmt.Sprintf("[%s] Fork choice: need to download header with hash %x", logPrefix, headerHash))
		if e.test {
			e.hd.BeaconRequestList.Remove(requestId)
		} else {
			e.blockDownloader.StartDownloading(requestId, headerHash, headerHash, nil)
		}

		return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
	}

	// Call forkchoice here
	status, latestValidHash, err := eth1_utils.UpdateForkChoice(e.ctx, e.executionService, forkChoice.HeadHash, forkChoice.SafeBlockHash, forkChoice.FinalizedBlockHash, fcuTimeout)
	if err != nil {
		return nil, err
	}
	if status == execution.ExecutionStatus_InvalidForkchoice {
		return nil, &engine_helpers.InvalidForkchoiceStateErr
	}
	if status == execution.ExecutionStatus_Busy {
		return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
	}
	if status == execution.ExecutionStatus_BadBlock {
		return &engine_types.PayloadStatus{Status: engine_types.InvalidStatus}, nil
	}
	return &engine_types.PayloadStatus{
		Status:          convertGrpcStatusToEngineStatus(status),
		LatestValidHash: &latestValidHash,
	}, nil
}
