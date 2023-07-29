package engineapi

import (
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

func (e *EngineServerExperimental) StartEngineMessageHandler() {
	go func() {
		for {
			interrupted, err := e.engineMessageHandler()
			if interrupted {
				return
			}
			if err != nil {
				e.logger.Error("[EngineServer] error received", "err", err)
			}
		}
	}()
}

// This loop is responsible for engine POS replacement.
func (e *EngineServerExperimental) engineMessageHandler() (bool, error) {
	logPrefix := "EngineApi"
	e.hd.SetPOSSync(true)
	syncing := e.blockDownloader.Status() != headerdownload.Idle
	if !syncing {
		e.logger.Info(fmt.Sprintf("[%s] Waiting for Consensus Layer...", logPrefix))
	}
	interrupt, requestId, requestWithStatus := e.hd.BeaconRequestList.WaitForRequest(syncing, e.test)

	chainReader := eth1_chain_reader.NewChainReaderEth1(e.ctx, e.config, e.executionService)
	e.hd.SetHeaderReader(chainReader)

	interrupted, err := e.handleInterrupt(interrupt)
	if err != nil {
		return false, err
	}

	if interrupted {
		return true, nil
	}

	if requestWithStatus == nil {
		e.logger.Warn(fmt.Sprintf("[%s] Nil beacon request. Should only happen in tests", logPrefix))
		return false, nil
	}

	request := requestWithStatus.Message
	requestStatus := requestWithStatus.Status

	// Decide what kind of action we need to take place
	forkChoiceMessage, forkChoiceInsteadOfNewPayload := request.(*engine_types.ForkChoiceState)
	e.hd.ClearPendingPayloadHash()
	e.hd.SetPendingPayloadStatus(nil)

	var payloadStatus *engine_types.PayloadStatus
	if forkChoiceInsteadOfNewPayload {
		payloadStatus, err = e.handlesForkChoice("ForkChoiceUpdated", chainReader, forkChoiceMessage, requestId)
	} else {
		payloadMessage := request.(*types.Block)
		payloadStatus, err = e.handleNewPayload("NewPayload", payloadMessage, requestStatus, requestId, chainReader)
	}

	if err != nil {
		if requestStatus == engine_helpers.New {
			e.hd.PayloadStatusCh <- engine_types.PayloadStatus{CriticalError: err}
		}
		return false, err
	}

	if requestStatus == engine_helpers.New && payloadStatus != nil {
		if payloadStatus.Status == engine_types.SyncingStatus || payloadStatus.Status == engine_types.AcceptedStatus {
			e.hd.PayloadStatusCh <- *payloadStatus
		} else {
			// Let the stage loop run to the end so that the transaction is committed prior to replying to CL
			e.hd.SetPendingPayloadStatus(payloadStatus)
		}
	}

	return false, nil
}

func (e *EngineServerExperimental) handleInterrupt(interrupt engine_helpers.Interrupt) (bool, error) {
	if interrupt != engine_helpers.None {
		if interrupt == engine_helpers.Stopping {
			close(e.hd.ShutdownCh)
			return false, fmt.Errorf("server is stopping")
		}
		return true, nil
	}
	return false, nil
}

func (e *EngineServerExperimental) handleNewPayload(
	logPrefix string,
	block *types.Block,
	requestStatus engine_helpers.RequestStatus,
	requestId int,
	chainReader consensus.ChainHeaderReader,
) (*engine_types.PayloadStatus, error) {
	header := block.Header()
	headerNumber := header.Number.Uint64()
	headerHash := block.Hash()

	e.logger.Info(fmt.Sprintf("[%s] Handling new payload", logPrefix), "height", headerNumber, "hash", headerHash)

	parent := chainReader.GetHeaderByHash(header.ParentHash)
	if parent == nil {
		e.logger.Debug(fmt.Sprintf("[%s] New payload: need to download parent", logPrefix), "height", headerNumber, "hash", headerHash, "parentHash", header.ParentHash)
		if e.test {
			e.hd.BeaconRequestList.Remove(requestId)
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}

		if !e.blockDownloader.StartDownloading(requestId, header.ParentHash, headerHash) {
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}
		currentHeader := chainReader.CurrentHeader()
		var currentHeadNumber *uint64
		if currentHeader != nil {
			currentHeadNumber = new(uint64)
			*currentHeadNumber = currentHeader.Number.Uint64()
		}
		if currentHeadNumber != nil && math.AbsoluteDifference(*currentHeadNumber, headerNumber) < 32 {
			// We try waiting until we finish downloading the PoS blocks if the distance from the head is enough,
			// so that we will perform full validation.
			success := false
			for i := 0; i < 10; i++ {
				time.Sleep(10 * time.Millisecond)
				if e.blockDownloader.Status() == headerdownload.Synced {
					success = true
					break
				}
			}
			if !success {
				return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
			}
		} else {
			return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
		}
	}

	e.hd.BeaconRequestList.Remove(requestId)
	// Save header and body
	if err := eth1_utils.InsertHeaderAndBodyAndWait(e.ctx, e.executionService, header, block.RawBody()); err != nil {
		return nil, err
	}

	e.logger.Debug(fmt.Sprintf("[%s] New payload begin verification", logPrefix))
	status, latestValidHash, err := eth1_utils.ValidateChain(e.ctx, e.executionService, headerHash, headerNumber)
	e.logger.Debug(fmt.Sprintf("[%s] New payload verification ended", logPrefix), "status", status.String(), "err", err)
	if err != nil {
		return nil, err
	}

	return &engine_types.PayloadStatus{
		Status:          convertGrpcStatusToEngineStatus(status),
		LatestValidHash: &latestValidHash,
	}, nil
}

func convertGrpcStatusToEngineStatus(status execution.ValidationStatus) engine_types.EngineStatus {
	switch status {
	case execution.ValidationStatus_Success:
		return engine_types.ValidStatus
	case execution.ValidationStatus_MissingSegment | execution.ValidationStatus_TooFarAway:
		return engine_types.AcceptedStatus
	case execution.ValidationStatus_BadBlock:
		return engine_types.InvalidStatus
	case execution.ValidationStatus_Busy:
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

	if headerNumber == nil {
		e.logger.Debug(fmt.Sprintf("[%s] Fork choice: need to download header with hash %x", logPrefix, headerHash))
		if e.test {
			e.hd.BeaconRequestList.Remove(requestId)
		} else {
			e.blockDownloader.StartDownloading(requestId, headerHash, headerHash)
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
			e.blockDownloader.StartDownloading(requestId, headerHash, headerHash)
		}
		return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
	}
	e.hd.BeaconRequestList.Remove(requestId)

	// Call forkchoice here
	status, latestValidHash, err := eth1_utils.UpdateForkChoice(e.ctx, e.executionService, forkChoice.HeadHash, forkChoice.SafeBlockHash, forkChoice.FinalizedBlockHash, 100)
	if err != nil {
		return nil, err
	}
	if status == execution.ValidationStatus_Busy {
		return &engine_types.PayloadStatus{Status: engine_types.SyncingStatus}, nil
	}
	if status == execution.ValidationStatus_BadBlock {
		return &engine_types.PayloadStatus{Status: engine_types.InvalidStatus}, nil
	}
	return &engine_types.PayloadStatus{
		Status:          convertGrpcStatusToEngineStatus(status),
		LatestValidHash: &latestValidHash,
	}, nil
}
