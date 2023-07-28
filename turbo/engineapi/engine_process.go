package engineapi

// This loop is responsible for engine POS replacement.
func (e *EngineServerExperimental) EngineLoopHandler() {
	// logPrefix := "EngineApi"
	// e.hd.SetPOSSync(true)
	// syncing := e.blockDownloader.Status() != headerdownload.Idle
	// if !syncing {
	// 	e.logger.Info(fmt.Sprintf("[] Waiting for Consensus Layer...", logPrefix))
	// }
	// interrupt, requestId, requestWithStatus := e.hd.BeaconRequestList.WaitForRequest(syncing, e.test)

	// e.hd.SetHeaderReader(&stagedsync.ChainReaderImpl{config: &cfg.chainConfig, tx: tx, blockReader: cfg.blockReader})

	// interrupted, err := handleInterrupt(interrupt, cfg, tx, useExternalTx, logger)
	// if err != nil {
	// 	return err
	// }

	// if interrupted {
	// 	return nil
	// }

	// if requestWithStatus == nil {
	// 	e.logger.Warn(fmt.Sprintf("[%s] Nil beacon request. Should only happen in tests", s.LogPrefix()))
	// 	return
	// }

	// request := requestWithStatus.Message
	// requestStatus := requestWithStatus.Status

	// // Decide what kind of action we need to take place
	// forkChoiceMessage, forkChoiceInsteadOfNewPayload := request.(*engine_types.ForkChoiceState)
	// e.hd.ClearPendingPayloadHash()
	// e.hd.SetPendingPayloadStatus(nil)

	// var payloadStatus *engine_types.PayloadStatus
	// if forkChoiceInsteadOfNewPayload {
	// 	payloadStatus, err = startHandlingForkChoice(forkChoiceMessage, requestStatus, requestId, s, u, ctx, tx, cfg, test, headerInserter, preProgress, logger)
	// } else {
	// 	payloadMessage := request.(*types.Block)
	// 	payloadStatus, err = handleNewPayload(payloadMessage, requestStatus, requestId, s, ctx, tx, cfg, test, headerInserter, logger)
	// }

	// if err != nil {
	// 	if requestStatus == engine_helpers.New {
	// 		e.hd.PayloadStatusCh <- engine_types.PayloadStatus{CriticalError: err}
	// 	}
	// 	return err
	// }

	// if requestStatus == engine_helpers.New && payloadStatus != nil {
	// 	if payloadStatus.Status == engine_types.SyncingStatus || payloadStatus.Status == engine_types.AcceptedStatus || !useExternalTx {
	// 		e.hd.PayloadStatusCh <- *payloadStatus
	// 	} else {
	// 		// Let the stage loop run to the end so that the transaction is committed prior to replying to CL
	// 		e.hd.SetPendingPayloadStatus(payloadStatus)
	// 	}
	// }

	// return nil
}
