package bor

import (
	//	"encoding/hex"
	//	"time"

	"github.com/ledgerwatch/erigon/cmd/devnet/contracts"
	//	"github.com/ledgerwatch/erigon/core/types"
)

// HandleStateSyncEvent - handle state sync event from rootchain
// 1. check if this deposit event has to be broadcasted to heimdall
// 2. create and broadcast  record transaction to heimdall

func handleStateSynced(event *contracts.TestStateSenderStateSynced) error {
	/*	start := time.Now()

		var vLog = types.Log{}
		if err := jsoniter.ConfigFastest.Unmarshal([]byte(logBytes), &vLog); err != nil {
			cp.Logger.Error("Error while unmarshalling event from rootchain", "error", err)
			return err
		}

		defer util.LogElapsedTimeForStateSyncedEvent(event, "sendStateSyncedToHeimdall", start)
		isOld, _ := cp.isOldTx(cp.cliCtx, vLog.TxHash.String(), uint64(vLog.Index), util.ClerkEvent, event)

		if isOld {
			cp.Logger.Info("Ignoring task to send deposit to heimdall as already processed",
				"event", eventName,
				"id", event.Id,
				"contract", event.ContractAddress,
				"data", hex.EncodeToString(event.Data),
				"borChainId", chainParams.BorChainID,
				"txHash", hmTypes.BytesToHeimdallHash(vLog.TxHash.Bytes()),
				"logIndex", uint64(vLog.Index),
				"blockNumber", vLog.BlockNumber,
			)

			return nil
		}

		cp.Logger.Debug(
			"⬜ New event found",
			"event", eventName,
			"id", event.Id,
			"contract", event.ContractAddress,
			"data", hex.EncodeToString(event.Data),
			"borChainId", chainParams.BorChainID,
			"txHash", hmTypes.BytesToHeimdallHash(vLog.TxHash.Bytes()),
			"logIndex", uint64(vLog.Index),
			"blockNumber", vLog.BlockNumber,
		)

		_, maxStateSyncSizeCheckSpan := tracing.StartSpan(sendStateSyncedToHeimdallCtx, "maxStateSyncSizeCheck")
		if util.GetBlockHeight(cp.cliCtx) > helper.GetSpanOverrideHeight() && len(event.Data) > helper.MaxStateSyncSize {
			cp.Logger.Info(`Data is too large to process, Resetting to ""`, "data", hex.EncodeToString(event.Data))
			event.Data = hmTypes.HexToHexBytes("")
		} else if len(event.Data) > helper.LegacyMaxStateSyncSize {
			cp.Logger.Info(`Data is too large to process, Resetting to ""`, "data", hex.EncodeToString(event.Data))
			event.Data = hmTypes.HexToHexBytes("")
		}
		tracing.EndSpan(maxStateSyncSizeCheckSpan)

		msg := clerkTypes.NewMsgEventRecord(
			hmTypes.BytesToHeimdallAddress(helper.GetAddress()),
			hmTypes.BytesToHeimdallHash(vLog.TxHash.Bytes()),
			uint64(vLog.Index),
			vLog.BlockNumber,
			event.Id.Uint64(),
			hmTypes.BytesToHeimdallAddress(event.ContractAddress.Bytes()),
			event.Data,
			chainParams.BorChainID,
		)

		// Check if we have the same transaction in mempool or not
		// Don't drop the transaction. Keep retrying after `util.RetryStateSyncTaskDelay = 24 seconds`,
		// until the transaction in mempool is processed or cancelled.
		inMempool, _ := cp.checkTxAgainstMempool(msg, event)

		if inMempool {
			cp.Logger.Info("Similar transaction already in mempool, retrying in sometime", "event", eventName, "retry delay", util.RetryStateSyncTaskDelay)
			return tasks.NewErrRetryTaskLater("transaction already in mempool", util.RetryStateSyncTaskDelay)
		}

		// return broadcast to heimdall
		err = cp.txBroadcaster.BroadcastToHeimdall(msg, event)

		if err != nil {
			cp.Logger.Error("Error while broadcasting clerk Record to heimdall", "error", err)
			return err
		}
	*/
	return nil
}
