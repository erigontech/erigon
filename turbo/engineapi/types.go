package engineapi

import (
	"context"
	"encoding/binary"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/engine"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
)

// Engine API specifies that payloadId is 8 bytes
func addPayloadId(json map[string]interface{}, payloadId uint64) {
	if payloadId != 0 {
		encodedPayloadId := make([]byte, 8)
		binary.BigEndian.PutUint64(encodedPayloadId, payloadId)
		json["payloadId"] = hexutility.Bytes(encodedPayloadId)
	}
}

func convertPayloadStatusToJson(ctx context.Context, db kv.RoDB, x *engine.EnginePayloadStatus) (map[string]interface{}, error) {
	json := map[string]interface{}{
		"status": x.Status.String(),
	}
	if x.ValidationError != "" {
		json["validationError"] = x.ValidationError
	}
	if x.LatestValidHash == nil || (x.Status != engine.EngineStatus_VALID && x.Status != engine.EngineStatus_INVALID) {
		return json, nil
	}

	latestValidHash := common.Hash(gointerfaces.ConvertH256ToHash(x.LatestValidHash))
	if latestValidHash == (common.Hash{}) || x.Status == engine.EngineStatus_VALID {
		json["latestValidHash"] = latestValidHash
		return json, nil
	}

	// Per the Engine API spec latestValidHash should be set to 0x0000000000000000000000000000000000000000000000000000000000000000
	// if it refers to a PoW block.
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	isValidHashPos, err := rawdb.IsPosBlock(tx, latestValidHash)
	if err != nil {
		return nil, err
	}

	if isValidHashPos {
		json["latestValidHash"] = latestValidHash
	} else {
		json["latestValidHash"] = common.Hash{}
	}
	return json, nil
}
