package execution_client

import "github.com/ledgerwatch/erigon/turbo/engineapi/engine_types"

type PayloadStatus int

const (
	PayloadStatusNone = iota
	PayloadStatusNotValidated
	PayloadStatusInvalidated
	PayloadStatusValidated
)

func newPayloadStatusByEngineStatus(status engine_types.EngineStatus) PayloadStatus {
	switch status {
	case engine_types.SyncingStatus, engine_types.AcceptedStatus:
		return PayloadStatusNotValidated
	case engine_types.InvalidStatus, engine_types.InvalidBlockHashStatus:
		return PayloadStatusInvalidated
	case engine_types.ValidStatus:
		return PayloadStatusValidated
	}
	return PayloadStatusNone
}
