package engine_types

type EngineStatus string

const (
	ValidStatus            EngineStatus = "VALID"
	InvalidStatus          EngineStatus = "INVALID"
	SyncingStatus          EngineStatus = "SYNCING"
	AcceptedStatus         EngineStatus = "ACCEPTED"
	InvalidBlockHashStatus EngineStatus = "INVALID_BLOCK_HASH"
)
