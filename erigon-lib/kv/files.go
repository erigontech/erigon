package kv

const (
	// Domains: File<identifier>Domain constant used
	// as part of the filenames in the /snapshots/{domain/history/idx} dirs.
	//
	// Must have corresponding constants in tables.go file in the format:
	//
	// Tbl<identifier>Keys
	// Tbl<identifier>Vals
	// Tbl<identifier>HistoryKeys
	// Tbl<identifier>HistoryVals
	FileAccountDomain    = "accounts"
	FileStorageDomain    = "storage"
	FileCodeDomain       = "code"
	FileCommitmentDomain = "commitment"

	// Inverted indexes: File<identifier>Idx constant used
	// as part of the filenames in the /snapshots/idx dir.
	//
	// Must have corresponding constants in tables.go file in the format:
	//
	// Tbl<identifier>Keys
	// Tbl<identifier>Idx
	//
	// They correspond to the "hot" DB tables for these indexes.
	FileLogAddressIdx = "logaddrs"
	FileLogTopicsIdx  = "logtopics"
	FileTracesFromIdx = "tracesfrom"
	FileTracesToIdx   = "tracesto"
)
