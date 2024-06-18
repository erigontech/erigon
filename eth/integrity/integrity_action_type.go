package integrity

type Action string

const (
	BlocksRead         Action = "BlocksRead"
	EfFiles            Action = "EfFiles"
	HistoryNoSystemTxs Action = "HistoryNoSystemTxs"
)

var AllActions = []Action{
	BlocksRead, EfFiles, HistoryNoSystemTxs,
}
