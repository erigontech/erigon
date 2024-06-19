package integrity

type Check string

const (
	Blocks             Check = "Blocks"
	InvertedIndex      Check = "InvertedIndex"
	HistoryNoSystemTxs Check = "HistoryNoSystemTxs"
)

var AllChecks = []Check{
	Blocks, InvertedIndex, HistoryNoSystemTxs,
}
