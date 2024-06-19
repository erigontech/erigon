package integrity

type Check string

const (
	Blocks             Check = "Blocks"
	BlocksTxnID        Check = "BlocksTxnID"
	InvertedIndex      Check = "InvertedIndex"
	HistoryNoSystemTxs Check = "HistoryNoSystemTxs"
)

var AllChecks = []Check{
	Blocks, BlocksTxnID, InvertedIndex, HistoryNoSystemTxs,
}
