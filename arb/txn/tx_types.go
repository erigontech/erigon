package txn

// Transaction types.
const (
	ArbitrumDepositTxType         byte = 0x64
	ArbitrumUnsignedTxType        byte = 0x65
	ArbitrumContractTxType        byte = 0x66
	ArbitrumRetryTxType           byte = 0x68
	ArbitrumSubmitRetryableTxType byte = 0x69
	ArbitrumInternalTxType        byte = 0x6A
	ArbitrumLegacyTxType          byte = 0x78
)
