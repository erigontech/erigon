package chainparams

const (
	BlobTxBlobGasPerBlob             = 1 << 17 // Gas consumption of a single data blob (== blob byte size)
	BlobTxMinBlobGasprice            = 1       // Minimum gas price for a blob transaction
	BlobTxBlobGaspriceUpdateFraction = 3338477 // Controls the maximum rate of change for blob gas price
	BlobTxFieldElementsPerBlob       = 4096    // Number of field elements stored in a single data blob

	BlobTxTargetBlobGasPerBlock = 3 * BlobTxBlobGasPerBlob // Target consumable blob gas for data blobs per block (for 1559-like pricing)
	MaxBlobGasPerBlock          = 6 * BlobTxBlobGasPerBlob // Maximum consumable blob gas for data blobs per block

	SloadGas = uint64(50) // Multiplied by the number of 32-byte words that are copied (round up) for any *COPY operation and added.

)
