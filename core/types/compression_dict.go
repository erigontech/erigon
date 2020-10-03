package types

import "github.com/valyala/gozstd"

var KeyCompressionDictDBReceipts = []byte("db_receipts")

// CompressionDicts - dictionaries for compression and decompression
type CompressionDicts struct {
	CReceipts *gozstd.CDict
	DReceipts *gozstd.DDict
}
