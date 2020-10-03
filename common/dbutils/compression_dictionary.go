package dbutils

import (
	"github.com/valyala/gozstd"
)

// CompressionDicts - dictionaries for compression and decompression
type CompressionDicts struct {
	CReceipts *gozstd.CDict
	DReceipts *gozstd.DDict
}

var CompressionDictionaries *CompressionDicts

//func init() {
//	var err error
//	CompressionDictionaries.CReceipts, err = gozstd.NewCDictLevel(ReceiptsDictionary, -2)
//	if err != nil {
//		panic(err)
//	}
//	CompressionDictionaries.DReceipts, err = gozstd.NewDDict(ReceiptsDictionary)
//	if err != nil {
//		panic(err)
//	}
//}
