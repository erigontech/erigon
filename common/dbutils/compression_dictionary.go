package dbutils

import (
	"github.com/AskAlexSharov/dictionaries"
	"github.com/valyala/gozstd"
)

// CompressionDictionaries - dictionaries for compression and decompression
type CompressionDictionaries struct {
	CReceipts *gozstd.CDict
	DReceipts *gozstd.DDict
}

// It's source of truth about compressionLevel - but to change it - you must migrate data stored in DB
var CompressionDicts = &CompressionDictionaries{
	CReceipts: mustCDict(dictionaries.Fast.Receipts, -2),
	DReceipts: mustDDict(dictionaries.Fast.Receipts),
}

func mustCDict(dict []byte, compressionLevel int) *gozstd.CDict {
	r, err := gozstd.NewCDictLevel(dict, compressionLevel)
	if err != nil {
		panic(err)
	}
	return r
}

func mustDDict(dict []byte) *gozstd.DDict {
	r, err := gozstd.NewDDict(dict)
	if err != nil {
		panic(err)
	}
	return r
}

// Release releases resources occupied by cd.
// cannot be used after the release.
func (d *CompressionDictionaries) Release() {
	d.CReceipts.Release()
	d.DReceipts.Release()
}
