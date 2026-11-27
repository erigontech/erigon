package state

import (
	"github.com/erigontech/erigon/db/version"
)

// common representation for any snapshot files
// seg, .v or indexes and existence filters, accessors.
type SnapInfo struct {
	Version  version.Version
	From, To uint64
	Name     string // filename
	FileType string
	// Path     string // full path
	Ext string // extension
}

// func (f *SnapInfo) IsSeg() bool      { return strings.Compare(f.Ext, ".seg") == 0 }
// func (f *SnapInfo) IsV() bool        { return strings.Compare(f.Ext, ".v") == 0 }
// func (f *SnapInfo) IsKV() bool       { return strings.Compare(f.Ext, ".kv") == 0 }
func (f *SnapInfo) IsDataFile() bool { return DataExtension(f.Ext).IsSet() }
