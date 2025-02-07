package state

import (
	"fmt"

	"github.com/erigontech/erigon-lib/downloader/snaptype"
)

// TODO: snaptype.Version should be replaced??
func fileName(baseName string, version snaptype.Version, from, to RootNum) string {
	return fmt.Sprintf("v%d-%06d-%06d-%s", version, from, to, baseName)
}

func SegName(id AppendableId, version snaptype.Version, from, to RootNum) string {
	return fileName(id.Name(), version, from, to) + ".seg"
}

func IdxName(id AppendableId, version snaptype.Version, from, to RootNum, idxNum uint64) string {
	return fileName(id.IndexPrefix()[idxNum], version, from, to) + ".idx"
}
