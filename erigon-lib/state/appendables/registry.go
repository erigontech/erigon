package appendables

import (
	"fmt"

	"github.com/erigontech/erigon-lib/downloader/snaptype"
)

type ApEnum string

var registry map[ApEnum]ApConfig

type ApConfig struct {
	snapFileId  string
	indexFileId []string
}

func AppeFileName(baseName string, version snaptype.Version, stepFrom, stepTo uint64) string {
	return fmt.Sprintf("v%d-%06d-%06d-%s", version, stepFrom, stepTo)
}

func AppeSegName(aenum ApEnum, version snaptype.Version, stepFrom, stepTo uint64) string {
	return AppeFileName(registry[aenum].snapFileId, version, stepFrom, stepTo) + ".seg"
}

func AppeIdxName(aenum ApEnum, version snaptype.Version, stepFrom, stepTo uint64, idxNum uint64) string {
	return AppeFileName(registry[aenum].indexFileId[idxNum], version, stepFrom, stepTo) + ".idx"
}
