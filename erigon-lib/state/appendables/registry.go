package appendables

import (
	"fmt"

	"github.com/erigontech/erigon-lib/downloader/snaptype"
)

// registry for appendable enums

var registeredAppendables = map[ApEnum]Appendable{}

func RegisterAppendable(aenum ApEnum, a Appendable) {
	registeredAppendables[aenum] = a
}

func GetRoSnapshot(aenum ApEnum) *RoSnapshots {
	return registeredAppendables[aenum].GetRoSnapshots()
}

func AppeFileName(baseName string, version snaptype.Version, stepFrom, stepTo uint64) string {
	return fmt.Sprintf("v%d-%06d-%06d-%s", version, stepFrom, stepTo)
}

func AppeSegName(aenum ApEnum, version snaptype.Version, stepFrom, stepTo uint64) string {
	return AppeFileName(string(aenum), version, stepFrom, stepTo) + ".seg"
}

func AppeIdxName(version snaptype.Version, stepFrom, stepTo uint64, idxName string) string {
	return AppeFileName(idxName, version, stepFrom, stepTo) + ".idx"
}
