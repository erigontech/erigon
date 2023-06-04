package freezer

import (
	"bytes"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func PutObjectSSZIntoFreezer(objectName, freezerNamespace string, numericalId uint64, object ssz.Marshaler, record Freezer) error {
	if record == nil {
		return nil
	}
	var buffer bytes.Buffer
	encoded, err := object.EncodeSSZ(nil)
	if err != nil {
		return err
	}
	if _, err = buffer.Write(utils.CompressSnappy(encoded)); err != nil {
		return err
	}
	id := fmt.Sprintf("%d", numericalId)

	return record.Put(&buffer, nil, freezerNamespace, objectName, id)
}
