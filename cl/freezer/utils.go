package freezer

import (
	"bytes"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/types/ssz"
	"github.com/ledgerwatch/erigon/cl/utils"
)

type marshalerHashable interface {
	ssz.Marshaler
	ssz.HashableSSZ
}

func PutObjectSSZIntoFreezer(objectName, freezerNamespace string, numericalId uint64, object marshalerHashable, record Freezer) error {
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
	// put the hash of the object as the sidecar.
	h, err := object.HashSSZ()
	if err != nil {
		return err
	}

	return record.Put(&buffer, h[:], freezerNamespace, objectName, id)
}
