package accounts

import "fmt"

func DecodeIncarnationFromStorage(enc []byte) (uint64, error) {
	if len(enc) == 0 {
		return 0, nil
	}

	var fieldSet = enc[0]
	var pos = 1

	//looks for the position incarnation is at
	if fieldSet&1 > 0 {
		decodeLength := int(enc[pos])
		if len(enc) < pos+decodeLength+1 {
			return 0, fmt.Errorf(
				"malformed CBOR for Account.Nonce: %s, Length %d",
				enc[pos+1:], decodeLength)
		}
		pos += decodeLength + 1
	}

	if fieldSet&2 > 0 {
		decodeLength := int(enc[pos])
		if len(enc) < pos+decodeLength+1 {
			return 0, fmt.Errorf(
				"malformed CBOR for Account.Nonce: %s, Length %d",
				enc[pos+1:], decodeLength)
		}
		pos += decodeLength + 1
	}

	if fieldSet&4 > 0 {
		decodeLength := int(enc[pos])

		//checks if the ending position is correct if not returns 0
		if len(enc) < pos+decodeLength+1 {
			return 0, fmt.Errorf(
				"malformed CBOR for Account.Incarnation: %s, Length %d",
				enc[pos+1:], decodeLength)
		}

		incarnation := bytesToUint64(enc[pos+1 : pos+decodeLength+1])
		return incarnation, nil
	}

	return 0, nil

}

func bytesToUint64(buf []byte) (x uint64) {
	for i, b := range buf {
		x = x<<8 + uint64(b)
		if i == 7 {
			return
		}
	}
	return
}
