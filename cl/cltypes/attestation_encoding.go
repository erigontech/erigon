package cltypes

import (
	"encoding/binary"

	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/common"
)

const maxAttestationSize = 2276

func EncodeAttestationsForStorage(attestations []*Attestation) []byte {
	if len(attestations) == 0 {
		return nil
	}
	// Pre-allocate some memory.
	encoded := make([]byte, 0, maxAttestationSize*len(attestations)+1)

	// Take the first attestation datas and use it as the default value.
	defaultData := attestations[0].Data
	// encode default attestation
	encoded = append(encoded, EncodeAttestationDataForStorage(defaultData, nil)...)
	for _, attestation := range attestations {
		// Encode attestation metadata
		// Also we need to keep track of aggregation bits size manually.
		encoded = append(encoded, encodeNumber(uint64(len(attestation.AggregationBits)))...)
		encoded = append(encoded, attestation.AggregationBits...)
		encoded = append(encoded, attestation.Signature[:]...)
		// Encode attestation body
		encoded = append(encoded, EncodeAttestationDataForStorage(attestation.Data, defaultData)...)
	}
	return utils.CompressSnappy(encoded)
}

func DecodeAttestationsForStorage(buf []byte) ([]*Attestation, error) {
	if len(buf) == 0 {
		return nil, nil
	}
	buf, err := utils.DecompressSnappy(buf)
	if err != nil {
		return nil, err
	}
	var attestations []*Attestation
	// Take the first attestation datas and use it as the default value.
	n, defaultData := DecodeAttestationDataForStorage(buf, nil)
	// current position is how much we read.
	pos := n
	for pos != len(buf) {
		// Figure out how long are aggragation bits
		bitsLength := decodeNumber(buf[pos:])
		pos += 4
		// Decode aggrefation bits
		attestation := &Attestation{
			AggregationBits: common.CopyBytes(buf[pos : pos+int(bitsLength)]),
		}
		pos += int(bitsLength)
		// Decode signature
		copy(attestation.Signature[:], buf[pos:])
		pos += 96
		// Encode attestation body
		n, attestation.Data = DecodeAttestationDataForStorage(buf[pos:], defaultData)
		pos += n
		attestations = append(attestations, attestation)
	}
	return attestations, nil
}

func encodeNumber(x uint64) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(x))
	return b
}

func decodeNumber(b []byte) uint64 {
	return uint64(binary.BigEndian.Uint32(b[:4]))
}

// EncodeAttestationsDataForStorage encodes attestation data and compress everything by defaultData.
func EncodeAttestationDataForStorage(data *AttestationData, defaultData *AttestationData) []byte {
	fieldSet := byte(0)
	var ret []byte
	// Encode in slot
	if defaultData == nil || data.Slot != defaultData.Slot {
		ret = append(ret, encodeNumber(data.Slot)...)
	} else {
		fieldSet = 1
	}

	if defaultData == nil || data.Index != defaultData.Index {
		ret = append(ret, encodeNumber(data.Index)...)
	} else {
		fieldSet |= 2
	}

	if defaultData == nil || data.BeaconBlockHash != defaultData.BeaconBlockHash {
		ret = append(ret, data.BeaconBlockHash[:]...)
	} else {
		fieldSet |= 4
	}

	if defaultData == nil || data.Source.Epoch != defaultData.Source.Epoch {
		ret = append(ret, encodeNumber(data.Source.Epoch)...)
	} else {
		fieldSet |= 8
	}

	if defaultData == nil || data.Source.Root != defaultData.Source.Root {
		ret = append(ret, data.Source.Root[:]...)
	} else {
		fieldSet |= 16
	}

	if defaultData == nil || data.Target.Epoch != defaultData.Target.Epoch {
		ret = append(ret, encodeNumber(data.Target.Epoch)...)
	} else {
		fieldSet |= 32
	}

	if defaultData == nil || data.Target.Root != defaultData.Target.Root {
		ret = append(ret, data.Target.Root[:]...)
	} else {
		fieldSet |= 64
	}
	return append([]byte{fieldSet}, ret...)
}

// DecodeAttestationDataForStorage decodes attestation data and decompress everything by defaultData.
func DecodeAttestationDataForStorage(buf []byte, defaultData *AttestationData) (n int, data *AttestationData) {
	data = &AttestationData{
		Target: &Checkpoint{},
		Source: &Checkpoint{},
	}
	if len(buf) == 0 {
		return
	}
	fieldSet := buf[0]
	n++
	if fieldSet&1 > 0 {
		data.Slot = defaultData.Slot
	} else {
		data.Slot = decodeNumber(buf[n:])
		n += 4
	}

	if fieldSet&2 > 0 {
		data.Index = defaultData.Index
	} else {
		data.Index = decodeNumber(buf[n:])
		n += 4
	}

	if fieldSet&4 > 0 {
		data.BeaconBlockHash = defaultData.BeaconBlockHash
	} else {
		data.BeaconBlockHash = common.BytesToHash(buf[n : n+32])
		n += 32
	}

	if fieldSet&8 > 0 {
		data.Source.Epoch = defaultData.Source.Epoch
	} else {
		data.Source.Epoch = decodeNumber(buf[n:])
		n += 4
	}

	if fieldSet&16 > 0 {
		data.Source.Root = defaultData.Source.Root
	} else {
		data.Source.Root = common.BytesToHash(buf[n : n+32])
		n += 32
	}

	if fieldSet&32 > 0 {
		data.Target.Epoch = defaultData.Target.Epoch
	} else {
		data.Target.Epoch = decodeNumber(buf[n:])
		n += 4
	}

	if fieldSet&64 > 0 {
		data.Target.Root = defaultData.Target.Root
	} else {
		data.Target.Root = common.BytesToHash(buf[n : n+32])
		n += 32
	}
	return
}
