package snapshot_format

// TODO: Make this actually usable.
// func EncodeAttestationsForStorage(attestations *solid.ListSSZ[*solid.Attestation], buf []byte) []byte {
// 	if attestations.Len() == 0 {
// 		return nil
// 	}
// 	encoded := buf

// 	referencedAttestations := []solid.AttestationData{
// 		nil, // Full diff
// 	}
// 	// Pre-allocate some memory.
// 	attestations.Range(func(_ int, attestation *solid.Attestation, _ int) bool {
// 		data := attestation.AttestantionData()
// 		sig := attestation.Signature()
// 		// Encode attestation metadata
// 		// Also we need to keep track of aggregation bits size manually.
// 		encoded = append(encoded, byte(len(attestation.AggregationBits())))
// 		encoded = append(encoded, attestation.AggregationBits()...)
// 		// Encode signature
// 		encoded = append(encoded, sig[:]...)
// 		// Encode attestation body
// 		var bestEncoding []byte
// 		bestEncodingIndex := 0
// 		// try all non-repeating attestations.
// 		for i, att := range referencedAttestations {
// 			currentEncoding := encodeAttestationDataForStorage(attestation.AttestantionData(), att)
// 			// check if we find a better fit.
// 			if len(bestEncoding) == 0 || len(bestEncoding) > len(currentEncoding) {
// 				bestEncodingIndex = i
// 				bestEncoding = currentEncoding
// 				// cannot get lower than 1, so accept it as best.
// 				if len(bestEncoding) == 1 {
// 					break
// 				}
// 			}
// 		}
// 		// If it is not repeated then save it.
// 		if len(bestEncoding) != 1 {
// 			referencedAttestations = append(referencedAttestations, data)
// 		}
// 		encoded = append(encoded, byte(bestEncodingIndex))
// 		encoded = append(encoded, bestEncoding...)
// 		// Encode attester index
// 		encoded = append(encoded, data.RawValidatorIndex()...)
// 		return true
// 	})
// 	return encoded
// }

// // EncodeAttestationsDataForStorage encodes attestation data and compress everything by defaultData.
// func encodeAttestationDataForStorage(data solid.AttestationData, defaultData solid.AttestationData) []byte {
// 	fieldSet := byte(0)
// 	var ret []byte

// 	numBuffer := make([]byte, 4)

// 	// Encode in slot
// 	if defaultData == nil || data.Slot() != defaultData.Slot() {
// 		slotBytes := make([]byte, 4)
// 		binary.LittleEndian.PutUint32(slotBytes, uint32(data.Slot()))
// 		ret = append(ret, slotBytes...)
// 	} else {
// 		fieldSet = 1
// 	}

// 	if defaultData == nil || !bytes.Equal(data.RawBeaconBlockRoot(), defaultData.RawBeaconBlockRoot()) {
// 		root := data.BeaconBlockRoot()
// 		ret = append(ret, root[:]...)
// 	} else {
// 		fieldSet |= 2
// 	}

// 	if defaultData == nil || data.Source().Epoch() != defaultData.Source().Epoch() {
// 		binary.LittleEndian.PutUint32(numBuffer, uint32(data.Source().Epoch()))
// 		ret = append(ret, numBuffer...)
// 	} else {
// 		fieldSet |= 4
// 	}

// 	if defaultData == nil || !bytes.Equal(data.Source().RawBlockRoot(), defaultData.Source().RawBlockRoot()) {
// 		ret = append(ret, data.Source().RawBlockRoot()...)
// 	} else {
// 		fieldSet |= 8
// 	}

// 	if defaultData == nil || data.Target().Epoch() != defaultData.Target().Epoch() {
// 		binary.LittleEndian.PutUint32(numBuffer, uint32(data.Target().Epoch()))

// 		ret = append(ret, numBuffer...)
// 	} else {
// 		fieldSet |= 16
// 	}

// 	if defaultData == nil || !bytes.Equal(data.Target().RawBlockRoot(), defaultData.Target().RawBlockRoot()) {
// 		root := data.Target().BlockRoot()
// 		ret = append(ret, root[:]...)
// 	} else {
// 		fieldSet |= 32
// 	}
// 	return append([]byte{fieldSet}, ret...)
// }

// func DecodeAttestationsForStorage(buf []byte, out []byte) error {
// 	var signature libcommon.Bytes96

// 	if len(buf) == 0 {
// 		return nil
// 	}

// 	referencedAttestations := []solid.AttestationData{
// 		nil, // Full diff
// 	}
// 	// current position is how much we read.
// 	pos := 0
// 	for pos != len(buf) {
// 		attestationData := solid.NewAttestationData()
// 		// Decode aggregations bits
// 		aggrBitsLength := int(buf[pos])
// 		pos++
// 		aggrBits := buf[pos : pos+aggrBitsLength]
// 		pos += aggrBitsLength
// 		// Decode signature
// 		copy(signature[:], buf[pos:])
// 		pos += 96
// 		// decode attestation body
// 		// 1) read comparison index
// 		comparisonIndex := int(buf[pos])
// 		pos++
// 		n := decodeAttestationDataForStorage(buf[pos:], referencedAttestations[comparisonIndex], attestationData)
// 		// field set is not null, so we need to remember it.
// 		if n != 1 {
// 			referencedAttestations = append(referencedAttestations, attestationData)
// 		}
// 		pos += n
// 		// decode attester index
// 		attestationData.SetValidatorIndexWithRawBytes(buf[pos:])
// 		pos += 8
// 		attestations.Append(solid.NewAttestionFromParameters(aggrBits, attestationData, signature))
// 	}
// 	return nil
// }

// // DecodeAttestationDataForStorage decodes attestation data and decompress everything by defaultData.
// func decodeAttestationDataForStorage(buf []byte, defaultData solid.AttestationData, target solid.AttestationData) (n int) {
// 	if len(buf) == 0 {
// 		return
// 	}
// 	fieldSet := buf[0]
// 	n++
// 	if fieldSet&1 > 0 {
// 		target.SetSlotWithRawBytes(defaultData.RawSlot())
// 	} else {
// 		target.SetSlot(uint64(binary.LittleEndian.Uint32(buf[n:])))
// 		n += 4
// 	}

// 	if fieldSet&2 > 0 {
// 		target.SetBeaconBlockRootWithRawBytes(defaultData.RawBeaconBlockRoot())
// 	} else {
// 		target.SetBeaconBlockRootWithRawBytes(buf[n : n+32])
// 		n += 32
// 	}

// 	if fieldSet&4 > 0 {
// 		target.Source().SetRawEpoch(defaultData.Source().RawEpoch())
// 	} else {
// 		target.Source().SetEpoch(uint64(binary.LittleEndian.Uint32(buf[n:])))
// 		n += 4
// 	}

// 	if fieldSet&8 > 0 {
// 		target.Source().SetRawBlockRoot(defaultData.Source().RawBlockRoot())
// 	} else {
// 		target.Source().SetRawBlockRoot(buf[n : n+32])
// 		n += 32
// 	}

// 	if fieldSet&16 > 0 {
// 		target.Target().SetRawEpoch(defaultData.Target().RawEpoch())
// 	} else {
// 		target.Target().SetEpoch(uint64(binary.LittleEndian.Uint32(buf[n:])))
// 		n += 4
// 	}

// 	if fieldSet&32 > 0 {
// 		target.Target().SetRawBlockRoot(defaultData.Target().RawBlockRoot())
// 	} else {
// 		target.Target().SetRawBlockRoot(buf[n : n+32])
// 		n += 32
// 	}
// 	return
// }
