package crypto

const VersionIdentifier byte = 0x03

// IdentifyVersion reads the version identifier byte from the given (marshaled) EncryptedMessage.
func IdentifyVersion(d []byte) byte {
	if len(d)%BlockSize == 0 {
		return 0x00
	}
	return d[0]
}
