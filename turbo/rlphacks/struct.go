package rlphacks

func GenerateStructLen(buffer []byte, l int) int {
	if l < 56 {
		buffer[0] = byte(192 + l)
		return 1
	}
	if l < 256 {
		// l can be encoded as 1 byte
		buffer[1] = byte(l)
		buffer[0] = byte(247 + 1)
		return 2
	}
	if l < 65536 {
		buffer[2] = byte(l & 255)
		buffer[1] = byte(l >> 8)
		buffer[0] = byte(247 + 2)
		return 3
	}
	buffer[3] = byte(l & 255)
	buffer[2] = byte((l >> 8) & 255)
	buffer[1] = byte(l >> 16)
	buffer[0] = byte(247 + 3)
	return 4
}
