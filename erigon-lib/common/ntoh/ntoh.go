package ntoh

func U64(buffer []byte, index int) uint64 {
	return (uint64(buffer[index]) << 56) |
		(uint64(buffer[index+1]) << 48) |
		(uint64(buffer[index+2]) << 40) |
		uint64(buffer[index+3])<<32 |
		(uint64(buffer[index+4]) << 24) |
		(uint64(buffer[index+5]) << 16) |
		(uint64(buffer[index+6]) << 8) |
		uint64(buffer[index+7])
}

func U32(buffer []byte, index int) uint32 {
	return (uint32(buffer[index]) << 24) |
		(uint32(buffer[index+1]) << 16) |
		(uint32(buffer[index+2]) << 8) |
		uint32(buffer[index+3])
}

func U16(buffer []byte, index int) uint16 {
	return uint16(buffer[index])<<8 |
		uint16(buffer[index+1])
}

func U8(buffer []byte, index int) uint8 {
	return buffer[index]
}

func UInt(buffer []byte, index int, len int) uint64 {
	switch len {
	case 0:
		return uint64(0)
	case 1:
		return uint64(buffer[index+0])
	case 2:
		return uint64(buffer[index+1]) | uint64(buffer[index+0])<<8
	case 3:
		return uint64(buffer[index+2]) | uint64(buffer[index+1])<<8 | uint64(buffer[index+0])<<16
	case 4:
		return uint64(buffer[index+3]) | uint64(buffer[index+2])<<8 | uint64(buffer[index+1])<<16 |
			uint64(buffer[index+0])<<24
	case 5:
		return uint64(buffer[index+4]) | uint64(buffer[index+3])<<8 | uint64(buffer[index+2])<<16 |
			uint64(buffer[index+1])<<24 | uint64(buffer[index+0])<<32
	case 6:
		return uint64(buffer[index+5]) | uint64(buffer[index+4])<<8 | uint64(buffer[index+3])<<16 |
			uint64(buffer[index+2])<<24 | uint64(buffer[index+1])<<32 | uint64(buffer[index+0])<<40
	case 7:
		return uint64(buffer[index+6]) | uint64(buffer[index+5])<<8 | uint64(buffer[index+4])<<16 |
			uint64(buffer[index+3])<<24 | uint64(buffer[index+2])<<32 | uint64(buffer[index+1])<<40 |
			uint64(buffer[index+0])<<48
	default:
		return uint64(buffer[index+7]) | uint64(buffer[index+6])<<8 | uint64(buffer[index+5])<<16 |
			uint64(buffer[index+4])<<24 | uint64(buffer[index+3])<<32 | uint64(buffer[index+2])<<40 |
			uint64(buffer[index+1])<<48 | uint64(buffer[index+0])<<56
	}
}
