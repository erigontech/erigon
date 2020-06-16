import struct

HashLength = 32
AddressLength = 20
BlockNumberLength = 8
IncarnationLength = 8


def bytesToUint64(val): return struct.unpack("<Q", val)[0]
def uint64ToBytes(val): return struct.pack("<Q", val)
