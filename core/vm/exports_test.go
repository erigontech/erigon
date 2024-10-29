package vm

func MemoryGasCost(mem *Memory, newMemSize uint64) (uint64, error) {
	return memoryGasCost(mem, newMemSize)
}
