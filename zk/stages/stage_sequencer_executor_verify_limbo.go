package stages

type limboStreamBytesGroup struct {
	blockNumber                uint64
	transactionsIndicesInBlock []int
}

func newLimboStreamBytesGroup(blockNumber uint64) *limboStreamBytesGroup {
	return &limboStreamBytesGroup{
		blockNumber:                blockNumber,
		transactionsIndicesInBlock: make([]int, 0, 1),
	}
}

type limboStreamBytesBuilderHelper struct {
	sendersToGroupMap map[string][]*limboStreamBytesGroup
}

func newLimboStreamBytesBuilderHelper() *limboStreamBytesBuilderHelper {
	return &limboStreamBytesBuilderHelper{
		sendersToGroupMap: make(map[string][]*limboStreamBytesGroup),
	}
}

func (_this *limboStreamBytesBuilderHelper) append(senderMapKey string, blockNumber uint64, transactionIndex int) ([]uint64, [][]int) {
	limboStreamBytesGroups := _this.add(senderMapKey, blockNumber, transactionIndex)

	size := len(limboStreamBytesGroups)
	resultBlocks := make([]uint64, size)
	resultTransactionsSet := make([][]int, size)

	for i := 0; i < size; i++ {
		group := limboStreamBytesGroups[i]
		resultBlocks[i] = group.blockNumber
		resultTransactionsSet[i] = group.transactionsIndicesInBlock
	}

	return resultBlocks, resultTransactionsSet
}

func (_this *limboStreamBytesBuilderHelper) add(senderMapKey string, blockNumber uint64, transactionIndex int) []*limboStreamBytesGroup {
	limboStreamBytesGroups, ok := _this.sendersToGroupMap[senderMapKey]
	if !ok {
		limboStreamBytesGroups = []*limboStreamBytesGroup{newLimboStreamBytesGroup(blockNumber)}
		_this.sendersToGroupMap[senderMapKey] = limboStreamBytesGroups
	}
	group := limboStreamBytesGroups[len(limboStreamBytesGroups)-1]
	if group.blockNumber != blockNumber {
		group = newLimboStreamBytesGroup(blockNumber)
		limboStreamBytesGroups = append(limboStreamBytesGroups, group)
		_this.sendersToGroupMap[senderMapKey] = limboStreamBytesGroups
	}
	group.transactionsIndicesInBlock = append(group.transactionsIndicesInBlock, transactionIndex)

	return limboStreamBytesGroups
}
