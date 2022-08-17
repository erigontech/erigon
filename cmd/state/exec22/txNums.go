package exec22

type TxNums []uint64

func (s TxNums) MaxOf(blockNum uint64) (txnNum uint64) { return s[blockNum] }
func (s TxNums) MinOf(blockNum uint64) (txnNum uint64) {
	if blockNum == 0 {
		return 0
	}
	return s[blockNum-1] + 1
}
