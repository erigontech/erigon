package txn

type NoTimeBoosted bool

func (tx *NoTimeBoosted) IsTimeBoosted() bool {
	return false
}

func (tx *NoTimeBoosted) SetTimeboosted(bool) {

}
