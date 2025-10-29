package arb

type NoTimeBoosted bool

func (tx *NoTimeBoosted) IsTimeBoosted() *bool {
	return nil
}

func (tx *NoTimeBoosted) SetTimeboosted(*bool) {

}
