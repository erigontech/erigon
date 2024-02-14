package metrics

type ValueGetter interface {
	GetValue() float64
	GetValueUint64() uint64
}
