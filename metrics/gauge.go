package metrics

// Gauges hold an int64 value that can be set arbitrarily.
type Gauge interface {
	Snapshot() Gauge
	Update(int64)
	Dec(int64)
	Inc(int64)
	Value() int64
}
