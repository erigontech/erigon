package heimdall

type Entity interface {
	BlockNumRange() ClosedRange
}
