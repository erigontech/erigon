package beacontest

import "errors"

var (
	ErrExpressionMustReturnBool = errors.New("cel expression must return bool")
	ErrUnknownType              = errors.New("unknown type")
)
