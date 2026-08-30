package payloadoptimizer

import (
	"errors"
	"reflect"

	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/types"
)

var ErrCustomTxnProvider = errors.New("payload optimizer build context cannot contain a custom transaction provider")

type BuildContext struct {
	params            *builder.Parameters
	forkVersion       [4]byte
	executionRequests types.FlatRequests
}

func NewBuildContext(params *builder.Parameters, forkVersion [4]byte, executionRequests types.FlatRequests) (BuildContext, error) {
	if params == nil {
		return BuildContext{}, errors.New("payload optimizer build context requires parameters")
	}
	if params.CustomTxnProvider != nil {
		return BuildContext{}, ErrCustomTxnProvider
	}
	owned := params.Copy()
	owned.PayloadId = 0
	return BuildContext{
		params:            owned,
		forkVersion:       forkVersion,
		executionRequests: copyRequests(executionRequests),
	}, nil
}

func (c BuildContext) Parameters() *builder.Parameters {
	return c.params.Copy()
}

func (c BuildContext) ForkVersion() [4]byte {
	return c.forkVersion
}

func (c BuildContext) ExecutionRequests() types.FlatRequests {
	return copyRequests(c.executionRequests)
}

func (c BuildContext) Equal(other BuildContext) bool {
	return c.forkVersion == other.forkVersion &&
		reflect.DeepEqual(c.params, other.params) &&
		reflect.DeepEqual(c.executionRequests, other.executionRequests)
}

func (c BuildContext) clone() BuildContext {
	return BuildContext{
		params:            c.Parameters(),
		forkVersion:       c.forkVersion,
		executionRequests: copyRequests(c.executionRequests),
	}
}

func copyRequests(requests types.FlatRequests) types.FlatRequests {
	if requests == nil {
		return nil
	}
	owned := make(types.FlatRequests, len(requests))
	for i := range requests {
		owned[i].Type = requests[i].Type
		owned[i].RequestData = append([]byte(nil), requests[i].RequestData...)
	}
	return owned
}
