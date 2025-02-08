package testhelpers

import (
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/txnprovider/shutter"
)

type MockEonTrackerCreatorOpt func(mock *MockEonTracker)

func MockEonTrackerCreator(opts ...MockEonTrackerCreatorOpt) func(t *testing.T) shutter.EonTracker {
	return func(t *testing.T) shutter.EonTracker {
		ctrl := gomock.NewController(t)
		et := NewMockEonTracker(ctrl)
		for _, opt := range opts {
			opt(et)
		}

		return et
	}
}

type CurrentEonMockResult struct {
	Eon shutter.Eon
	Ok  bool
}

func WithCurrentEonMockResult(results ...CurrentEonMockResult) MockEonTrackerCreatorOpt {
	i := -1
	return func(et *MockEonTracker) {
		et.EXPECT().
			CurrentEon().
			DoAndReturn(func() (shutter.Eon, bool) {
				i++
				return results[i].Eon, results[i].Ok
			}).
			Times(len(results))
	}
}

type RecentEonMockResult struct {
	Eon shutter.Eon
	Ok  bool
}

func WithRecentEonMockResult(results ...RecentEonMockResult) MockEonTrackerCreatorOpt {
	i := -1
	return func(et *MockEonTracker) {
		et.EXPECT().
			RecentEon(gomock.Any()).
			DoAndReturn(func(index shutter.EonIndex) (shutter.Eon, bool) {
				i++
				return results[i].Eon, results[i].Ok
			}).
			Times(len(results))
	}
}
