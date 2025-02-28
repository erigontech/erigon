// Copyright 2025 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package testhelpers

import (
	"testing"

	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/txnprovider/shutter"
)

type MockEonTrackerCreatorOpt func(mock *MockEonTracker)

func MockEonTrackerCreator(opts ...MockEonTrackerCreatorOpt) func(t *testing.T) *MockEonTracker {
	return func(t *testing.T) *MockEonTracker {
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
	return func(et *MockEonTracker) {
		i := -1
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
	return func(et *MockEonTracker) {
		i := -1
		et.EXPECT().
			RecentEon(gomock.Any()).
			DoAndReturn(func(index shutter.EonIndex) (shutter.Eon, bool) {
				i++
				return results[i].Eon, results[i].Ok
			}).
			Times(len(results))
	}
}
