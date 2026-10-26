// Copyright 2026 The Erigon Authors
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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPBinLeafProgressPublicationDoesNotRegress(t *testing.T) {
	publisher := &pbinLeafProgressPublisher{allocated: make([]uint64, 2)}
	publisher.publish(0, 128, 1)
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		publisher.publishWithHook(1, 0, 1, func() {
			close(started)
			<-release
		})
		close(done)
	}()
	<-started
	require.Equal(t, uint64(128), publisher.published.Load(), "progress publication must not expose a stale zero aggregate")
	close(release)
	<-done
	require.Equal(t, uint64(128), publisher.published.Load(), "an empty worker must not regress published allocation")
}
