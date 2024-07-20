// Copyright 2024 The Erigon Authors
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

package remoteproto_test

import (
	"slices"
	"testing"

	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	types "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/stretchr/testify/assert"
)

func TestSort(t *testing.T) {
	tests := []struct {
		name string
		got  *remote.NodesInfoReply
		want *remote.NodesInfoReply
	}{
		{
			name: "sort by name",
			got: &remote.NodesInfoReply{
				NodesInfo: []*types.NodeInfoReply{
					{Name: "b", Enode: "c"},
					{Name: "a", Enode: "d"},
				},
			},
			want: &remote.NodesInfoReply{
				NodesInfo: []*types.NodeInfoReply{
					{Name: "a", Enode: "d"},
					{Name: "b", Enode: "c"},
				},
			},
		},
		{
			name: "sort by enode",
			got: &remote.NodesInfoReply{
				NodesInfo: []*types.NodeInfoReply{
					{Name: "a", Enode: "d"},
					{Name: "a", Enode: "c"},
				},
			},
			want: &remote.NodesInfoReply{
				NodesInfo: []*types.NodeInfoReply{
					{Name: "a", Enode: "c"},
					{Name: "a", Enode: "d"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			slices.SortFunc(tt.got.NodesInfo, remote.NodeInfoReplyCmp)
			assert.Equal(t, tt.want, tt.got)
		})
	}
}
