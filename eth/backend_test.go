package eth

import (
	"context"
	"testing"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestNodesInfo_Deduplication(t *testing.T) {
	tests := []struct {
		name  string
		limit int
		nodes []*types.NodeInfoReply
		want  []*types.NodeInfoReply
	}{
		{
			name:  "one node",
			nodes: []*types.NodeInfoReply{{Name: "name", Enode: "enode"}},
			want:  []*types.NodeInfoReply{{Name: "name", Enode: "enode"}},
		},
		{
			name: "two different nodes",
			nodes: []*types.NodeInfoReply{
				{Name: "name1", Enode: "enode1"},
				{Name: "name", Enode: "enode"},
			},
			want: []*types.NodeInfoReply{
				{Name: "name", Enode: "enode"},
				{Name: "name1", Enode: "enode1"},
			},
		},
		{
			name: "two same nodes",
			nodes: []*types.NodeInfoReply{
				{Name: "name", Enode: "enode"},
				{Name: "name", Enode: "enode"},
			},
			want: []*types.NodeInfoReply{
				{Name: "name", Enode: "enode"},
			},
		},
		{
			name: "three nodes",
			nodes: []*types.NodeInfoReply{
				{Name: "name2", Enode: "enode2"},
				{Name: "name", Enode: "enode"},
				{Name: "name1", Enode: "enode1"},
			},
			want: []*types.NodeInfoReply{
				{Name: "name", Enode: "enode"},
				{Name: "name1", Enode: "enode1"},
				{Name: "name2", Enode: "enode2"},
			},
		},
		{
			name: "three nodes with repeats",
			nodes: []*types.NodeInfoReply{
				{Name: "name", Enode: "enode"},
				{Name: "name1", Enode: "enode1"},
				{Name: "name", Enode: "enode"},
			},
			want: []*types.NodeInfoReply{
				{Name: "name", Enode: "enode"},
				{Name: "name1", Enode: "enode1"},
			},
		},
		{
			name: "three same nodes",
			nodes: []*types.NodeInfoReply{
				{Name: "name", Enode: "enode"},
				{Name: "name", Enode: "enode"},
				{Name: "name", Enode: "enode"},
			},
			want: []*types.NodeInfoReply{
				{Name: "name", Enode: "enode"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eth := Ethereum{}

			for _, n := range tt.nodes {
				n := n
				eth.sentries = append(eth.sentries,
					direct.NewSentryClientRemote(&sentry.SentryClientMock{
						NodeInfoFunc: func(context.Context, *emptypb.Empty, ...grpc.CallOption) (*types.NodeInfoReply, error) {
							return n, nil
						},
					}),
				)
			}

			got, err := eth.NodesInfo(tt.limit)
			if err != nil {
				t.Error(err)
			}

			assert.Equal(t, tt.want, got.NodesInfo)
		})
	}
}
