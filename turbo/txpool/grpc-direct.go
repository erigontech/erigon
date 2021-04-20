package txpool

import (
	"context"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/gointerfaces"
	proto_txpool "github.com/ledgerwatch/turbo-geth/gointerfaces/txpool"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"google.golang.org/grpc"
)

// ClientDirect implements TxpoolClient interface by connecting the instance of the client directly with the corresponding
// instance of SentryServer
type ClientDirect struct {
	server proto_txpool.TxpoolServer
}

func NewClientDirect(server proto_txpool.TxpoolServer) *ClientDirect {
	return &ClientDirect{server: server}
}

func (c *ClientDirect) FindUnknownTransactions(ctx context.Context, in *proto_txpool.TxHashes, opts ...grpc.CallOption) (*proto_txpool.TxHashes, error) {
	return c.server.FindUnknownTransactions(ctx, in)
}

func (c *ClientDirect) ImportTransactions(ctx context.Context, in *proto_txpool.ImportRequest, opts ...grpc.CallOption) (*proto_txpool.ImportReply, error) {
	return c.server.ImportTransactions(ctx, in)
}

func (c *ClientDirect) GetTransactions(ctx context.Context, in *proto_txpool.GetTransactionsRequest, opts ...grpc.CallOption) (*proto_txpool.GetTransactionsReply, error) {
	return c.server.GetTransactions(ctx, in)
}

func (c *ClientDirect) GetSerializedTransactions(ctx context.Context, hashes common.Hashes) ([]rlp.RawValue, error) {
	reply, err := c.GetTransactions(ctx, &proto_txpool.GetTransactionsRequest{Hashes: gointerfaces.ConvertHashesToH256(hashes)})
	if err != nil {
		return nil, err
	}
	result := make([]rlp.RawValue, len(reply.Txs))
	return result, err
}
