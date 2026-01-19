package jsonrpc

import (
	"context"
	"errors"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
)

type ArbAPIImpl struct {
	*APIImpl
}

func (api *ArbAPIImpl) Coinbase(ctx context.Context) (common.Address, error) {
	return common.Address{}, errors.New("the method eth_coinbase does not exist/is not available")
}

func (api *ArbAPIImpl) Mining(ctx context.Context) (bool, error) {
	return false, errors.New("the method eth_mining does not exist/is not available")
}

func (api *ArbAPIImpl) Hashrate(ctx context.Context) (uint64, error) {
	return 0, errors.New("the method eth_hashrate does not exist/is not available")
}

func (api *ArbAPIImpl) GetWork(ctx context.Context) ([4]string, error) {
	return [4]string{}, errors.New("the method eth_getWork does not exist/is not available")
}

func (api *ArbAPIImpl) SubmitWork(ctx context.Context, nonce types.BlockNonce, powHash, digest common.Hash) (bool, error) {
	return false, errors.New("the method eth_submitWork does not exist/is not available")
}

func (api *ArbAPIImpl) SubmitHashrate(ctx context.Context, hashRate hexutil.Uint64, id common.Hash) (bool, error) {
	return false, errors.New("the method eth_submitHashrate does not exist/is not available")
}

func (api *ArbAPIImpl) ProtocolVersion(_ context.Context) (hexutil.Uint, error) {
	return 0, errors.New("the method eth_protocolVersion does not exist/is not available")
}
