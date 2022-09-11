package engineapi

import (
	"fmt"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/stretchr/testify/require"
)

func ValidatePayloadErr(kv.RwTx, *types.Header, *types.RawBody, uint64, []*types.Header, []*types.RawBody) error {
	return fmt.Errorf("you have an error")
}

func ValidatePayloadNil(kv.RwTx, *types.Header, *types.RawBody, uint64, []*types.Header, []*types.RawBody) error {
	return nil
}

func TestValidatePayLoad(t *testing.T) {
	_, tx := memdb.NewTestTx(t)

	k := NewForkValidator(0, ValidatePayloadErr)
	blockEnc := common.FromHex("f90260f901f9a083cafc574e1f51ba9dc0568fc617a08ea2429fb384059c972f13b19fa1c8dd55a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347948888f1f195afa192cfee860698584c030f4c9db1a0ef1552a40b7165c3cd773806b9e0c165b75356e0314bf0706f279c729f51e017a05fe50b260da6308036625b850b5d6ced6d0a9f814c0688bc91ffb7b7a3a54b67a0bc37d79753ad738a6dac4921e57392f145d8887476de3f783dfa7edae9283e52b90100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008302000001832fefd8825208845506eb0780a0bd4472abb6659ebe3ee06ee4d7b72a00a9f4d001caca51342001075469aff49888a13a5a8c8f2bb1c4f861f85f800a82c35094095e7baea6a6c7c4c2dfeb977efac326af552d870a801ba09bea4c4daac7c7c52e093e6a4c35dbbcf8856f1af7b059ba20253e70848d094fa08a8fae537ce25ed8cb5af9adac3f141af69bd515bd2ba031522df09b97dd72b1c0")
	var block types.Block
	if err := rlp.DecodeBytes(blockEnc, &block); err != nil {
		t.Fatal("decode error: ", err)
	}

	_, _, err, _ := k.ValidatePayload(tx, block.Header(), block.RawBody(), true)
	require.Error(t, err)
	k = NewForkValidatorMock(0, ValidatePayloadNil)
	_, _, err, _ = k.ValidatePayload(tx, block.Header(), block.RawBody(), true)
	require.Nil(t, err)
}

func TestValidationPayLoadSideForkError(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	k := NewForkValidator(0, ValidatePayloadErr)

	block0 := types.NewBlock(&types.Header{
		Number: common.Big0,
	}, nil, nil, nil)
	block := types.NewBlock(&types.Header{
		Number:     common.Big1,
		ParentHash: block0.Hash(),
	}, nil, nil, nil)
	//hash := block.Hash()
	k.TryAddingPoWBlock(block0)
	require.NoError(t, rawdb.WriteHeaderNumber(tx, block.ParentHash(), 0))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, block.ParentHash(), 0))

	_, _, err, _ := k.ValidatePayload(tx, block.Header(), block.RawBody(), false)
	require.Error(t, err)
}

func TestValidationPayLoadSideFork(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	k := NewForkValidator(0, ValidatePayloadNil)

	block0 := types.NewBlock(&types.Header{
		Number: common.Big0,
	}, nil, nil, nil)
	block := types.NewBlock(&types.Header{
		Number:     common.Big1,
		ParentHash: block0.Hash(),
	}, nil, nil, nil)
	//hash := block.Hash()
	k.TryAddingPoWBlock(block0)
	require.NoError(t, rawdb.WriteHeaderNumber(tx, block.ParentHash(), 0))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, block.ParentHash(), 0))

	_, _, err, _ := k.ValidatePayload(tx, block.Header(), block.RawBody(), false)
	require.NoError(t, err)
}

func TestTryAddingPoWBlock(t *testing.T) {
	k := NewForkValidatorMock(0, nil)

	blockEnc := common.FromHex("f90260f901f9a083cafc574e1f51ba9dc0568fc617a08ea2429fb384059c972f13b19fa1c8dd55a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347948888f1f195afa192cfee860698584c030f4c9db1a0ef1552a40b7165c3cd773806b9e0c165b75356e0314bf0706f279c729f51e017a05fe50b260da6308036625b850b5d6ced6d0a9f814c0688bc91ffb7b7a3a54b67a0bc37d79753ad738a6dac4921e57392f145d8887476de3f783dfa7edae9283e52b90100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008302000001832fefd8825208845506eb0780a0bd4472abb6659ebe3ee06ee4d7b72a00a9f4d001caca51342001075469aff49888a13a5a8c8f2bb1c4f861f85f800a82c35094095e7baea6a6c7c4c2dfeb977efac326af552d870a801ba09bea4c4daac7c7c52e093e6a4c35dbbcf8856f1af7b059ba20253e70848d094fa08a8fae537ce25ed8cb5af9adac3f141af69bd515bd2ba031522df09b97dd72b1c0")
	var block types.Block
	if err := rlp.DecodeBytes(blockEnc, &block); err != nil {
		t.Fatal("decode error: ", err)
	}
	val := block.Hash()
	k.TryAddingPoWBlock(&block)
	re := forkSegment{
		header: block.Header(),
		body:   block.RawBody(),
	}
	require.Equal(t, k.sideForksBlock[val].header.Hash(), re.header.Hash())
}
