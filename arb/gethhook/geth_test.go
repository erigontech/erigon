// Copyright 2021-2022, Offchain Labs, Inc.
// For license information, see https://github.com/nitro/blob/master/LICENSE

package gethhook

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/types"

	"github.com/erigontech/nitro-erigon/arbos"
	"github.com/erigontech/nitro-erigon/arbos/arbosState"
	"github.com/erigontech/nitro-erigon/arbos/arbostypes"
	"github.com/erigontech/nitro-erigon/arbos/util"
	"github.com/erigontech/nitro-erigon/util/testhelpers"

	arbparapms "github.com/erigontech/erigon/arb/chain/params"
)

type TestChainContext struct {
}

func (r *TestChainContext) Engine() consensus.Engine {
	return arbos.Engine{}
}

func (r *TestChainContext) GetHeader(hash common.Hash, num uint64) *types.Header {
	return &types.Header{}
}

var testChainConfig = &chain.Config{
	ChainID:               big.NewInt(0),
	HomesteadBlock:        big.NewInt(0),
	DAOForkBlock:          nil,
	TangerineWhistleBlock: big.NewInt(0),
	SpuriousDragonBlock:   big.NewInt(0),

	ByzantiumBlock:      big.NewInt(0),
	ConstantinopleBlock: big.NewInt(0),
	PetersburgBlock:     big.NewInt(0),
	IstanbulBlock:       big.NewInt(0),
	MuirGlacierBlock:    big.NewInt(0),
	BerlinBlock:         big.NewInt(0),
	LondonBlock:         big.NewInt(0),
	ArbitrumChainParams: arbparapms.ArbitrumDevTestParams(),
}

func TestEthDepositMessage(t *testing.T) {

	_, statedb, e3 := arbosState.NewArbosMemoryBackedArbOSState()

	defer e3.Close()
	addr := common.HexToAddress("0x32abcdeffffff")
	balance := common.BigToHash(big.NewInt(789789897789798))
	balance2 := common.BigToHash(big.NewInt(98))

	addrBal, err := statedb.GetBalance(addr)
	if err != nil {
		Fail(t)
	}

	if addrBal.ToBig().Sign() != 0 {
		Fail(t)
	}

	firstRequestId := common.BigToHash(big.NewInt(3))
	header := arbostypes.L1IncomingMessageHeader{
		Kind:        arbostypes.L1MessageType_EthDeposit,
		Poster:      addr,
		BlockNumber: 864513,
		Timestamp:   8794561564,
		RequestId:   &firstRequestId,
		L1BaseFee:   big.NewInt(10000000000000),
	}
	msgBuf := bytes.Buffer{}
	if err := util.AddressToWriter(addr, &msgBuf); err != nil {
		t.Error(err)
	}
	if err := util.HashToWriter(balance, &msgBuf); err != nil {
		t.Error(err)
	}
	msg := arbostypes.L1IncomingMessage{
		Header: &header,
		L2msg:  msgBuf.Bytes(),
	}

	serialized, err := msg.Serialize()
	if err != nil {
		t.Error(err)
	}

	secondRequestId := common.BigToHash(big.NewInt(4))
	header.RequestId = &secondRequestId
	header.Poster = util.RemapL1Address(addr)
	msgBuf2 := bytes.Buffer{}
	if err := util.AddressToWriter(addr, &msgBuf2); err != nil {
		t.Error(err)
	}
	if err := util.HashToWriter(balance2, &msgBuf2); err != nil {
		t.Error(err)
	}
	msg2 := arbostypes.L1IncomingMessage{
		Header: &header,
		L2msg:  msgBuf2.Bytes(),
	}
	serialized2, err := msg2.Serialize()
	if err != nil {
		t.Error(err)
	}

	RunMessagesThroughAPI(t, [][]byte{serialized, serialized2}, statedb)

	balanceAfter, err := statedb.GetBalance(addr)
	if err != nil {
		t.Error(err)
	}

	sum := new(big.Int).Add(balance.Big(), balance2.Big())
	if balanceAfter.Cmp(uint256.MustFromBig(sum)) != 0 {
		Fail(t)
	}
}

func RunMessagesThroughAPI(t *testing.T, msgs [][]byte, statedb *state.IntraBlockState) {
	chainId := big.NewInt(6456554)
	for _, data := range msgs {
		msg, err := arbostypes.ParseIncomingL1Message(bytes.NewReader(data), nil)
		if err != nil {
			t.Error(err)
		}
		txes, err := arbos.ParseL2Transactions(msg, chainId)
		if err != nil {
			t.Error(err)
		}
		header := &types.Header{
			Number:     big.NewInt(1000),
			Difficulty: big.NewInt(1000),
		}
		gasPool := new(core.GasPool).AddGas(100000) // TODO add blob gas
		blockHashfn := func(n uint64) (common.Hash, error) {
			return common.Hash{}, nil
		}
		var engine consensus.EngineReader // TODO
		var stateWriter state.StateWriter // TODO

		for _, tx := range txes {
			_, _, err := core.ApplyTransaction(testChainConfig, blockHashfn, engine, nil, gasPool, statedb, stateWriter, header, tx, &header.GasUsed, header.BlobGasUsed, vm.Config{})
			if err != nil {
				Fail(t, err)
			}
		}

		arbos.FinalizeBlock(nil, nil, statedb, testChainConfig)
	}
}

func Require(t *testing.T, err error, printables ...interface{}) {
	t.Helper()
	testhelpers.RequireImpl(t, err, printables...)
}

func Fail(t *testing.T, printables ...interface{}) {
	t.Helper()
	testhelpers.FailImpl(t, printables...)
}
