package vm

import (
	"context"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"math/big"
	"testing"
)

func _TestVersionAdd(t *testing.T)  {
	db := ethdb.NewMemDatabase()
	tds, _ := state.NewTrieDbState(context.Background(), common.Hash{}, db, 0)
	stateDb:=state.New(tds)
	t.Log("0"+string(tds.Dump()))

	senderKey,_:=crypto.GenerateKey()
	senderAddr :=crypto.PubkeyToAddress(senderKey.PublicKey)

	so:=stateDb.GetOrNewStateObject(senderAddr)
	so.AddBalance(big.NewInt(100))
	key:=common.HexToHash("0x1")
	val:=common.HexToHash("0x2")
	so.SetState(key,val )
	t.Log("so state",so.GetState(key))

	t.Log("1"+string(tds.Dump()))
	stateDb.CreateAccount(senderAddr, true)
	t.Log("21"+string(tds.Dump()))
	so2:=stateDb.GetOrNewStateObject(senderAddr)

	t.Log("so2 state",so2.GetState(key))
	t.Log("so state",so.GetState(key))
	if so.GetState(key) != so2.GetState(key) {
		t.FailNow()
	}
	//
	//
	//st := state.New(tds)
	//
	//evm:=NewEVM(Context{}, st, &params.ChainConfig{}, Config{})
	//
	//sender := AccountRef(senderAddr)
	//evm.Create2(sender, []byte{}, 1000000, big.NewInt(100), big.NewInt(150))
	//t.Log(tds.Dump())
}