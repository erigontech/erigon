// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package trie

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/big"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

func init() {
	spew.Config.Indent = "    "
	spew.Config.DisableMethods = false
}

// Used for testing
func newEmpty() *Trie {
	trie := New(common.Hash{})
	return trie
}

func TestEmptyTrie(t *testing.T) {
	var trie Trie
	res := trie.Hash()
	exp := EmptyRoot
	if res != common.Hash(exp) {
		t.Errorf("expected %x got %x", exp, res)
	}
}

func TestNull(t *testing.T) {
	var trie Trie
	key := make([]byte, 32)
	value := []byte("test")
	trie.Update(key, value)
	v, _ := trie.Get(key)
	if !bytes.Equal(v, value) {
		t.Fatal("wrong value")
	}
}

func TestInsert(t *testing.T) {
	t.Skip("we don't support different key length")
	trie := newEmpty()

	updateString(trie, "doe", "reindeer")
	updateString(trie, "dog", "puppy")
	updateString(trie, "dogglesworth", "cat")

	fmt.Printf("\n\n%s\n\n", trie.root.fstring(""))
	exp := common.HexToHash("8aad789dff2f538bca5d8ea56e8abe10f4c7ba3a5dea95fea4cd6e7c3a1168d3")
	root := trie.Hash()
	if root != exp {
		t.Errorf("case 1: exp %x got %x", exp, root)
	}

	trie = newEmpty()
	updateString(trie, "A", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	exp = common.HexToHash("d23786fb4a010da3ce639d66d5e904a11dbc02746d1ce25029e53290cabf28ab")
	root = trie.Hash()
	if root != exp {
		t.Errorf("case 2: exp %x got %x", exp, root)
	}
}
func TestInsert2(t *testing.T) {
	/**
	  	            "0x52b1e6dd9d145a370c4bf3aa97644c95e5ff76f2": {
	                    "root": "ba8967b16f6e42900825a5a321d26ab6f21e2faa9e2cb186e5527c5276e92af9",
	                    "codeHash": "7678943ba1f399d76abe8e77b6f899c193f72aaefb5c4bd47fffb63c7f57ad9e",
	                    "code": "606060405236156101a05760e060020a60003504630924120081146101c25780630a16697a146101cf5780630fd1f94e146101d8578063137c638b1461022e57806321835af61461023b57806324032866146102545780632f95b833146102d65780633017fe24146102e55780633233c686146102ef57806337f4c00e146102fa5780634500054f146103055780634e417a98146103785780634e71d92d146103e15780634f059a43146103f35780636146195414610451578063625cc4651461046157806367ce940d1461046a5780637d298ee314610477578063830953ab146104f9578063938b5f321461050457806395ee122114610516578063974654f41461052a578063a06db7dc14610535578063a9d2293d14610541578063ae45850b14610597578063b0f07e44146105a9578063c19d93fb146105cb578063c6502da81461062e578063c680362214610637578063ca94692d1461064a578063cc3471af14610673578063d379be23146106c9578063d62457f6146106e3578063ea8a1af0146106ee578063f5562753146107f3578063f6b4dfb414610854575b610868600080548190600160a060020a03908116339091161461087a57610994565b610868600b5460ff165b90565b610868600d5481565b6108686000731deeda36e15ec9e80f3d7414d67a4803ae45fc80630fd1f94e6040518160e060020a02815260040180905060206040518083038160008760325a03f2156100025750506040515191506101cc9050565b6108685b62012cc86101cc565b61086860043560008160001415610dc65750600161084f565b6108686004356024356000731deeda36e15ec9e80f3d7414d67a4803ae45fc80630bd295e6600360005085856040518460e060020a0281526004018084815260200183600160a060020a03168152602001828152602001935050505060206040518083038160008760325a03f215610002575050604051519150505b92915050565b61099860085461ffff166101cc565b61086860026101cc565b610868600a546101cc565b6108686006546101cc565b6108686000731deeda36e15ec9e80f3d7414d67a4803ae45fc8063a09431546003600050336040518360e060020a0281526004018083815260200182600160a060020a031681526020019250505060206040518083038160008760325a03f2156100025750506040515191506101cc9050565b6109af60408051602081810183526000825282516004805460026001821615610100026000190190911604601f81018490048402830184019095528482529293909291830182828015610a7d5780601f10610a5257610100808354040283529160200191610a7d565b61086860006000600180610b7b6105cf565b6108686000731deeda36e15ec9e80f3d7414d67a4803ae45fc8063f5562753436040518260e060020a0281526004018082815260200191505060206040518083038160008760325a03f2156100025750506040515191506101cc9050565b610a1d6000600480610c986105cf565b61086860025481565b6108685b620186a06101cc565b6108686004356024355b6000731deeda36e15ec9e80f3d7414d67a4803ae45fc8063a1873db6600360005085856040518460e060020a0281526004018084815260200183600160a060020a03168152602001828152602001935050505060206040518083038160008760325a03f2156100025750506040515191506102d09050565b6108686009546101cc565b610a1f600c54600160a060020a031681565b610868600b5462010000900460ff166101cc565b6108686007546101cc565b610a3c600e5460ff1681565b6108686000731deeda36e15ec9e80f3d7414d67a4803ae45fc8063a9d2293d6040518160e060020a02815260040180905060206040518083038160008760325a03f2156100025750506040515191506101cc9050565b610a1f600054600160a060020a031681565b610868600080548190600160a060020a039081163390911614610a8957610994565b6108685b6000731deeda36e15ec9e80f3d7414d67a4803ae45fc80635054d98a60036000506040518260e060020a0281526004018082815260200191505060206040518083038160008760325a03f2156100025750506040515191506101cc9050565b61086860015481565b610868600b54610100900460ff166101cc565b61086860035474010000000000000000000000000000000000000000900460e060020a026101cc565b6108686000731deeda36e15ec9e80f3d7414d67a4803ae45fc8063cc3471af6040518160e060020a02815260040180905060206040518083038160008760325a03f2156100025750506040515191506101cc9050565b610a1f600854620100009004600160a060020a03166101cc565b6108686005546101cc565b610a1d604080517fa09431540000000000000000000000000000000000000000000000000000000081526003600482015233600160a060020a031660248201529051731deeda36e15ec9e80f3d7414d67a4803ae45fc809163a0943154916044808301926020929190829003018160008760325a03f215610002575050604051511590506107f157604080517f7e9265620000000000000000000000000000000000000000000000000000000081526003600482015233600160a060020a031660248201529051731deeda36e15ec9e80f3d7414d67a4803ae45fc8091637e9265629160448083019260009291908290030181838760325a03f215610002575050505b565b6108686004356000731deeda36e15ec9e80f3d7414d67a4803ae45fc8063f5562753836040518260e060020a0281526004018082815260200191505060206040518083038160008760325a03f215610002575050604051519150505b919050565b610a1f600354600160a060020a03166101cc565b60408051918252519081900360200190f35b60045460006002600183161561010002600019019092169190910411156108a45760009150610994565b6108ac6105cf565b9050600081141580156108c0575060018114155b80156108cd575060028114155b156108db5760009150610994565b600480546000828152602060026001841615610100026000190190931692909204601f908101929092047f8a35acfbc15ff81a39ae7d344fd709f28e8600b4aa8c65c6b64bfe7fe36bd19b9081019236929083901061095d5782800160ff198235161785555b5061098d9291505b808211156109945760008155600101610949565b82800160010185558215610941579182015b8281111561094157823582600050559160200191906001019061096f565b5050600191505b5090565b6040805161ffff9092168252519081900360200190f35b60405180806020018281038252838181518152602001915080519060200190808383829060006004602084601f0104600f02600301f150905090810190601f168015610a0f5780820380516001836020036101000a031916815260200191505b509250505060405180910390f35b005b60408051600160a060020a03929092168252519081900360200190f35b6040805160ff9092168252519081900360200190f35b820191906000526020600020905b815481529060010190602001808311610a6057829003601f168201915b505050505090506101cc565b6004546000600260018316156101000260001901909216919091041115610ab35760009150610994565b610abb6105cf565b905060008114158015610acf575060018114155b8015610adc575060028114155b15610aea5760009150610994565b604080517f7c0278fc000000000000000000000000000000000000000000000000000000008152600360048201818152602483019384523660448401819052731deeda36e15ec9e80f3d7414d67a4803ae45fc8094637c0278fc946000939190606401848480828437820191505094505050505060006040518083038160008760325a03f215610002575050505090565b1415610c8557604080516001547f0fee183d0000000000000000000000000000000000000000000000000000000082526003600483015233600160a060020a0316602483015234604483015260648201529051731deeda36e15ec9e80f3d7414d67a4803ae45fc8091630fee183d916084828101926020929190829003018160008760325a03f21561000257505060405151925050811515610c8a577389efe605e9ecbe22849cd85d5449cc946c26f8f36312c82bcc33346040518360e060020a0281526004018083600160a060020a031681526020018281526020019250505060206040518083038160008760325a03f2156100025750506040515115159050610c8a57610002565b505090565b81925050610994565b505b50565b1415610c93575a9150610cab3383610481565b1515610cb75750610c95565b731deeda36e15ec9e80f3d7414d67a4803ae45fc8063da46be0a60038433610cdd61046e565b610ce5610232565b6040518660e060020a0281526004018086815260200185815260200184600160a060020a031681526020018381526020018281526020019550505050505060006040518083038160008760325a03f21561000257505050610c933360408051600080547fc17e6817000000000000000000000000000000000000000000000000000000008352600160a060020a03908116600484015230163160248301529151731deeda36e15ec9e80f3d7414d67a4803ae45fc809263c17e68179260448082019360209390928390039091019082908760325a03f2156100025750505050565b30600160a060020a031660405180807f5f5f6469672875696e7432353629000000000000000000000000000000000000815260200150600e019050604051809103902060e060020a8091040260e060020a9004600184036040518260e060020a0281526004018082815260200191505060006040518083038160008760325a03f292505050151561084f5761000256",
	                    "storage": {
	                        "0x0000000000000000000000000000000000000000000000000000000000000000": "7dd677b54fc954824a7bc49bd26cbdfa12c75adf",
	                        "0x0000000000000000000000000000000000000000000000000000000000000001": "12091e9dab6f07",
	                        "0x0000000000000000000000000000000000000000000000000000000000000002": "2e2bd37f2782",
	                        "0x0000000000000000000000000000000000000000000000000000000000000003": "3defb9627dd677b54fc954824a7bc49bd26cbdfa12c75adf",
	                        "0x0000000000000000000000000000000000000000000000000000000000000006": "04a817c800",
	                        "0x0000000000000000000000000000000000000000000000000000000000000007": "1e8480",
	                        "0x0000000000000000000000000000000000000000000000000000000000000008": "61727f495f6bcaa392abbfde17cc7fb88ee45934000a",
	                        "0x0000000000000000000000000000000000000000000000000000000000000009": "075d52e6c822ab",
	                        "0x000000000000000000000000000000000000000000000000000000000000000b": "0101",
	                        "0x000000000000000000000000000000000000000000000000000000000000000c": "6c8f2a135f6ed072de4503bd7c4999a1a17f824b",
	                        "0x000000000000000000000000000000000000000000000000000000000000000d": "1202d9",
	                        "0x000000000000000000000000000000000000000000000000000000000000000e": "ff"
	                    }
	                },
	*/

	trie := newEmpty()
	h:= func(b []byte) []byte {
		v,_:=common.HashData(b)
		return v.Bytes()
	}

	trie.Update(h(common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000")), common.Hex2Bytes("7dd677b54fc954824a7bc49bd26cbdfa12c75adf"))
	trie.Update(h(common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000001")), common.Hex2Bytes("12091e9dab6f07"))
	trie.Update(h(common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000002")), common.Hex2Bytes("2e2bd37f2782"))
	trie.Update(h(common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000003")), common.Hex2Bytes("3defb9627dd677b54fc954824a7bc49bd26cbdfa12c75adf"))
	trie.Update(h(common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000006")), common.Hex2Bytes("04a817c800"))
	trie.Update(h(common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000007")), common.Hex2Bytes("1e8480"))
	trie.Update(h(common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000008")), common.Hex2Bytes("61727f495f6bcaa392abbfde17cc7fb88ee45934000a"))
	trie.Update(h(common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000009")), common.Hex2Bytes("075d52e6c822ab"))
	trie.Update(h(common.Hex2Bytes("000000000000000000000000000000000000000000000000000000000000000b")), common.Hex2Bytes("0101"))
	trie.Update(h(common.Hex2Bytes("000000000000000000000000000000000000000000000000000000000000000c")), common.Hex2Bytes("6c8f2a135f6ed072de4503bd7c4999a1a17f824b"))
	trie.Update(h(common.Hex2Bytes("000000000000000000000000000000000000000000000000000000000000000d")), common.Hex2Bytes("1202d9"))
	trie.Update(h(common.Hex2Bytes("000000000000000000000000000000000000000000000000000000000000000e")), common.Hex2Bytes("ff"))
	fmt.Println(trie.Get(common.Hex2Bytes("000000000000000000000000000000000000000000000000000000000000000e")))
	fmt.Println(common.HexToHash("ff").Bytes())
	fmt.Printf("\n\n%s\n\n", trie.root.fstring(""))
	exp := common.HexToHash("ba8967b16f6e42900825a5a321d26ab6f21e2faa9e2cb186e5527c5276e92af9")
	root := trie.Hash()
	if root != exp {
		t.Errorf("case 1: exp %x got %x", exp, root)
	}
}

func TestGet(t *testing.T) {
	t.Skip("different length of key is not supported")
	trie := newEmpty()

	updateString(trie, "doe", "reindeer")
	updateString(trie, "dog", "puppy")
	updateString(trie, "dogglesworth", "cat")

	for i := 0; i < 2; i++ {
		res := getString(trie, "dog")
		if !bytes.Equal(res, []byte("puppy")) {
			t.Errorf("expected puppy got %x", res)
		}

		unknown := getString(trie, "unknown")
		if unknown != nil {
			t.Errorf("expected nil got %x", unknown)
		}

		if i == 1 {
			return
		}
	}
}

func TestDelete(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth")

	trie := newEmpty()
	vals := []struct{ k, v string }{
		{"do", "verb"},
		{"ether", "wookiedoo"},
		{"horse", "stallion"},
		{"shaman", "horse"},
		{"doge", "coin"},
		{"ether", ""},
		{"dog", "puppy"},
		{"shaman", ""},
	}
	for _, val := range vals {
		if val.v != "" {
			updateString(trie, val.k, val.v)
		} else {
			deleteString(trie, val.k)
		}
	}

	hash := trie.Hash()
	exp := common.HexToHash("5991bb8c6514148a29db676a14ac506cd2cd5775ace63c30a4fe457715e9ac84")
	if hash != exp {
		t.Errorf("expected %x got %x", exp, hash)
	}
}

func TestEmptyValues(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth")
	trie := newEmpty()

	vals := []struct{ k, v string }{
		{"do", "verb"},
		{"ether", "wookiedoo"},
		{"horse", "stallion"},
		{"shaman", "horse"},
		{"doge", "coin"},
		{"ether", ""},
		{"dog", "puppy"},
		{"shaman", ""},
	}
	for _, val := range vals {
		updateString(trie, val.k, val.v)
	}

	hash := trie.Hash()
	exp := common.HexToHash("5991bb8c6514148a29db676a14ac506cd2cd5775ace63c30a4fe457715e9ac84")
	if hash != exp {
		t.Errorf("expected %x got %x", exp, hash)
	}
}

func TestReplication(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth")

	trie := newEmpty()
	vals := []struct{ k, v string }{
		{"do", "verb"},
		{"ether", "wookiedoo"},
		{"horse", "stallion"},
		{"shaman", "horse"},
		{"doge", "coin"},
		{"dog", "puppy"},
		{"somethingveryoddindeedthis is", "myothernodedata"},
	}
	for _, val := range vals {
		updateString(trie, val.k, val.v)
	}
	exp := trie.Hash()

	// create a new trie on top of the database and check that lookups work.
	trie2 := New(exp)
	for _, kv := range vals {
		if string(getString(trie2, kv.k)) != kv.v {
			t.Errorf("trie2 doesn't have %q => %q", kv.k, kv.v)
		}
	}
	hash := trie2.Hash()
	if hash != exp {
		t.Errorf("root failure. expected %x got %x", exp, hash)
	}

	// perform some insertions on the new trie.
	vals2 := []struct{ k, v string }{
		{"do", "verb"},
		{"ether", "wookiedoo"},
		{"horse", "stallion"},
		// {"shaman", "horse"},
		// {"doge", "coin"},
		// {"ether", ""},
		// {"dog", "puppy"},
		// {"somethingveryoddindeedthis is", "myothernodedata"},
		// {"shaman", ""},
	}
	for _, val := range vals2 {
		updateString(trie2, val.k, val.v)
	}
	if hash := trie2.Hash(); hash != exp {
		t.Errorf("root failure. expected %x got %x", exp, hash)
	}
}

func TestLargeValue(t *testing.T) {
	trie := newEmpty()
	trie.Update([]byte("key1"), []byte{99, 99, 99, 99})
	trie.Update([]byte("key2"), bytes.Repeat([]byte{1}, 32))
	trie.Hash()
}

type countingDB struct {
	ethdb.Database
	gets map[string]int
}

func (db *countingDB) Get(bucket string, key []byte) ([]byte, error) {
	db.gets[string(key)]++
	return db.Database.Get(bucket, key)
}

// TestRandomCases tests som cases that were found via random fuzzing
func TestRandomCases(t *testing.T) {
	var rt []randTestStep = []randTestStep{
		{op: 6, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 0
		{op: 6, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 1
		{op: 0, key: common.Hex2Bytes("d51b182b95d677e5f1c82508c0228de96b73092d78ce78b2230cd948674f66fd1483bd"), value: common.Hex2Bytes("0000000000000002")},   // step 2
		{op: 2, key: common.Hex2Bytes("c2a38512b83107d665c65235b0250002882ac2022eb00711552354832c5f1d030d0e408e"), value: common.Hex2Bytes("")},                 // step 3
		{op: 3, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 4
		{op: 3, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 5
		{op: 6, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 6
		{op: 3, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 7
		{op: 0, key: common.Hex2Bytes("c2a38512b83107d665c65235b0250002882ac2022eb00711552354832c5f1d030d0e408e"), value: common.Hex2Bytes("0000000000000008")}, // step 8
		{op: 0, key: common.Hex2Bytes("d51b182b95d677e5f1c82508c0228de96b73092d78ce78b2230cd948674f66fd1483bd"), value: common.Hex2Bytes("0000000000000009")},   // step 9
		{op: 2, key: common.Hex2Bytes("fd"), value: common.Hex2Bytes("")},                                                                                       // step 10
		{op: 6, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 11
		{op: 6, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 12
		{op: 0, key: common.Hex2Bytes("fd"), value: common.Hex2Bytes("000000000000000d")},                                                                       // step 13
		{op: 6, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 14
		{op: 1, key: common.Hex2Bytes("c2a38512b83107d665c65235b0250002882ac2022eb00711552354832c5f1d030d0e408e"), value: common.Hex2Bytes("")},                 // step 15
		{op: 3, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 16
		{op: 0, key: common.Hex2Bytes("c2a38512b83107d665c65235b0250002882ac2022eb00711552354832c5f1d030d0e408e"), value: common.Hex2Bytes("0000000000000011")}, // step 17
		{op: 5, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 18
		{op: 3, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")},                                                                                         // step 19
		// FIXME: fix these testcases for turbo-geth
		//{op: 0, key: common.Hex2Bytes("d51b182b95d677e5f1c82508c0228de96b73092d78ce78b2230cd948674f66fd1483bd"), value: common.Hex2Bytes("0000000000000014")},           // step 20
		//{op: 0, key: common.Hex2Bytes("d51b182b95d677e5f1c82508c0228de96b73092d78ce78b2230cd948674f66fd1483bd"), value: common.Hex2Bytes("0000000000000015")},           // step 21
		//{op: 0, key: common.Hex2Bytes("c2a38512b83107d665c65235b0250002882ac2022eb00711552354832c5f1d030d0e408e"), value: common.Hex2Bytes("0000000000000016")},         // step 22
		{op: 5, key: common.Hex2Bytes(""), value: common.Hex2Bytes("")}, // step 23
		//{op: 1, key: common.Hex2Bytes("980c393656413a15c8da01978ed9f89feb80b502f58f2d640e3a2f5f7a99a7018f1b573befd92053ac6f78fca4a87268"), value: common.Hex2Bytes("")}, // step 24
		//{op: 1, key: common.Hex2Bytes("fd"), value: common.Hex2Bytes("")}, // step 25
	}
	runRandTest(rt)

}

// randTest performs random trie operations.
// Instances of this test are created by Generate.
type randTest []randTestStep

type randTestStep struct {
	op    int
	key   []byte // for opUpdate, opDelete, opGet
	value []byte // for opUpdate
	err   error  // for debugging
}

const (
	opUpdate = iota
	opDelete
	opGet
	opCommit
	opHash
	opReset
	opItercheckhash
	opCheckCacheInvariant
	opMax // boundary value, not an actual op
)

func (randTest) Generate(r *rand.Rand, size int) reflect.Value {
	var allKeys [][]byte
	genKey := func() []byte {
		if len(allKeys) < 2 || r.Intn(100) < 10 {
			// new key
			key := make([]byte, r.Intn(50))
			r.Read(key)
			allKeys = append(allKeys, key)
			return key
		}
		// use existing key
		return allKeys[r.Intn(len(allKeys))]
	}

	var steps randTest
	for i := 0; i < size; i++ {
		step := randTestStep{op: r.Intn(opMax)}
		switch step.op {
		case opUpdate:
			step.key = genKey()
			step.value = make([]byte, 8)
			binary.BigEndian.PutUint64(step.value, uint64(i))
		case opGet, opDelete:
			step.key = genKey()
		}
		steps = append(steps, step)
	}
	return reflect.ValueOf(steps)
}

func runRandTest(rt randTest) bool {
	tr := New(common.Hash{})
	values := make(map[string]string) // tracks content of the trie

	for i, step := range rt {
		fmt.Printf("{op: %d, key: common.Hex2Bytes(\"%x\"), value: common.Hex2Bytes(\"%x\")}, // step %d\n",
			step.op, step.key, step.value, i)
		switch step.op {
		case opUpdate:
			tr.Update(step.key, step.value)
			values[string(step.key)] = string(step.value)
		case opDelete:
			tr.Delete(step.key)
			delete(values, string(step.key))
		case opGet:
			v, _ := tr.Get(step.key)
			want := values[string(step.key)]
			if string(v) != want {
				rt[i].err = fmt.Errorf("mismatch for key 0x%x, got 0x%x want 0x%x", step.key, v, want)
			}
		case opCommit:
		case opHash:
			tr.Hash()
		case opReset:
			hash := tr.Hash()
			newtr := New(hash)
			tr = newtr
		case opItercheckhash:
			// FIXME: restore for turbo-geth
			/*
				checktr := New(common.Hash{})
				it := NewIterator(tr.NodeIterator(nil))
				for it.Next() {
					checktr.Update(it.Key, it.Value)
				}
				if tr.Hash() != checktr.Hash() {
					rt[i].err = fmt.Errorf("hash mismatch in opItercheckhash")
				}
			*/
		case opCheckCacheInvariant:
		}
		// Abort the test on error.
		if rt[i].err != nil {
			return false
		}
	}
	return true
}

func TestRandom(t *testing.T) {
	t.Skip("should be restored. skipped for turbo-geth")

	if err := quick.Check(runRandTest, nil); err != nil {
		if cerr, ok := err.(*quick.CheckError); ok {
			t.Fatalf("random test iteration %d failed: %s", cerr.Count, spew.Sdump(cerr.In))
		}
		t.Fatal(err)
	}
}

func BenchmarkGet(b *testing.B)      { benchGet(b, false) }
func BenchmarkGetDB(b *testing.B)    { benchGet(b, true) }
func BenchmarkUpdateBE(b *testing.B) { benchUpdate(b, binary.BigEndian) }
func BenchmarkUpdateLE(b *testing.B) { benchUpdate(b, binary.LittleEndian) }

const benchElemCount = 20000

func benchGet(b *testing.B, commit bool) {
	trie := new(Trie)
	var tmpdir string
	var tmpdb ethdb.Database
	if commit {
		tmpdir, tmpdb = tempDB()
		trie = New(common.Hash{})
	}
	k := make([]byte, 32)
	for i := 0; i < benchElemCount; i++ {
		binary.LittleEndian.PutUint64(k, uint64(i))
		trie.Update(k, k)
	}
	binary.LittleEndian.PutUint64(k, benchElemCount/2)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		trie.Get(k)
	}
	b.StopTimer()

	if commit {
		tmpdb.Close()
		os.RemoveAll(tmpdir)
	}
}

func benchUpdate(b *testing.B, e binary.ByteOrder) *Trie {
	trie := newEmpty()
	k := make([]byte, 32)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		e.PutUint64(k, uint64(i))
		trie.Update(k, k)
	}
	return trie
}

// Benchmarks the trie hashing. Since the trie caches the result of any operation,
// we cannot use b.N as the number of hashing rouns, since all rounds apart from
// the first one will be NOOP. As such, we'll use b.N as the number of account to
// insert into the trie before measuring the hashing.
func BenchmarkHash(b *testing.B) {
	// Make the random benchmark deterministic
	random := rand.New(rand.NewSource(0))

	// Create a realistic account trie to hash
	addresses := make([][20]byte, b.N)
	for i := 0; i < len(addresses); i++ {
		for j := 0; j < len(addresses[i]); j++ {
			addresses[i][j] = byte(random.Intn(256))
		}
	}
	accounts := make([][]byte, len(addresses))
	for i := 0; i < len(accounts); i++ {
		var (
			nonce   = uint64(random.Int63())
			balance = new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))
			root    = EmptyRoot
			code    = crypto.Keccak256(nil)
		)
		accounts[i], _ = rlp.EncodeToBytes([]interface{}{nonce, balance, root, code})
	}
	// Insert the accounts into the trie and hash it
	trie := newEmpty()
	for i := 0; i < len(addresses); i++ {
		trie.Update(crypto.Keccak256(addresses[i][:]), accounts[i])
	}
	b.ResetTimer()
	b.ReportAllocs()
	trie.Hash()
}

func tempDB() (string, ethdb.Database) {
	dir, err := ioutil.TempDir("", "trie-bench")
	if err != nil {
		panic(fmt.Sprintf("can't create temporary directory: %v", err))
	}
	return dir, ethdb.MustOpen(dir)
}

func getString(trie *Trie, k string) []byte {
	v, _ := trie.Get([]byte(k))
	return v
}

func updateString(trie *Trie, k, v string) {
	trie.Update([]byte(k), []byte(v))
}

func deleteString(trie *Trie, k string) {
	trie.Delete([]byte(k))
}

func TestDeepHash(t *testing.T) {
	acc := accounts.NewAccount()
	prefix := "prefix"
	var testdata = [][]struct {
		key   string
		value string
	}{
		{{"key1", "value1"}},
		{{"key1", "value1"}, {"key2", "value2"}},
		{{"key1", "value1"}, {"key2", "value2"}, {"key3", "value3"}},
		{{"key1", "value1"}, {"key2", "value2"}, {"\xffek3", "value3"}},
	}
	for i, keyVals := range testdata {
		fmt.Println("Test", i)
		trie := New(common.Hash{})
		for _, keyVal := range keyVals {
			trie.Update([]byte(keyVal.key), []byte(keyVal.value))
		}
		trie.PrintTrie()
		hash1 := trie.Hash()

		prefixTrie := New(common.Hash{})
		prefixTrie.UpdateAccount([]byte(prefix), &acc)
		for _, keyVal := range keyVals {
			// Add a prefix to every key
			prefixTrie.Update([]byte(prefix+keyVal.key), []byte(keyVal.value))
		}

		got2, hash2 := prefixTrie.DeepHash([]byte(prefix))
		if !got2 {
			t.Errorf("Expected DeepHash returning true, got false, testcase %d", i)
		}
		if hash1 != hash2 {
			t.Errorf("DeepHash mistmatch: %x, expected %x, testcase %d", hash2, hash1, i)
		}
	}
}

func TestHashMapLeak(t *testing.T) {
	debug.OverrideGetNodeData(true)
	defer debug.OverrideGetNodeData(false)
	// freeze the randomness
	random := rand.New(rand.NewSource(794656320434))

	// now create a trie with some small and some big leaves
	trie := newEmpty()
	nTouches := 256 * 10

	var key [1]byte
	var val [8]byte
	for i := 0; i < nTouches; i++ {
		key[0] = byte(random.Intn(256))
		binary.BigEndian.PutUint64(val[:], random.Uint64())

		option := random.Intn(3)
		if option == 0 {
			// small leaf node
			trie.Update(key[:], val[:])
		} else if option == 1 {
			// big leaf node
			trie.Update(key[:], crypto.Keccak256(val[:]))
		} else {
			// test delete as well
			trie.Delete(key[:])
		}
	}

	// check the size of trie's hash map
	trie.Hash()

	nHashes := trie.HashMapSize()
	nExpected := 1 + 16 + 256/3

	assert.GreaterOrEqual(t, nHashes, nExpected*7/8)
	assert.LessOrEqual(t, nHashes, nExpected*9/8)
}

func genRandomByteArrayOfLen(length uint) []byte {
	array := make([]byte, length)
	for i := uint(0); i < length; i++ {
		array[i] = byte(rand.Intn(256))
	}
	return array
}

func getAddressForIndex(index int) [20]byte {
	var address [20]byte
	binary.BigEndian.PutUint32(address[:], uint32(index))
	return address
}

func TestCodeNodeValid(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	numberOfAccounts := 20

	addresses := make([][20]byte, numberOfAccounts)
	for i := 0; i < len(addresses); i++ {
		addresses[i] = getAddressForIndex(i)
	}
	codeValues := make([][]byte, len(addresses))
	for i := 0; i < len(addresses); i++ {
		codeValues[i] = genRandomByteArrayOfLen(128)
		codeHash := common.BytesToHash(crypto.Keccak256(codeValues[i]))
		balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))
		acc := accounts.NewAccount()
		acc.Nonce = uint64(random.Int63())
		acc.Balance.SetFromBig(balance)
		acc.Root = EmptyRoot
		acc.CodeHash = codeHash

		trie.UpdateAccount(crypto.Keccak256(addresses[i][:]), &acc)
		err := trie.UpdateAccountCode(crypto.Keccak256(addresses[i][:]), codeValues[i])
		assert.Nil(t, err, "should successfully insert code")
	}

	for i := 0; i < len(addresses); i++ {
		value, gotValue := trie.GetAccountCode(crypto.Keccak256(addresses[i][:]))
		assert.True(t, gotValue, "should receive code value")
		assert.True(t, bytes.Equal(value, codeValues[i]), "should receive the right code")
	}
}

func TestCodeNodeUpdateNotExisting(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	address := getAddressForIndex(0)
	codeValue := genRandomByteArrayOfLen(128)

	codeHash := common.BytesToHash(crypto.Keccak256(codeValue))
	balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	err := trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue)
	assert.Nil(t, err, "should successfully insert code")

	nonExistingAddress := getAddressForIndex(9999)
	codeValue2 := genRandomByteArrayOfLen(128)

	err = trie.UpdateAccountCode(crypto.Keccak256(nonExistingAddress[:]), codeValue2)
	assert.Error(t, err, "should return an error for non existing acc")
}

func TestCodeNodeGetNotExistingAccount(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	address := getAddressForIndex(0)
	codeValue := genRandomByteArrayOfLen(128)

	codeHash := common.BytesToHash(crypto.Keccak256(codeValue))
	balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	err := trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue)
	assert.Nil(t, err, "should successfully insert code")

	nonExistingAddress := getAddressForIndex(9999)

	value, gotValue := trie.GetAccountCode(crypto.Keccak256(nonExistingAddress[:]))
	assert.True(t, gotValue, "should indicate that account doesn't exist at all (not just hashed)")
	assert.Nil(t, value, "the value should be nil")
}

func TestCodeNodeGetHashedAccount(t *testing.T) {
	trie := newEmpty()

	address := getAddressForIndex(0)

	fakeAccount := genRandomByteArrayOfLen(50)
	fakeAccountHash := common.BytesToHash(crypto.Keccak256(fakeAccount))

	hex := keybytesToHex(crypto.Keccak256(address[:]))

	_, trie.root = trie.insert(trie.root, hex, hashNode{hash: fakeAccountHash[:]})

	value, gotValue := trie.GetAccountCode(crypto.Keccak256(address[:]))
	assert.False(t, gotValue, "should indicate that account exists but hashed")
	assert.Nil(t, value, "the value should be nil")
}

func TestCodeNodeGetExistingAccountNoCodeNotEmpty(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	address := getAddressForIndex(0)
	codeValue := genRandomByteArrayOfLen(128)

	codeHash := common.BytesToHash(crypto.Keccak256(codeValue))
	balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)

	value, gotValue := trie.GetAccountCode(crypto.Keccak256(address[:]))
	assert.False(t, gotValue, "should indicate that account exists with code but the code isn't in cache")
	assert.Nil(t, value, "the value should be nil")
}

func TestCodeNodeGetExistingAccountEmptyCode(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	address := getAddressForIndex(0)

	codeHash := EmptyCodeHash
	balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)

	value, gotValue := trie.GetAccountCode(crypto.Keccak256(address[:]))
	assert.True(t, gotValue, "should indicate that account exists with empty code")
	assert.Nil(t, value, "the value should be nil")
}

func TestCodeNodeWrongHash(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	address := getAddressForIndex(0)

	codeValue1 := genRandomByteArrayOfLen(128)
	codeHash1 := common.BytesToHash(crypto.Keccak256(codeValue1))

	balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash1

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)

	codeValue2 := genRandomByteArrayOfLen(128)
	err := trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue2)
	assert.Error(t, err, "should NOT be able to insert code with wrong hash")
}

func TestCodeNodeUpdateAccountAndCodeValidHash(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	address := getAddressForIndex(0)

	codeValue1 := genRandomByteArrayOfLen(128)
	codeHash1 := common.BytesToHash(crypto.Keccak256(codeValue1))

	balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash1

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	err := trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue1)
	assert.Nil(t, err, "should successfully insert code")

	codeValue2 := genRandomByteArrayOfLen(128)
	codeHash2 := common.BytesToHash(crypto.Keccak256(codeValue2))

	acc.CodeHash = codeHash2

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	err = trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue2)
	assert.Nil(t, err, "should successfully insert code")
}

func TestCodeNodeUpdateAccountAndCodeInvalidHash(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	address := getAddressForIndex(0)

	codeValue1 := genRandomByteArrayOfLen(128)
	codeHash1 := common.BytesToHash(crypto.Keccak256(codeValue1))

	balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash1

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	err := trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue1)
	assert.Nil(t, err, "should successfully insert code")

	codeValue2 := genRandomByteArrayOfLen(128)
	codeHash2 := common.BytesToHash(crypto.Keccak256(codeValue2))

	codeValue3 := genRandomByteArrayOfLen(128)

	acc.CodeHash = codeHash2

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	err = trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue3)
	assert.Error(t, err, "should NOT be able to insert code with wrong hash")
}

func TestCodeNodeUpdateAccountChangeCodeHash(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	address := getAddressForIndex(0)

	codeValue1 := genRandomByteArrayOfLen(128)
	codeHash1 := common.BytesToHash(crypto.Keccak256(codeValue1))

	balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash1

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	err := trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue1)
	assert.Nil(t, err, "should successfully insert code")

	codeValue2 := genRandomByteArrayOfLen(128)
	codeHash2 := common.BytesToHash(crypto.Keccak256(codeValue2))

	acc.CodeHash = codeHash2

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	value, gotValue := trie.GetAccountCode(crypto.Keccak256(address[:]))
	assert.Nil(t, value, "the value should reset after the code change happen")
	assert.False(t, gotValue, "should indicate that the code isn't in the cache")
}

func TestCodeNodeUpdateAccountNoChangeCodeHash(t *testing.T) {
	trie := newEmpty()

	random := rand.New(rand.NewSource(0))

	address := getAddressForIndex(0)

	codeValue1 := genRandomByteArrayOfLen(128)
	codeHash1 := common.BytesToHash(crypto.Keccak256(codeValue1))

	balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))

	acc := accounts.NewAccount()
	acc.Nonce = uint64(random.Int63())
	acc.Balance.SetFromBig(balance)
	acc.Root = EmptyRoot
	acc.CodeHash = codeHash1

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	err := trie.UpdateAccountCode(crypto.Keccak256(address[:]), codeValue1)
	assert.Nil(t, err, "should successfully insert code")

	acc.Nonce = uint64(random.Int63())
	balance = new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))
	acc.Balance.SetFromBig(balance)

	trie.UpdateAccount(crypto.Keccak256(address[:]), &acc)
	value, gotValue := trie.GetAccountCode(crypto.Keccak256(address[:]))
	assert.Equal(t, codeValue1, value, "the value should NOT reset after account's non codehash had changed")
	assert.True(t, gotValue, "should indicate that the code is still in the cache")
}
