package bor

import (
	"os"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon/common/fdlimit"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/tests/bor/helper"
	"github.com/ledgerwatch/log/v3"
)

var (
	// addr1 = 0x71562b71999873DB5b286dF957af199Ec94617F7
	pkey1, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	// addr2 = 0x9fB29AAc15b9A4B7F17c3385939b007540f4d791
	pkey2, _ = crypto.HexToECDSA("9b28f36fbd67381120752d6172ecdcf10e06ab2d9a1367aac00cdcd6ac7855d3")
)

func TestMining(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StreamHandler(os.Stderr, log.TerminalFormat())))
	fdlimit.Raise(2048)

	genesis := helper.InitGenesis(t, "./testdata/genesis_2val.json", 8)
	// fmt.Println(genesis)
	stack, ethbackend, err := helper.InitMiner(&genesis, pkey1, true)
	if err != nil {
		panic(err)
	}
	defer stack.Close()

	time.Sleep(1 * time.Second)

	ethbackend.Start()

	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)

	}

}
