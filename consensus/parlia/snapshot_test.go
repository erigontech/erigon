package parlia

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/assert"
)

func TestValidatorSetSort(t *testing.T) {
	size := 100
	validators := make([]libcommon.Address, size)
	for i := 0; i < size; i++ {
		validators[i] = randomAddress()
	}
	sort.Sort(validatorsAscending(validators))
	for i := 0; i < size-1; i++ {
		assert.True(t, bytes.Compare(validators[i][:], validators[i+1][:]) < 0)
	}
}

func randomAddress() libcommon.Address {
	addrBytes := make([]byte, 20)
	rand.Read(addrBytes)
	return libcommon.BytesToAddress(addrBytes)
}
