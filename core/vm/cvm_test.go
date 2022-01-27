package vm

import (
	"github.com/ledgerwatch/erigon-lib/starknet"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCreate(t *testing.T) {
	require := require.New(t)
	stubGrpcClient := starknet.NewGrpcClientStub()
	cvm := NewCVM(&dummyStatedb{}, stubGrpcClient)

	caller := AccountRef{}
	contract := []byte("7b22616269223a205b5d7d")
	salt := []byte("salt")

	_, _, err := cvm.Create(caller, contract, salt)

	require.NoError(err)
	require.Equal(1, stubGrpcClient.GetAddressCalls())
}
