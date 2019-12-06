package dbutils

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/stretchr/testify/assert"
)

func TestHeaderTypeDetection(t *testing.T) {

	headerHashKey := common.Hex2Bytes("00000000000000006e")
	assert.False(t, IsHeaderKey(headerHashKey))
	assert.False(t, IsHeaderTDKey(headerHashKey))
	assert.True(t, IsHeaderHashKey(headerHashKey))

	headerKey := common.Hex2Bytes("0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd")
	assert.True(t, IsHeaderKey(headerKey))
	assert.False(t, IsHeaderTDKey(headerKey))
	assert.False(t, IsHeaderHashKey(headerKey))

	headerTdKey := common.Hex2Bytes("0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd74")
	assert.False(t, IsHeaderKey(headerTdKey))
	assert.True(t, IsHeaderTDKey(headerTdKey))
	assert.False(t, IsHeaderHashKey(headerTdKey))

}
