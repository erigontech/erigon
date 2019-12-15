package dbutils

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/stretchr/testify/assert"
)

func TestHeaderTypeDetection(t *testing.T) {

	// good input
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

	// bad input
	emptyKey := common.Hex2Bytes("")
	assert.False(t, IsHeaderKey(emptyKey))
	assert.False(t, IsHeaderTDKey(emptyKey))
	assert.False(t, IsHeaderHashKey(emptyKey))

	tooLongKey := common.Hex2Bytes("0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd0000000000004321ed7240d411782ae438adfd85f7edad373cea722318c6e7f5f5b30f9abc9b36fd")
	assert.False(t, IsHeaderKey(tooLongKey))
	assert.False(t, IsHeaderTDKey(tooLongKey))
	assert.False(t, IsHeaderHashKey(tooLongKey))

	notRelatedInput := common.Hex2Bytes("alex")
	assert.False(t, IsHeaderKey(notRelatedInput))
	assert.False(t, IsHeaderTDKey(notRelatedInput))
	assert.False(t, IsHeaderHashKey(notRelatedInput))

}
