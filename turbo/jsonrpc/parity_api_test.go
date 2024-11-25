// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package jsonrpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/rpc/rpccfg"

	libcommon "github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/erigon-lib/common/hexutility"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/rpc"
)

var latestBlock = rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)

func TestParityAPIImpl_ListStorageKeys_NoOffset(t *testing.T) {
	assert := assert.New(t)
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	baseApi := NewBaseApi(nil, nil, m.BlockReader, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs, nil)
	api := NewParityAPIImpl(baseApi, m.DB)
	answers := []string{
		"0000000000000000000000000000000000000000000000000000000000000000",
		"0000000000000000000000000000000000000000000000000000000000000002",
		"0a2127994676ca91e4eb3d2a1e46ec9dcee074dc2643bb5ebd4e9ac6541a3148",
		"0fe673b4bc06161f39bc26f4e8831c810a72ffe69e5c8cb26f7f54752618e696",
		"120e23dcb7e4437386073613853db77b10011a2404eefc716b97c7767e37f8eb",
	}
	addr := libcommon.HexToAddress("0x920fd5070602feaea2e251e9e7238b6c376bcae5")
	result, err := api.ListStorageKeys(context.Background(), addr, 5, nil, latestBlock)
	if err != nil {
		t.Errorf("calling ListStorageKeys: %v", err)
	}
	assert.Equal(len(answers), len(result))
	for k, v := range result {
		assert.Equal(answers[k], common.Bytes2Hex(v))
	}
}

func TestParityAPIImpl_ListStorageKeys_WithOffset_ExistingPrefix(t *testing.T) {
	assert := assert.New(t)
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewParityAPIImpl(newBaseApiForTest(m), m.DB)
	answers := []string{
		"29d05770ca9ee7088a64e18c8e5160fc62c3c2179dc8ef9b4dbc970c9e51b4d8",
		"29edc84535d98b29835079d685b97b41ee8e831e343cc80793057e462353a26d",
		"2c05ac60f9aa2df5e64ef977f271e4b9a2d13951f123a2cb5f5d4ad5eb344f1a",
		"4644be453c81744b6842ddf615d7fca0e14a23b09734be63d44c23452de95631",
		"4974416255391052161ba8184fe652f3bf8c915592c65f7de127af8e637dce5d",
	}
	addr := libcommon.HexToAddress("0x920fd5070602feaea2e251e9e7238b6c376bcae5")
	offset := libcommon.Hex2Bytes("29")
	b := hexutility.Bytes(offset)
	result, err := api.ListStorageKeys(context.Background(), addr, 5, &b, latestBlock)
	if err != nil {
		t.Errorf("calling ListStorageKeys: %v", err)
	}
	assert.Equal(len(answers), len(result))
	for k, v := range result {
		assert.Equal(answers[k], common.Bytes2Hex(v))
	}
}

func TestParityAPIImpl_ListStorageKeys_WithOffset_NonExistingPrefix(t *testing.T) {
	assert := assert.New(t)
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewParityAPIImpl(newBaseApiForTest(m), m.DB)
	answers := []string{
		"4644be453c81744b6842ddf615d7fca0e14a23b09734be63d44c23452de95631",
		"4974416255391052161ba8184fe652f3bf8c915592c65f7de127af8e637dce5d",
	}
	addr := libcommon.HexToAddress("0x920fd5070602feaea2e251e9e7238b6c376bcae5")
	offset := libcommon.Hex2Bytes("30")
	b := hexutility.Bytes(offset)
	result, err := api.ListStorageKeys(context.Background(), addr, 2, &b, latestBlock)
	if err != nil {
		t.Errorf("calling ListStorageKeys: %v", err)
	}
	assert.Equal(len(answers), len(result))
	for k, v := range result {
		assert.Equal(answers[k], common.Bytes2Hex(v))
	}
}

func TestParityAPIImpl_ListStorageKeys_WithOffset_EmptyResponse(t *testing.T) {
	assert := assert.New(t)
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewParityAPIImpl(newBaseApiForTest(m), m.DB)
	addr := libcommon.HexToAddress("0x920fd5070602feaea2e251e9e7238b6c376bcae5")
	offset := libcommon.Hex2Bytes("ff")
	b := hexutility.Bytes(offset)
	result, err := api.ListStorageKeys(context.Background(), addr, 2, &b, latestBlock)
	if err != nil {
		t.Errorf("calling ListStorageKeys: %v", err)
	}
	assert.Equal(0, len(result))
}

func TestParityAPIImpl_ListStorageKeys_AccNotFound(t *testing.T) {
	assert := assert.New(t)
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewParityAPIImpl(newBaseApiForTest(m), m.DB)
	addr := libcommon.HexToAddress("0x920fd5070602feaea2e251e9e7238b6c376bcaef")
	_, err := api.ListStorageKeys(context.Background(), addr, 2, nil, latestBlock)
	assert.Error(err, errors.New("acc not found"))
}
