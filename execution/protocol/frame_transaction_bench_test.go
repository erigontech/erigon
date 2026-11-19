// Copyright 2026 The Erigon Authors
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

package protocol

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
)

func BenchmarkFrameTransactionLogs(b *testing.B) {
	for _, frames := range []int{1, 16, 63} {
		b.Run(fmt.Sprint(frames), func(b *testing.B) {
			evm, tx := frameTestEnv(b)
			ibs := evm.IntraBlockState()
			target := common.HexToAddress("0x2000")
			require.NoError(b, ibs.SetCode(accounts.InternAddress(tx.SenderAddress), program.New().Push(3).Push0().Push0().Op(vm.APPROVE).Bytes(), tracing.CodeChangeUnspecified))
			code := program.New()
			for range 16 {
				code.Push(1024).Push0().Op(vm.LOG0)
			}
			require.NoError(b, ibs.SetCode(accounts.InternAddress(target), code.Bytes(), tracing.CodeChangeUnspecified))
			tx.Frames = []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 1_000}}}
			for range frames {
				tx.Frames = append(tx.Frames, types.Frame{Mode: types.FrameSender, Target: &target, Limits: mdgas.MdGas{Execution: 150_000}})
			}
			b.ReportAllocs()
			for b.Loop() {
				snapshot := ibs.PushSnapshot()
				result, err := applyFrameTest(b, evm, tx)
				require.NoError(b, err)
				require.NoError(b, result.Err)
				require.Len(b, result.FrameReceipts[len(tx.Frames)-1].Logs, 16)
				ibs.RevertToSnapshot(snapshot, nil)
				ibs.PopSnapshot(snapshot)
			}
		})
	}
}
