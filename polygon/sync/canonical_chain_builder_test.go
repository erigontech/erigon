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

package sync

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/v3/core/types"
)

type mockDifficultyCalculator struct{}

func (*mockDifficultyCalculator) HeaderDifficulty(_ context.Context, header *types.Header) (uint64, error) {
	if header.Difficulty == nil {
		return 0, errors.New("unset header.Difficulty")
	}
	return header.Difficulty.Uint64(), nil
}

type mockHeaderValidator struct{}

func (v *mockHeaderValidator) ValidateHeader(_ context.Context, _ *types.Header, _ *types.Header, _ time.Time) error {
	return nil
}

func makeRoot() *types.Header {
	return &types.Header{
		Number: big.NewInt(0),
	}
}

func makeCCB(root *types.Header) CanonicalChainBuilder {
	difficultyCalc := mockDifficultyCalculator{}
	builder := NewCanonicalChainBuilder(root, &difficultyCalc, &mockHeaderValidator{})
	return builder
}

type connectCCBTest struct {
	t       *testing.T
	root    *types.Header
	builder CanonicalChainBuilder

	currentHeaderTime uint64
}

func newConnectCCBTest(t *testing.T) (*connectCCBTest, *types.Header) {
	root := makeRoot()
	builder := makeCCB(root)
	test := &connectCCBTest{
		t:       t,
		root:    root,
		builder: builder,
	}
	return test, root
}

func (test *connectCCBTest) makeHeader(parent *types.Header, difficulty uint64) *types.Header {
	test.currentHeaderTime++
	return &types.Header{
		ParentHash: parent.Hash(),
		Difficulty: new(big.Int).SetUint64(difficulty),
		Number:     big.NewInt(parent.Number.Int64() + 1),
		Time:       test.currentHeaderTime,
		Extra:      bytes.Repeat([]byte{0x00}, types.ExtraVanityLength+types.ExtraSealLength),
	}
}

func (test *connectCCBTest) makeHeaders(parent *types.Header, difficulties []uint64) []*types.Header {
	count := len(difficulties)
	headers := make([]*types.Header, 0, count)
	for i := 0; i < count; i++ {
		header := test.makeHeader(parent, difficulties[i])
		headers = append(headers, header)
		parent = header
	}
	return headers
}

func (test *connectCCBTest) testConnect(
	ctx context.Context,
	headers []*types.Header,
	expectedTip *types.Header,
	expectedHeaders []*types.Header,
	expectedNewConnectedHeaders []*types.Header,
) {
	t := test.t
	builder := test.builder

	newConnectedHeaders, err := builder.Connect(ctx, headers)
	require.NoError(t, err)
	require.Equal(t, expectedNewConnectedHeaders, newConnectedHeaders)

	newTip := builder.Tip()
	assert.Equal(t, expectedTip.Hash(), newTip.Hash())

	require.NotNil(t, newTip.Number)
	count := uint64(len(expectedHeaders))
	start := newTip.Number.Uint64() - (count - 1)

	actualHeaders := builder.HeadersInRange(start, count)
	require.Equal(t, len(expectedHeaders), len(actualHeaders))
	for i, h := range actualHeaders {
		assert.Equal(t, expectedHeaders[i].Hash(), h.Hash())
	}
}

func TestCCBEmptyState(t *testing.T) {
	t.Parallel()
	test, root := newConnectCCBTest(t)

	tip := test.builder.Tip()
	assert.Equal(t, root.Hash(), tip.Hash())

	headers := test.builder.HeadersInRange(0, 1)
	require.Equal(t, 1, len(headers))
	assert.Equal(t, root.Hash(), headers[0].Hash())
}

func TestCCBConnectEmpty(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	test, root := newConnectCCBTest(t)
	test.testConnect(ctx, []*types.Header{}, root, []*types.Header{root}, nil)
}

// connect 0 to 0
func TestCCBConnectRoot(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	test, root := newConnectCCBTest(t)
	test.testConnect(ctx, []*types.Header{root}, root, []*types.Header{root}, nil)
}

// connect 1 to 0
func TestCCBConnectOneToRoot(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	test, root := newConnectCCBTest(t)
	newTip := test.makeHeader(root, 1)
	test.testConnect(ctx, []*types.Header{newTip}, newTip, []*types.Header{root, newTip}, []*types.Header{newTip})
}

// connect 1-2-3 to 0
func TestCCBConnectSomeToRoot(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	test, root := newConnectCCBTest(t)
	headers := test.makeHeaders(root, []uint64{1, 2, 3})
	test.testConnect(ctx, headers, headers[len(headers)-1], append([]*types.Header{root}, headers...), headers)
}

// connect any subset of 0-1-2-3 to 0-1-2-3
func TestCCBConnectOverlapsFull(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	test, root := newConnectCCBTest(t)
	headers := test.makeHeaders(root, []uint64{1, 2, 3})
	newConnectedHeaders, err := test.builder.Connect(ctx, headers)
	require.NoError(t, err)
	require.Equal(t, headers, newConnectedHeaders)

	expectedTip := headers[len(headers)-1]
	expectedHeaders := append([]*types.Header{root}, headers...)

	for subsetLen := 1; subsetLen <= len(headers); subsetLen++ {
		for i := 0; i+subsetLen-1 < len(expectedHeaders); i++ {
			headers := expectedHeaders[i : i+subsetLen]
			test.testConnect(ctx, headers, expectedTip, expectedHeaders, nil)
		}
	}
}

// connect 0-1 to 0
func TestCCBConnectOverlapPartialOne(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	test, root := newConnectCCBTest(t)
	newTip := test.makeHeader(root, 1)
	test.testConnect(ctx, []*types.Header{root, newTip}, newTip, []*types.Header{root, newTip}, []*types.Header{newTip})
}

// connect 2-3-4-5 to 0-1-2-3
func TestCCBConnectOverlapPartialSome(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	test, root := newConnectCCBTest(t)
	headers := test.makeHeaders(root, []uint64{1, 2, 3})
	newConnectedHeaders, err := test.builder.Connect(ctx, headers)
	require.NoError(t, err)
	require.Equal(t, headers, newConnectedHeaders)

	headers45 := test.makeHeaders(headers[len(headers)-1], []uint64{4, 5})
	overlapHeaders := append(headers[1:], headers45...)
	expectedTip := overlapHeaders[len(overlapHeaders)-1]
	expectedHeaders := append([]*types.Header{root, headers[0]}, overlapHeaders...)
	test.testConnect(ctx, overlapHeaders, expectedTip, expectedHeaders, headers45)
}

// connect 2 to 0-1 at 0, then connect 10 to 0-1
func TestCCBConnectAltMainBecomesFork(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	test, root := newConnectCCBTest(t)
	header1 := test.makeHeader(root, 1)
	header2 := test.makeHeader(root, 2)
	newConnectedHeaders, err := test.builder.Connect(ctx, []*types.Header{header1})
	require.NoError(t, err)
	require.Equal(t, []*types.Header{header1}, newConnectedHeaders)

	// the tip changes to header2
	test.testConnect(ctx, []*types.Header{header2}, header2, []*types.Header{root, header2}, []*types.Header{header2})

	header10 := test.makeHeader(header1, 10)
	test.testConnect(ctx, []*types.Header{header10}, header10, []*types.Header{root, header1, header10}, []*types.Header{header10})
}

// connect 1 to 0-2 at 0, then connect 10 to 0-1
func TestCCBConnectAltForkBecomesMain(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	test, root := newConnectCCBTest(t)
	header1 := test.makeHeader(root, 1)
	header2 := test.makeHeader(root, 2)
	newConnectedHeaders, err := test.builder.Connect(ctx, []*types.Header{header2})
	require.NoError(t, err)
	require.Equal(t, []*types.Header{header2}, newConnectedHeaders)

	// the tip stays at header2
	test.testConnect(ctx, []*types.Header{header1}, header2, []*types.Header{root, header2}, []*types.Header{header1})

	header10 := test.makeHeader(header1, 10)
	test.testConnect(ctx, []*types.Header{header10}, header10, []*types.Header{root, header1, header10}, []*types.Header{header10})
}

// connect 10 and 11 to 1, then 20 and 22 to 2 one by one starting from a [0-1, 0-2] tree
func TestCCBConnectAltForksAtLevel2(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	test, root := newConnectCCBTest(t)
	header1 := test.makeHeader(root, 1)
	header10 := test.makeHeader(header1, 10)
	header11 := test.makeHeader(header1, 11)
	header2 := test.makeHeader(root, 2)
	header20 := test.makeHeader(header2, 20)
	header22 := test.makeHeader(header2, 22)
	newConnectedHeaders, err := test.builder.Connect(ctx, []*types.Header{header1})
	require.NoError(t, err)
	require.Equal(t, []*types.Header{header1}, newConnectedHeaders)
	newConnectedHeaders, err = test.builder.Connect(ctx, []*types.Header{header2})
	require.NoError(t, err)
	require.Equal(t, []*types.Header{header2}, newConnectedHeaders)

	test.testConnect(ctx, []*types.Header{header10}, header10, []*types.Header{root, header1, header10}, []*types.Header{header10})
	test.testConnect(ctx, []*types.Header{header11}, header11, []*types.Header{root, header1, header11}, []*types.Header{header11})
	test.testConnect(ctx, []*types.Header{header20}, header20, []*types.Header{root, header2, header20}, []*types.Header{header20})
	test.testConnect(ctx, []*types.Header{header22}, header22, []*types.Header{root, header2, header22}, []*types.Header{header22})
}

// connect 11 and 10 to 1, then 22 and 20 to 2 one by one starting from a [0-1, 0-2] tree
// then connect 100 to 10, and 200 to 20
func TestCCBConnectAltForksAtLevel2Reverse(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	test, root := newConnectCCBTest(t)
	header1 := test.makeHeader(root, 1)
	header10 := test.makeHeader(header1, 10)
	header11 := test.makeHeader(header1, 11)
	header2 := test.makeHeader(root, 2)
	header20 := test.makeHeader(header2, 20)
	header22 := test.makeHeader(header2, 22)
	header100 := test.makeHeader(header10, 100)
	header200 := test.makeHeader(header20, 200)
	newConnectedHeaders, err := test.builder.Connect(ctx, []*types.Header{header1})
	require.NoError(t, err)
	require.Equal(t, []*types.Header{header1}, newConnectedHeaders)
	newConnectedHeaders, err = test.builder.Connect(ctx, []*types.Header{header2})
	require.NoError(t, err)
	require.Equal(t, []*types.Header{header2}, newConnectedHeaders)

	test.testConnect(ctx, []*types.Header{header11}, header11, []*types.Header{root, header1, header11}, []*types.Header{header11})
	test.testConnect(ctx, []*types.Header{header10}, header11, []*types.Header{root, header1, header11}, []*types.Header{header10})
	test.testConnect(ctx, []*types.Header{header22}, header22, []*types.Header{root, header2, header22}, []*types.Header{header22})
	test.testConnect(ctx, []*types.Header{header20}, header22, []*types.Header{root, header2, header22}, []*types.Header{header20})

	test.testConnect(ctx, []*types.Header{header100}, header100, []*types.Header{root, header1, header10, header100}, []*types.Header{header100})
	test.testConnect(ctx, []*types.Header{header200}, header200, []*types.Header{root, header2, header20, header200}, []*types.Header{header200})
}

func TestCCBPruneNode(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// R(td:0) -> A(td:1) -> B(td:2)
	// |
	// +--------> X(td:2) -> Y(td:4) -> Z(td:6)
	// |          |
	// |          +--------> K(td:5)
	// |
	// +--------> P(td:3)
	type example struct {
		ccb     CanonicalChainBuilder
		headerR *types.Header
		headerA *types.Header
		headerB *types.Header
		headerX *types.Header
		headerY *types.Header
		headerZ *types.Header
		headerK *types.Header
		headerP *types.Header
	}
	constructExample := func() example {
		test, headerR := newConnectCCBTest(t)
		ccb := test.builder
		headerA := test.makeHeader(headerR, 1)
		headerB := test.makeHeader(headerA, 1)
		_, err := ccb.Connect(ctx, []*types.Header{headerA, headerB})
		require.NoError(t, err)
		headerX := test.makeHeader(headerR, 2)
		headerY := test.makeHeader(headerX, 2)
		headerZ := test.makeHeader(headerY, 2)
		_, err = ccb.Connect(ctx, []*types.Header{headerX, headerY, headerZ})
		require.NoError(t, err)
		headerK := test.makeHeader(headerX, 3)
		_, err = ccb.Connect(ctx, []*types.Header{headerK})
		require.NoError(t, err)
		headerP := test.makeHeader(headerR, 3)
		_, err = ccb.Connect(ctx, []*types.Header{headerP})
		require.NoError(t, err)
		require.Equal(t, headerZ, ccb.Tip())
		return example{
			ccb:     ccb,
			headerR: headerR,
			headerA: headerA,
			headerB: headerB,
			headerX: headerX,
			headerY: headerY,
			headerZ: headerZ,
			headerK: headerK,
			headerP: headerP,
		}
	}
	t.Run("unknown hash", func(t *testing.T) {
		ex := constructExample()
		headerU := &types.Header{Number: big.NewInt(777)}
		err := ex.ccb.PruneNode(headerU.Hash())
		require.Error(t, err)
		require.Contains(t, err.Error(), "could not find node to prune")
	})
	t.Run("can't prune root", func(t *testing.T) {
		ex := constructExample()
		err := ex.ccb.PruneNode(ex.headerR.Hash())
		require.Error(t, err)
		require.Contains(t, err.Error(), "can't prune root node")
	})
	t.Run("prune Z - change of tip", func(t *testing.T) {
		ex := constructExample()
		err := ex.ccb.PruneNode(ex.headerZ.Hash())
		require.NoError(t, err)
		require.Equal(t, ex.headerK, ex.ccb.Tip())
	})
	t.Run("prune Y - change of tip", func(t *testing.T) {
		ex := constructExample()
		err := ex.ccb.PruneNode(ex.headerY.Hash())
		require.NoError(t, err)
		require.Equal(t, ex.headerK, ex.ccb.Tip())
	})
	t.Run("prune K - no change of tip", func(t *testing.T) {
		ex := constructExample()
		err := ex.ccb.PruneNode(ex.headerK.Hash())
		require.NoError(t, err)
		require.Equal(t, ex.headerZ, ex.ccb.Tip())
	})
	t.Run("prune X - no change of tip", func(t *testing.T) {
		ex := constructExample()
		err := ex.ccb.PruneNode(ex.headerX.Hash())
		require.NoError(t, err)
		require.Equal(t, ex.headerP, ex.ccb.Tip())
	})
	t.Run("prune P - no change of tip", func(t *testing.T) {
		ex := constructExample()
		err := ex.ccb.PruneNode(ex.headerP.Hash())
		require.NoError(t, err)
		require.Equal(t, ex.headerZ, ex.ccb.Tip())
	})
	t.Run("prune A - no change of tip", func(t *testing.T) {
		ex := constructExample()
		err := ex.ccb.PruneNode(ex.headerA.Hash())
		require.NoError(t, err)
		require.Equal(t, ex.headerZ, ex.ccb.Tip())
	})
	t.Run("prune P, prune Y, prune K, prune X, prune A", func(t *testing.T) {
		// prune P - no change (tip Z)
		ex := constructExample()
		err := ex.ccb.PruneNode(ex.headerP.Hash())
		require.NoError(t, err)
		require.Equal(t, ex.headerZ, ex.ccb.Tip())
		// prune Y - change (tip K)
		err = ex.ccb.PruneNode(ex.headerY.Hash())
		require.NoError(t, err)
		require.Equal(t, ex.headerK, ex.ccb.Tip())
		// prune K - change (tip X)
		err = ex.ccb.PruneNode(ex.headerK.Hash())
		require.NoError(t, err)
		require.Equal(t, ex.headerX, ex.ccb.Tip())
		// prune X - change (tip B)
		err = ex.ccb.PruneNode(ex.headerX.Hash())
		require.NoError(t, err)
		require.Equal(t, ex.headerB, ex.ccb.Tip())
		// prune A - change (tip R) - only root left
		err = ex.ccb.PruneNode(ex.headerA.Hash())
		require.NoError(t, err)
		require.Equal(t, ex.headerR, ex.ccb.Tip())
		require.Equal(t, ex.headerR, ex.ccb.Root())
	})
}

func TestCCBLowestCommonAncestor(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// R(td:0) -> A(td:1) -> B(td:2)
	// |
	// +--------> X(td:2) -> Y(td:4) -> Z(td:6)
	// |          |
	// |          +--------> K(td:5)
	// |
	// +--------> P(td:3)
	test, headerR := newConnectCCBTest(t)
	ccb := test.builder
	headerA := test.makeHeader(headerR, 1)
	headerB := test.makeHeader(headerA, 1)
	_, err := ccb.Connect(ctx, []*types.Header{headerA, headerB})
	require.NoError(t, err)
	headerX := test.makeHeader(headerR, 2)
	headerY := test.makeHeader(headerX, 2)
	headerZ := test.makeHeader(headerY, 2)
	_, err = ccb.Connect(ctx, []*types.Header{headerX, headerY, headerZ})
	require.NoError(t, err)
	headerK := test.makeHeader(headerX, 3)
	_, err = ccb.Connect(ctx, []*types.Header{headerK})
	require.NoError(t, err)
	headerP := test.makeHeader(headerR, 3)
	_, err = ccb.Connect(ctx, []*types.Header{headerP})
	require.NoError(t, err)
	require.Equal(t, headerZ, ccb.Tip())
	headerU := &types.Header{Number: big.NewInt(777)}
	headerU2 := &types.Header{Number: big.NewInt(999)}
	t.Run("LCA(R,U)=nil,false", func(t *testing.T) {
		assertLca(t, ccb, headerR, headerU, nil, false)
	})
	t.Run("LCA(U,R)=nil,false", func(t *testing.T) {
		assertLca(t, ccb, headerU, headerR, nil, false)
	})
	t.Run("LCA(U,U)=nil,false", func(t *testing.T) {
		assertLca(t, ccb, headerU, headerU, nil, false)
	})
	t.Run("LCA(U2,U)=nil,false", func(t *testing.T) {
		assertLca(t, ccb, headerU2, headerU, nil, false)
	})
	t.Run("LCA(R,R)=R", func(t *testing.T) {
		assertLca(t, ccb, headerR, headerR, headerR, true)
	})
	t.Run("LCA(Y,Y)=Y", func(t *testing.T) {
		assertLca(t, ccb, headerY, headerY, headerY, true)
	})
	t.Run("LCA(Y,Z)=Y", func(t *testing.T) {
		assertLca(t, ccb, headerY, headerZ, headerY, true)
	})
	t.Run("LCA(X,Y)=X", func(t *testing.T) {
		assertLca(t, ccb, headerX, headerY, headerX, true)
	})
	t.Run("LCA(R,Z)=R", func(t *testing.T) {
		assertLca(t, ccb, headerR, headerZ, headerR, true)
	})
	t.Run("LCA(R,A)=R", func(t *testing.T) {
		assertLca(t, ccb, headerR, headerA, headerR, true)
	})
	t.Run("LCA(R,P)=R", func(t *testing.T) {
		assertLca(t, ccb, headerR, headerP, headerR, true)
	})
	t.Run("LCA(K,B)=R", func(t *testing.T) {
		assertLca(t, ccb, headerK, headerB, headerR, true)
	})
	t.Run("LCA(X,A)=R", func(t *testing.T) {
		assertLca(t, ccb, headerX, headerA, headerR, true)
	})
	t.Run("LCA(Z,K)=X", func(t *testing.T) {
		assertLca(t, ccb, headerZ, headerK, headerX, true)
	})
}

func assertLca(t *testing.T, ccb CanonicalChainBuilder, a, b, wantLca *types.Header, wantOk bool) {
	lca, ok := ccb.LowestCommonAncestor(a.Hash(), b.Hash())
	require.Equal(t, wantOk, ok)
	require.Equal(t, wantLca, lca)
}
