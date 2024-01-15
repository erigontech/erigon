package sync

import (
	"bytes"
	"errors"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/core/types"
	heimdallspan "github.com/ledgerwatch/erigon/polygon/heimdall"
)

type testDifficultyCalculator struct {
}

func (*testDifficultyCalculator) HeaderDifficulty(header *types.Header) (uint64, error) {
	if header.Difficulty == nil {
		return 0, errors.New("unset header.Difficulty")
	}
	return header.Difficulty.Uint64(), nil
}

func (*testDifficultyCalculator) SetSpan(*heimdallspan.HeimdallSpan) {}

func makeRoot() *types.Header {
	return &types.Header{
		Number: big.NewInt(0),
	}
}

func makeCCB(root *types.Header) CanonicalChainBuilder {
	difficultyCalc := testDifficultyCalculator{}
	builder := NewCanonicalChainBuilder(root, &difficultyCalc, nil, nil)
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
		Difficulty: big.NewInt(int64(difficulty)),
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
	headers []*types.Header,
	expectedTip *types.Header,
	expectedHeaders []*types.Header,
) {
	t := test.t
	builder := test.builder

	err := builder.Connect(headers)
	require.Nil(t, err)

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
	test, root := newConnectCCBTest(t)

	tip := test.builder.Tip()
	assert.Equal(t, root.Hash(), tip.Hash())

	headers := test.builder.HeadersInRange(0, 1)
	require.Equal(t, 1, len(headers))
	assert.Equal(t, root.Hash(), headers[0].Hash())
}

func TestCCBConnectEmpty(t *testing.T) {
	test, root := newConnectCCBTest(t)
	test.testConnect([]*types.Header{}, root, []*types.Header{root})
}

// connect 0 to 0
func TestCCBConnectRoot(t *testing.T) {
	test, root := newConnectCCBTest(t)
	test.testConnect([]*types.Header{root}, root, []*types.Header{root})
}

// connect 1 to 0
func TestCCBConnectOneToRoot(t *testing.T) {
	test, root := newConnectCCBTest(t)
	newTip := test.makeHeader(root, 1)
	test.testConnect([]*types.Header{newTip}, newTip, []*types.Header{root, newTip})
}

// connect 1-2-3 to 0
func TestCCBConnectSomeToRoot(t *testing.T) {
	test, root := newConnectCCBTest(t)
	headers := test.makeHeaders(root, []uint64{1, 2, 3})
	test.testConnect(headers, headers[len(headers)-1], append([]*types.Header{root}, headers...))
}

// connect any subset of 0-1-2-3 to 0-1-2-3
func TestCCBConnectOverlapsFull(t *testing.T) {
	test, root := newConnectCCBTest(t)
	headers := test.makeHeaders(root, []uint64{1, 2, 3})
	require.Nil(t, test.builder.Connect(headers))

	expectedTip := headers[len(headers)-1]
	expectedHeaders := append([]*types.Header{root}, headers...)

	for subsetLen := 1; subsetLen <= len(headers); subsetLen++ {
		for i := 0; i+subsetLen-1 < len(expectedHeaders); i++ {
			headers := expectedHeaders[i : i+subsetLen]
			test.testConnect(headers, expectedTip, expectedHeaders)
		}
	}
}

// connect 0-1 to 0
func TestCCBConnectOverlapPartialOne(t *testing.T) {
	test, root := newConnectCCBTest(t)
	newTip := test.makeHeader(root, 1)
	test.testConnect([]*types.Header{root, newTip}, newTip, []*types.Header{root, newTip})
}

// connect 2-3-4-5 to 0-1-2-3
func TestCCBConnectOverlapPartialSome(t *testing.T) {
	test, root := newConnectCCBTest(t)
	headers := test.makeHeaders(root, []uint64{1, 2, 3})
	require.Nil(t, test.builder.Connect(headers))

	overlapHeaders := append(headers[1:], test.makeHeaders(headers[len(headers)-1], []uint64{4, 5})...)
	expectedTip := overlapHeaders[len(overlapHeaders)-1]
	expectedHeaders := append([]*types.Header{root, headers[0]}, overlapHeaders...)
	test.testConnect(overlapHeaders, expectedTip, expectedHeaders)
}

// connect 2 to 0-1 at 0, then connect 10 to 0-1
func TestCCBConnectAltMainBecomesFork(t *testing.T) {
	test, root := newConnectCCBTest(t)
	header1 := test.makeHeader(root, 1)
	header2 := test.makeHeader(root, 2)
	require.Nil(t, test.builder.Connect([]*types.Header{header1}))

	// the tip changes to header2
	test.testConnect([]*types.Header{header2}, header2, []*types.Header{root, header2})

	header10 := test.makeHeader(header1, 10)
	test.testConnect([]*types.Header{header10}, header10, []*types.Header{root, header1, header10})
}

// connect 1 to 0-2 at 0, then connect 10 to 0-1
func TestCCBConnectAltForkBecomesMain(t *testing.T) {
	test, root := newConnectCCBTest(t)
	header1 := test.makeHeader(root, 1)
	header2 := test.makeHeader(root, 2)
	require.Nil(t, test.builder.Connect([]*types.Header{header2}))

	// the tip stays at header2
	test.testConnect([]*types.Header{header1}, header2, []*types.Header{root, header2})

	header10 := test.makeHeader(header1, 10)
	test.testConnect([]*types.Header{header10}, header10, []*types.Header{root, header1, header10})
}

// connect 10 and 11 to 1, then 20 and 22 to 2 one by one starting from a [0-1, 0-2] tree
func TestCCBConnectAltForksAtLevel2(t *testing.T) {
	test, root := newConnectCCBTest(t)
	header1 := test.makeHeader(root, 1)
	header10 := test.makeHeader(header1, 10)
	header11 := test.makeHeader(header1, 11)
	header2 := test.makeHeader(root, 2)
	header20 := test.makeHeader(header2, 20)
	header22 := test.makeHeader(header2, 22)
	require.Nil(t, test.builder.Connect([]*types.Header{header1}))
	require.Nil(t, test.builder.Connect([]*types.Header{header2}))

	test.testConnect([]*types.Header{header10}, header10, []*types.Header{root, header1, header10})
	test.testConnect([]*types.Header{header11}, header11, []*types.Header{root, header1, header11})
	test.testConnect([]*types.Header{header20}, header20, []*types.Header{root, header2, header20})
	test.testConnect([]*types.Header{header22}, header22, []*types.Header{root, header2, header22})
}

// connect 11 and 10 to 1, then 22 and 20 to 2 one by one starting from a [0-1, 0-2] tree
// then connect 100 to 10, and 200 to 20
func TestCCBConnectAltForksAtLevel2Reverse(t *testing.T) {
	test, root := newConnectCCBTest(t)
	header1 := test.makeHeader(root, 1)
	header10 := test.makeHeader(header1, 10)
	header11 := test.makeHeader(header1, 11)
	header2 := test.makeHeader(root, 2)
	header20 := test.makeHeader(header2, 20)
	header22 := test.makeHeader(header2, 22)
	header100 := test.makeHeader(header10, 100)
	header200 := test.makeHeader(header20, 200)
	require.Nil(t, test.builder.Connect([]*types.Header{header1}))
	require.Nil(t, test.builder.Connect([]*types.Header{header2}))

	test.testConnect([]*types.Header{header11}, header11, []*types.Header{root, header1, header11})
	test.testConnect([]*types.Header{header10}, header11, []*types.Header{root, header1, header11})
	test.testConnect([]*types.Header{header22}, header22, []*types.Header{root, header2, header22})
	test.testConnect([]*types.Header{header20}, header22, []*types.Header{root, header2, header22})

	test.testConnect([]*types.Header{header100}, header100, []*types.Header{root, header1, header10, header100})
	test.testConnect([]*types.Header{header200}, header200, []*types.Header{root, header2, header20, header200})
}
