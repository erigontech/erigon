//go:build go1.27

package native

import (
	"encoding/json"
	"math/big"
	"reflect"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/stretchr/testify/require"
)

func mkFrame(depth, width int) callFrame {
	f := callFrame{
		Type: vm.CALL, From: common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
		Gas: 21000, GasUsed: 12345, To: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Input: make([]byte, 128), Output: make([]byte, 64), Value: big.NewInt(1e18),
		Logs: []callLog{{Index: 1, Address: common.HexToAddress("0x2222222222222222222222222222222222222222"),
			Topics: []common.Hash{common.HexToHash("0xaa"), {}}, Data: make([]byte, 96), Position: 0}},
	}
	if depth > 0 {
		for i := 0; i < width; i++ {
			f.Calls = append(f.Calls, mkFrame(depth-1, width))
		}
	}
	return f
}

// fillNonZero sets every field of v to a non-zero value, recursing into structs
// and allocating one element for slices and pointers. A field added to callFrame
// is therefore populated automatically, without anyone remembering to extend a
// fixture.
func fillNonZero(t *testing.T, v reflect.Value, depth int) {
	t.Helper()
	switch v.Kind() {
	case reflect.Struct:
		for i := range v.NumField() {
			if v.Type().Field(i).IsExported() {
				fillNonZero(t, v.Field(i), depth)
			}
		}
	case reflect.Slice:
		if depth <= 0 { // callFrame.Calls recurses; stop before it blows the stack
			return
		}
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		fillNonZero(t, v.Index(0), depth-1)
	case reflect.Pointer:
		if depth <= 0 {
			return
		}
		v.Set(reflect.New(v.Type().Elem()))
		fillNonZero(t, v.Elem(), depth-1)
	case reflect.Array:
		for i := range v.Len() {
			fillNonZero(t, v.Index(i), depth)
		}
	case reflect.String:
		v.SetString("x")
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(7)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(7)
	case reflect.Bool:
		v.SetBool(true)
	}
}

// MarshalJSONTo must be byte-identical to the generated MarshalJSON. The frame is
// filled by reflection rather than by hand, so adding a field to callFrame without
// teaching MarshalJSONTo about it fails here instead of silently changing the RPC
// output under go1.27.
func TestCallFrameMarshalJSONToMatchesGenerated(t *testing.T) {
	var filled callFrame
	fillNonZero(t, reflect.ValueOf(&filled).Elem(), 2)

	for name, f := range map[string]callFrame{
		"every-field-set": filled,
		"zero":            {},
		"revert":          {Error: "execution reverted", Revertal: "boom", Output: []byte{1, 2}},
		"deep":            mkFrame(3, 2),
	} {
		t.Run(name, func(t *testing.T) {
			want, err := f.MarshalJSON()
			require.NoError(t, err)
			got, err := json.Marshal(f)
			require.NoError(t, err)
			require.Equal(t, string(want), string(got))
		})
	}
}

// Every exported field of callFrame must be reachable by the reflection fill, so
// the guard above cannot be defeated by a field kind it silently skips.
func TestFillNonZeroTouchesEveryCallFrameField(t *testing.T) {
	var filled callFrame
	fillNonZero(t, reflect.ValueOf(&filled).Elem(), 2)
	rv := reflect.ValueOf(filled)
	for i := range rv.NumField() {
		f := rv.Type().Field(i)
		if !f.IsExported() {
			continue
		}
		require.False(t, rv.Field(i).IsZero(), "field %s left zero; fillNonZero needs to handle %s", f.Name, f.Type)
	}
}

func BenchmarkCallFrameMarshalJSONTo(b *testing.B) {
	f := mkFrame(5, 3) // 364 frames, ~ a real trace tree
	raw, _ := json.Marshal(f)
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := json.Marshal(f); err != nil {
			b.Fatal(err)
		}
	}
}
