// +build gofuzzbeta

package txpool

import (
	"testing"
)

// https://blog.golang.org/fuzz-beta
// golang.org/s/draft-fuzzing-design
//gotip doc testing
//gotip doc testing.F
//gotip doc testing.F.Add
//gotip doc testing.F.Fuzz

//func FuzzParseQuery(f *testing.F) {
//	f.Add("x=1&y=2")
//	f.Fuzz(func(t *testing.T, queryStr string) {
//		query, err := url.ParseQuery(queryStr)
//		if err != nil {
//			t.Skip()
//		}
//		queryStr2 := query.Encode()
//		query2, err := url.ParseQuery(queryStr2)
//		if err != nil {
//			t.Fatalf("ParseQuery failed to decode a valid encoded query %s: %v", queryStr2, err)
//		}
//		if !reflect.DeepEqual(query, query2) {
//			t.Errorf("ParseQuery gave different query after being encoded\nbefore: %v\nafter: %v", query, query2)
//		}
//	})
//}

func FuzzPromoteStep(f *testing.F) {
	f.Add([]uint8{0b11111, 0b10001, 0b10101, 0b00001, 0b00000}, []uint8{0b11111, 0b10001, 0b10101, 0b00001, 0b00000}, []uint8{0b11111, 0b10001, 0b10101, 0b00001, 0b00000})
	f.Fuzz(func(t *testing.T, s1, s2, s3 []uint8) {
		t.Parallel()
		pending := NewSubPool()
		for i := range s1 {
			s1[i] &= 0b11111
			pending.Add(&MetaTx{SubPool: SubPoolMarker(s1[i])})
		}
		baseFee := NewSubPool()
		for i := range s2 {
			s2[i] &= 0b11111
			baseFee.Add(&MetaTx{SubPool: SubPoolMarker(s2[i])})
		}
		queue := NewSubPool()
		for i := range s3 {
			s3[i] &= 0b11111
			queue.Add(&MetaTx{SubPool: SubPoolMarker(s3[i])})
		}
		PromoteStep(pending, baseFee, queue)

		if pending.Best() != nil && pending.Best().SubPool < 0b11110 {
			t.Fatalf("Pending best too small %b", pending.Best().SubPool)
		}
	})
}
