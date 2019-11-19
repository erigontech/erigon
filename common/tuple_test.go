package common

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
)

/*
type puts map[string]putsBucket   //map[bucket]putsBucket
type putsBucket map[string][]byte //map[key]value
*/

type value struct {
	key   string
	value []byte
}

func Test2Tuple(t *testing.T) {
	t.Parallel()

	tests := []struct {
		bucket     string
		putsBucket []value
		expected   [][]byte
	}{
		{
			"bucket",
			[]value{
				{
					"",
					nil,
				},
			},
			[][]byte{
				[]byte("bucket"),
				{},
			},
		},
		{
			"bucket",
			[]value{
				{
					"0001",
					[]byte{1, 2, 3, 4},
				},
			},
			[][]byte{
				[]byte("bucket"),
				[]byte("0001"),
			},
		},
		{
			"bucket",
			[]value{
				{
					"0001",
					[]byte{1, 2, 3, 4},
				},
				{
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"0004",
					[]byte{4, 2, 1, 3},
				},
			},
			[][]byte{
				[]byte("bucket"),
				[]byte("0001"),
				[]byte("bucket"),
				[]byte("0002"),
				[]byte("bucket"),
				[]byte("0003"),
				[]byte("bucket"),
				[]byte("0004"),
			},
		},
		{
			"bucket",
			[]value{
				{
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"0001",
					[]byte{1, 2, 3, 4},
				},
				{
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"0004",
					[]byte{4, 2, 1, 3},
				},
			},
			[][]byte{
				[]byte("bucket"),
				[]byte("0001"),
				[]byte("bucket"),
				[]byte("0002"),
				[]byte("bucket"),
				[]byte("0003"),
				[]byte("bucket"),
				[]byte("0004"),
			},
		},
		{
			"bucket",
			[]value{
				{
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"0001",
					[]byte{1, 2, 3, 4},
				},
				{
					"0004",
					[]byte{4, 2, 1, 3},
				},
			},
			[][]byte{
				[]byte("bucket"),
				[]byte("0001"),
				[]byte("bucket"),
				[]byte("0002"),
				[]byte("bucket"),
				[]byte("0003"),
				[]byte("bucket"),
				[]byte("0004"),
			},
		},
		{
			"bucket",
			[]value{
				{
					"0004",
					[]byte{4, 2, 1, 3},
				},

				{
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"0001",
					[]byte{1, 2, 3, 4},
				},
			},
			[][]byte{
				[]byte("bucket"),
				[]byte("0001"),
				[]byte("bucket"),
				[]byte("0002"),
				[]byte("bucket"),
				[]byte("0003"),
				[]byte("bucket"),
				[]byte("0004"),
			},
		},
	}

	for i, test := range tests {
		test := test
		t.Run(fmt.Sprintf("testcase %d", i), func(t *testing.T) {
			tuples := NewTuples(len(test.putsBucket), 2, 1)
			bucketB := []byte(test.bucket)
			for _, value := range test.putsBucket {
				if err := tuples.Append(bucketB, []byte(value.key)); err != nil {
					t.Fatal(err)
				}
			}

			sort.Sort(tuples)

			if !reflect.DeepEqual(tuples.Values, test.expected) {
				t.Fatalf("expected %v\ngot %v", test.expected, tuples.Values)
			}
		})
	}
}

func Test3Tuple(t *testing.T) {
	t.Parallel()

	tests := []struct {
		bucket     string
		putsBucket []value
		expected   [][]byte
	}{
		{
			"bucket",
			[]value{
				{
					"",
					nil,
				},
			},
			[][]byte{
				[]byte("bucket"),
				{},
				nil,
			},
		},
		{
			"bucket",
			[]value{
				{
					"0001",
					[]byte{1, 2, 3, 4},
				},
			},
			[][]byte{
				[]byte("bucket"),
				[]byte("0001"),
				{1, 2, 3, 4},
			},
		},
		{
			"bucket",
			[]value{
				{
					"0001",
					[]byte{1, 2, 3, 4},
				},
				{
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"0004",
					[]byte{4, 2, 1, 3},
				},
			},
			[][]byte{
				[]byte("bucket"),
				[]byte("0001"),
				{1, 2, 3, 4},
				[]byte("bucket"),
				[]byte("0002"),
				{2, 3, 4, 1},
				[]byte("bucket"),
				[]byte("0003"),
				{3, 4, 2, 1},
				[]byte("bucket"),
				[]byte("0004"),
				{4, 2, 1, 3},
			},
		},
		{
			"bucket",
			[]value{
				{
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"0001",
					[]byte{1, 2, 3, 4},
				},
				{
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"0004",
					[]byte{4, 2, 1, 3},
				},
			},
			[][]byte{
				[]byte("bucket"),
				[]byte("0001"),
				{1, 2, 3, 4},
				[]byte("bucket"),
				[]byte("0002"),
				{2, 3, 4, 1},
				[]byte("bucket"),
				[]byte("0003"),
				{3, 4, 2, 1},
				[]byte("bucket"),
				[]byte("0004"),
				{4, 2, 1, 3},
			},
		},
		{
			"bucket",
			[]value{
				{
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"0001",
					[]byte{1, 2, 3, 4},
				},
				{
					"0004",
					[]byte{4, 2, 1, 3},
				},
			},
			[][]byte{
				[]byte("bucket"),
				[]byte("0001"),
				{1, 2, 3, 4},
				[]byte("bucket"),
				[]byte("0002"),
				{2, 3, 4, 1},
				[]byte("bucket"),
				[]byte("0003"),
				{3, 4, 2, 1},
				[]byte("bucket"),
				[]byte("0004"),
				{4, 2, 1, 3},
			},
		},
		{
			"bucket",
			[]value{
				{
					"0004",
					[]byte{4, 2, 1, 3},
				},

				{
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"0001",
					[]byte{1, 2, 3, 4},
				},
			},
			[][]byte{
				[]byte("bucket"),
				[]byte("0001"),
				{1, 2, 3, 4},
				[]byte("bucket"),
				[]byte("0002"),
				{2, 3, 4, 1},
				[]byte("bucket"),
				[]byte("0003"),
				{3, 4, 2, 1},
				[]byte("bucket"),
				[]byte("0004"),
				{4, 2, 1, 3},
			},
		},
	}

	for i, test := range tests {
		test := test
		t.Run(fmt.Sprintf("testcase %d", i), func(t *testing.T) {
			tuples := NewTuples(len(test.putsBucket), 3, 1)
			bucketB := []byte(test.bucket)
			for _, value := range test.putsBucket {
				if err := tuples.Append(bucketB, []byte(value.key), value.value); err != nil {
					t.Fatal(err)
				}
			}

			sort.Sort(tuples)

			if !reflect.DeepEqual(tuples.Values, test.expected) {
				t.Fatalf("expected %v\ngot %v", test.expected, tuples.Values)
			}
		})
	}
}
