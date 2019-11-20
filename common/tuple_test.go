package common

import (
	"fmt"
	"reflect"
	"sort"
	"testing"
)

type value struct {
	bucket string
	key    string
	value  []byte
}

func Test2Tuple(t *testing.T) {
	t.Parallel()

	tests := []struct {
		putsBucket []value
		expected   [][]byte
	}{
		{
			[]value{
				{
					"bucket",
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
			[]value{
				{
					"bucket",
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
			[]value{
				{
					"bucket",
					"0001",
					[]byte{1, 2, 3, 4},
				},
				{
					"bucket",
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"bucket",
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"bucket",
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
			[]value{
				{
					"bucket",
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"bucket",
					"0001",
					[]byte{1, 2, 3, 4},
				},
				{
					"bucket",
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"bucket",
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
			[]value{
				{
					"bucket",
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"bucket",
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"bucket",
					"0001",
					[]byte{1, 2, 3, 4},
				},
				{
					"bucket",
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
			[]value{
				{
					"bucket",
					"0004",
					[]byte{4, 2, 1, 3},
				},
				{
					"bucket",
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"bucket",
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"bucket",
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

		{
			[]value{
				{
					"bucket1",
					"0001",
					[]byte{1, 2, 3, 4},
				},
				{
					"bucket2",
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"bucket3",
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"bucket4",
					"0004",
					[]byte{4, 2, 1, 3},
				},
			},
			[][]byte{
				[]byte("bucket1"),
				[]byte("0001"),
				[]byte("bucket2"),
				[]byte("0002"),
				[]byte("bucket3"),
				[]byte("0003"),
				[]byte("bucket4"),
				[]byte("0004"),
			},
		},
		{
			[]value{
				{
					"bucket2",
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"bucket1",
					"0001",
					[]byte{1, 2, 3, 4},
				},
				{
					"bucket3",
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"bucket4",
					"0004",
					[]byte{4, 2, 1, 3},
				},
			},
			[][]byte{
				[]byte("bucket1"),
				[]byte("0001"),
				[]byte("bucket2"),
				[]byte("0002"),
				[]byte("bucket3"),
				[]byte("0003"),
				[]byte("bucket4"),
				[]byte("0004"),
			},
		},
		{
			[]value{
				{
					"bucket2",
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"bucket3",
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"bucket1",
					"0001",
					[]byte{1, 2, 3, 4},
				},
				{
					"bucket4",
					"0004",
					[]byte{4, 2, 1, 3},
				},
			},
			[][]byte{
				[]byte("bucket1"),
				[]byte("0001"),
				[]byte("bucket2"),
				[]byte("0002"),
				[]byte("bucket3"),
				[]byte("0003"),
				[]byte("bucket4"),
				[]byte("0004"),
			},
		},
		{
			[]value{
				{
					"bucket4",
					"0004",
					[]byte{4, 2, 1, 3},
				},
				{
					"bucket3",
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"bucket2",
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"bucket1",
					"0001",
					[]byte{1, 2, 3, 4},
				},
			},
			[][]byte{
				[]byte("bucket1"),
				[]byte("0001"),
				[]byte("bucket2"),
				[]byte("0002"),
				[]byte("bucket3"),
				[]byte("0003"),
				[]byte("bucket4"),
				[]byte("0004"),
			},
		},
	}

	for i, test := range tests {
		test := test
		t.Run(fmt.Sprintf("testcase %d", i), func(t *testing.T) {
			tuples := NewTuples(len(test.putsBucket), 2, 1)
			for _, value := range test.putsBucket {
				if err := tuples.Append([]byte(value.bucket), []byte(value.key)); err != nil {
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
		putsBucket []value
		expected   [][]byte
	}{
		{
			[]value{
				{
					"bucket",
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
			[]value{
				{
					"bucket",
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
			[]value{
				{
					"bucket",
					"0001",
					[]byte{1, 2, 3, 4},
				},
				{
					"bucket",
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"bucket",
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"bucket",
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
			[]value{
				{
					"bucket",
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"bucket",
					"0001",
					[]byte{1, 2, 3, 4},
				},
				{
					"bucket",
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"bucket",
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
			[]value{
				{
					"bucket",
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"bucket",
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"bucket",
					"0001",
					[]byte{1, 2, 3, 4},
				},
				{
					"bucket",
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
			[]value{
				{
					"bucket",
					"0004",
					[]byte{4, 2, 1, 3},
				},
				{
					"bucket",
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"bucket",
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"bucket",
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

		{
			[]value{
				{
					"bucket1",
					"0001",
					[]byte{1, 2, 3, 4},
				},
				{
					"bucket2",
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"bucket3",
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"bucket4",
					"0004",
					[]byte{4, 2, 1, 3},
				},
			},
			[][]byte{
				[]byte("bucket1"),
				[]byte("0001"),
				{1, 2, 3, 4},
				[]byte("bucket2"),
				[]byte("0002"),
				{2, 3, 4, 1},
				[]byte("bucket3"),
				[]byte("0003"),
				{3, 4, 2, 1},
				[]byte("bucket4"),
				[]byte("0004"),
				{4, 2, 1, 3},
			},
		},
		{
			[]value{
				{
					"bucket2",
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"bucket1",
					"0001",
					[]byte{1, 2, 3, 4},
				},
				{
					"bucket3",
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"bucket4",
					"0004",
					[]byte{4, 2, 1, 3},
				},
			},
			[][]byte{
				[]byte("bucket1"),
				[]byte("0001"),
				{1, 2, 3, 4},
				[]byte("bucket2"),
				[]byte("0002"),
				{2, 3, 4, 1},
				[]byte("bucket3"),
				[]byte("0003"),
				{3, 4, 2, 1},
				[]byte("bucket4"),
				[]byte("0004"),
				{4, 2, 1, 3},
			},
		},
		{
			[]value{
				{
					"bucket2",
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"bucket3",
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"bucket1",
					"0001",
					[]byte{1, 2, 3, 4},
				},
				{
					"bucket4",
					"0004",
					[]byte{4, 2, 1, 3},
				},
			},
			[][]byte{
				[]byte("bucket1"),
				[]byte("0001"),
				{1, 2, 3, 4},
				[]byte("bucket2"),
				[]byte("0002"),
				{2, 3, 4, 1},
				[]byte("bucket3"),
				[]byte("0003"),
				{3, 4, 2, 1},
				[]byte("bucket4"),
				[]byte("0004"),
				{4, 2, 1, 3},
			},
		},
		{
			[]value{
				{
					"bucket4",
					"0004",
					[]byte{4, 2, 1, 3},
				},
				{
					"bucket3",
					"0003",
					[]byte{3, 4, 2, 1},
				},
				{
					"bucket2",
					"0002",
					[]byte{2, 3, 4, 1},
				},
				{
					"bucket1",
					"0001",
					[]byte{1, 2, 3, 4},
				},
			},
			[][]byte{
				[]byte("bucket1"),
				[]byte("0001"),
				{1, 2, 3, 4},
				[]byte("bucket2"),
				[]byte("0002"),
				{2, 3, 4, 1},
				[]byte("bucket3"),
				[]byte("0003"),
				{3, 4, 2, 1},
				[]byte("bucket4"),
				[]byte("0004"),
				{4, 2, 1, 3},
			},
		},
	}

	for i, test := range tests {
		test := test
		t.Run(fmt.Sprintf("testcase %d", i), func(t *testing.T) {
			tuples := NewTuples(len(test.putsBucket), 3, 1)
			for _, value := range test.putsBucket {
				if err := tuples.Append([]byte(value.bucket), []byte(value.key), value.value); err != nil {
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
