/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package recsplit

import (
	"testing"
)

func TestRecSplit2(t *testing.T) {
	rs, err := NewRecSplit(RecSplitArgs{
		KeyCount:   2,
		BucketSize: 10,
		Salt:       0,
		TmpDir:     t.TempDir(),
		LeafSize:   8,
		StartSeed:  []uint32{5, 100034, 405060, 60606},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = rs.AddKey([]byte("first_key")); err != nil {
		t.Error(err)
	}
	if err = rs.Build(); err == nil {
		t.Errorf("test is expected to fail, too few keys added")
	}
	if err = rs.AddKey([]byte("second_key")); err != nil {
		t.Error(err)
	}
	if err = rs.Build(); err != nil {
		t.Error(err)
	}
	if err = rs.Build(); err == nil {
		t.Errorf("test is expected to fail, hash gunction was built already")
	}
	if err = rs.AddKey([]byte("key_to_fail")); err == nil {
		t.Errorf("test is expected to fail, hash function was built")
	}
}

func TestRecSplitDuplicate(t *testing.T) {
	rs, err := NewRecSplit(RecSplitArgs{
		KeyCount:   2,
		BucketSize: 10,
		Salt:       0,
		TmpDir:     t.TempDir(),
		LeafSize:   8,
		StartSeed:  []uint32{5, 100034, 405060, 60606},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := rs.AddKey([]byte("first_key")); err != nil {
		t.Error(err)
	}
	if err := rs.AddKey([]byte("first_key")); err != nil {
		t.Error(err)
	}
	if err := rs.Build(); err == nil {
		t.Errorf("test is expected to fail, duplicate key")
	}
}

func TestRecSplitLeafSizeTooLarge(t *testing.T) {
	_, err := NewRecSplit(RecSplitArgs{
		KeyCount:   2,
		BucketSize: 10,
		Salt:       0,
		TmpDir:     t.TempDir(),
		LeafSize:   64,
		StartSeed:  []uint32{5, 100034, 405060, 60606},
	})
	if err == nil {
		t.Errorf("test is expected to fail, leaf size too large")
	}
}
