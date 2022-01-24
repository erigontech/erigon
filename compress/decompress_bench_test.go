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

package compress

import (
	"testing"
)

func BenchmarkDecompressNext(b *testing.B) {
	t := new(testing.T)
	d := prepareDict(t)
	defer d.Close()
	g := d.MakeGetter()
	for i := 0; i < b.N; i++ {
		_, _ = g.Next(nil)
		if !g.HasNext() {
			g.Reset(0)
		}
	}
}

func BenchmarkDecompressSkip(b *testing.B) {
	t := new(testing.T)
	d := prepareDict(t)
	defer d.Close()
	g := d.MakeGetter()

	for i := 0; i < b.N; i++ {
		_ = g.Skip()
		if !g.HasNext() {
			g.Reset(0)
		}
	}
}

func BenchmarkDecompressMatch(b *testing.B) {
	t := new(testing.T)
	d := prepareDict(t)
	defer d.Close()
	g := d.MakeGetter()
	for i := 0; i < b.N; i++ {
		_, _ = g.Match([]byte("longlongword"))
	}
}

func BenchmarkDecompressMatchPrefix(b *testing.B) {
	t := new(testing.T)
	d := prepareDict(t)
	defer d.Close()
	g := d.MakeGetter()

	for i := 0; i < b.N; i++ {
		_ = g.MatchPrefix([]byte("longlongword"))
	}
}
