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
	"fmt"
	"path"
	"testing"
)

func TestCompressEmptyDict(t *testing.T) {
	tmpDir := t.TempDir()
	file := path.Join(tmpDir, "compressed")
	c, err := NewCompressor(t.Name(), file, tmpDir, 100)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.AddWord([]byte("word")); err != nil {
		t.Fatal(err)
	}
	if err = c.Compress(); err != nil {
		t.Fatal(err)
	}
	var d *Decompressor
	if d, err = NewDecompressor(file); err != nil {
		t.Fatal(err)
	}
	g := d.MakeGetter()
	if !g.HasNext() {
		t.Fatalf("expected a word")
	}
	word, _ := g.Next(nil)
	if string(word) != "word" {
		t.Fatalf("expeced word, got (hex) %x", word)
	}
	if g.HasNext() {
		t.Fatalf("not expecting anything else")
	}
	defer d.Close()
}

func TestCompressDict1(t *testing.T) {
	tmpDir := t.TempDir()
	file := path.Join(tmpDir, t.Name())
	t.Name()
	c, err := NewCompressor(t.Name(), file, tmpDir, 1)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if err = c.AddWord([]byte(fmt.Sprintf("longlongword %d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err = c.Compress(); err != nil {
		t.Fatal(err)
	}
	var d *Decompressor
	if d, err = NewDecompressor(file); err != nil {
		t.Fatal(err)
	}
	g := d.MakeGetter()
	i := 0
	for g.HasNext() {
		word, _ := g.Next(nil)
		expected := fmt.Sprintf("longlongword %d", i)
		if string(word) != expected {
			t.Errorf("expected %s, got (hex) %x", expected, word)
		}

		i++
	}
	g.Reset(0)
	word, a := g.Current(nil)
	fmt.Printf("a:%d\n", a)
	expected := fmt.Sprintf("longlongword %d", 0)
	if string(word) != expected {
		t.Errorf("expected %s, got (hex) %x", expected, word)
	}

	defer d.Close()
}
