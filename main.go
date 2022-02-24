package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/ledgerwatch/erigon-lib/compress"
)

const chars = "1234567890QWERTYUIOPASDFGHJKLZXCVBNMqwertyuiopasdfghjklzxcvbnm"
const max_size = 62

func rand_word() []byte {
	min, max := 30, 100
	word_len := rand.Intn(max-min) + min
	b := make([]byte, word_len)
	for i := 0; i < word_len; i++ {
		b[i] = byte(chars[rand.Intn(max_size)])
	}
	return b
}

func main() {

	// f1_name := "compressed_cgo"
	f2_name := "compressed_original"
	max, min := 10_000, 1_000

	stop_at := time.Now().Add(time.Hour * 5)
	// times, runs := 0, 100
	for {
		if stop_at.Before(time.Now()) { //  times == runs

			break
		} else {
			// times++

			/* ---------- Create test data ---------- */
			size := rand.Intn(max-min) + min
			words := make([][]byte, size)

			for i := 0; i < size; i++ {
				words[i] = rand_word()
			}

			/* ---------- Compress with cgo ---------- */
			// file1 := filepath.Join(".", f1_name)
			// c1, err := cmp_local.NewCompressor(context.Background(), f1_name, file1, ".", 100, 4)
			// if err != nil {
			// 	fmt.Println("Error C1:", err)
			// 	return
			// }

			// now := time.Now()
			// for _, word := range words {
			// 	if err := c1.AddWord(word); err != nil {
			// 		fmt.Println("Error C1 addword: ", err)
			// 		return
			// 	}
			// }

			// if err := c1.Compress(); err != nil {
			// 	fmt.Println("Error C1 compress: ", err)
			// 	return
			// }
			// end := time.Since(now)
			// fmt.Println("CGO compress: ", end.Milliseconds(), "ms")

			// c1.Close()

			/* ---------- Compress original ---------- */
			file2 := filepath.Join(".", f2_name)
			c2, err := compress.NewCompressor(context.Background(), f2_name, file2, ".", 100, 4)
			if err != nil {
				fmt.Println("Error C2:", err)
				return
			}

			now := time.Now()
			for _, word := range words {
				if err := c2.AddWord(word); err != nil {
					fmt.Println("Error C1 addword: ", err)
					return
				}
			}

			if err := c2.Compress(); err != nil {
				fmt.Println("Error C2 compress: ", err)
				return
			}
			end := time.Since(now)
			fmt.Println("GO compress: ", end.Milliseconds(), "ms")

			c2.Close()

			/* ---------- Decompressor test ---------- */
			// var d1 *compress.Decompressor
			// if d1, err = compress.NewDecompressor(file1); err != nil {
			// 	fmt.Println("Error D1: ", err)
			// 	return
			// }
			var d2 *compress.Decompressor
			if d2, err = compress.NewDecompressor(file2); err != nil {
				fmt.Println("Error D2: ", err)
				return
			}

			// s1, s2 := d1.Size(), d2.Size() // compare sizes first
			// if s1 != s2 {
			// 	fmt.Printf("Decompressor: Unequal sizes - cgo_file: %d, original_file: %d", s1, s2)
			// 	return
			// }

			// g1 := d1.MakeGetter()
			// i := 0
			// for g1.HasNext() {
			// 	word, _ := g1.Next(nil)
			// 	expected := words[i]
			// 	if string(word) != string(expected) {
			// 		fmt.Printf("D1 Expected: %s, got: %s\n", expected, word)
			// 	}
			// 	i++
			// }
			// d1.Close()

			g2 := d2.MakeGetter()
			i := 0
			for g2.HasNext() {
				word, _ := g2.Next(nil)
				expected := words[i]
				if string(word) != string(expected) {
					fmt.Printf("D2 Expected: %s, got: %s\n", expected, word)
				}
				i++
			}
			d2.Close()

			// os.Remove(f1_name)
			os.Remove(f2_name)
			fmt.Println("-----------------------------------------------")
			time.Sleep(time.Millisecond * 300)
		}

	}

}
