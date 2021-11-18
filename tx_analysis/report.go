package main

import "sync"

type report struct {
	mu sync.Mutex
}
