package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	settings := Settings{
		DBPath:        "./db",
		LogPrefix:     "",
		Terminated:    make(chan struct{}),
		RetryCount:    100,
		RetryInterval: time.Second,
		PollInterval:  time.Second,
	}

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		close(settings.Terminated)
	}()

	blockSource := NewHttpBlockSource("localhost:8000")
	err := RunImport(&settings, &blockSource)

	if err != nil {
		panic(err)
	}
}
